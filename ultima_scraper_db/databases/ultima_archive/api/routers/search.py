import re
from typing import Any, Generic, List, Literal, TypeVar

import ultima_scraper_api
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRouter
from pydantic import BaseModel
from pydantic.generics import GenericModel
from sqlalchemy import nullslast, or_, orm, select
from sqlalchemy.sql import func, or_, select

from ultima_scraper_db.databases.ultima_archive.api.client import (
    UAClient,
    get_ua_client,
)
from ultima_scraper_db.databases.ultima_archive.api.routers.users import (
    AdvancedOptions,
    restricted,
)
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    UserInfoModel,
)
from ultima_scraper_db.databases.ultima_archive.site_api import (
    MessageModel,
    PostModel,
    UserAliasModel,
    UserModel,
)
from ultima_scraper_db.helpers import extract_identifier_from_url

T = TypeVar("T")
router = APIRouter(
    prefix="/search",
    tags=["search"],
    responses={404: {"description": "Not found"}},
)


@router.get("/users/{site_name}")
async def search_users(
    site_name: str,
    q: str = "",
    order_by: str | None = None,
    order_direction: str = "asc",  # new parameter to control sort direction
    page: int = 1,
    limit: int = 20,
    ua_client: UAClient = Depends(get_ua_client),
):
    database_api = ua_client.database_api
    site_api = ua_client.select_site_api(site_name)
    # Ensure valid order direction
    if order_direction.lower() not in ["asc", "desc"]:
        raise HTTPException(
            status_code=400, detail="Invalid order_direction, must be 'asc' or 'desc'."
        )
    offset = (page - 1) * limit  # calculate offset for pagination
    q = q.replace(" ", "")
    q = q.replace("@", "")
    q = extract_identifier_from_url(q)
    async with database_api.create_site_api(site_name) as site_db_api:

        # Create subqueries for both paid posts and messages
        paid_posts_subquery = (
            select(PostModel.user_id, func.count().label("posts_ppv_count"))
            .where(PostModel.paid.is_(True))
            .group_by(PostModel.user_id)
            .subquery()
        )

        paid_messages_subquery = (
            select(MessageModel.user_id, func.count().label("messages_ppv_count"))
            .where(MessageModel.paid.is_(True))
            .group_by(MessageModel.user_id)
            .subquery()
        )
        stmt = (
            select(
                UserModel,
                (
                    func.coalesce(paid_posts_subquery.c.posts_ppv_count, 0)
                    + func.coalesce(paid_messages_subquery.c.messages_ppv_count, 0)
                ).label("ppv_count"),
            )
            .outerjoin(
                paid_posts_subquery, UserModel.id == paid_posts_subquery.c.user_id
            )
            .outerjoin(
                paid_messages_subquery, UserModel.id == paid_messages_subquery.c.user_id
            )
            .outerjoin(UserAliasModel)
            .where(
                or_(
                    UserModel.username.ilike(f"%{q}%"),
                    UserAliasModel.username.ilike(f"%{q}%"),
                )
            )
            .offset(offset)
            .limit(limit)
            .options(*restricted)
            .options(orm.selectinload(UserModel.user_info))
        )

        if order_by in ["downloaded_at", "size"]:
            field = (
                UserModel.downloaded_at
                if order_by == "downloaded_at"
                else UserInfoModel.size
            )
            if order_direction.lower() == "desc":
                stmt = stmt.order_by(nullslast(field.desc()), UserModel.id)
            else:
                stmt = stmt.order_by(field, UserModel.id)
        else:
            stmt = stmt.order_by(UserModel.id)
        results = await site_db_api.get_session().execute(stmt)
        final_users: list[dict[str, Any]] = []
        accurate_username = False
        for user, ppv_count in results:
            temp_user = jsonable_encoder(user)
            temp_user["user_info"]["ppv_count"] = ppv_count
            if q == user.username:
                accurate_username = True
            final_users.append(temp_user)
        if not final_users or not accurate_username:
            authed = await site_api.login(guest=True)
            if authed:
                site_user = await authed.get_user(q)
                if site_user:
                    db_user = await site_db_api.get_user(
                        site_user.id,
                        extra_options=restricted,
                    )
                    user = await site_db_api.create_or_update_user(
                        site_user, db_user, performer_optimize=True
                    )
                    user.content_manager = None
                    await user.awaitable_attrs.aliases
                    await user.awaitable_attrs.user_info
                    await user.awaitable_attrs.remote_urls
                    await authed.authenticator.close()
                    results = await site_db_api.get_session().scalars(stmt)
                    final_users = [
                        jsonable_encoder(user, exclude={"user_info": "user"})
                        for user in results.all()
                    ]

        return final_users


class PaginatedResponse(GenericModel, Generic[T]):
    total: int
    results: List[T]


@router.get(
    "/{site_name}/content",
    response_model=PaginatedResponse[dict[str, Any]],
)
async def search_content(
    site_name: str,
    category: Literal["posts", "messages"],
    q: str = "",
    user_id: int | None = None,
    ppv: bool | None = None,
    page: int = Query(1, ge=1),
    limit: int = Query(100, le=1000),
    ua_client: UAClient = Depends(get_ua_client),
):
    database_api = ua_client.database_api
    offset = (page - 1) * limit

    async with database_api.create_site_api(site_name) as site_db_api:
        session = site_db_api.get_session()

        if category == "posts":
            model = PostModel

        elif category == "messages":
            model = MessageModel

        else:
            raise HTTPException(status_code=400, detail="Invalid content type")

        stmt = select(model)
        if user_id:
            stmt = stmt.where(model.user_id == user_id)
        if q:
            stmt = stmt.where(model.text.ilike(f"%{q}%"))
        if ppv is not None:
            stmt = stmt.where(model.price > 0 if ppv else model.price == 0)

        total_stmt = select(func.count()).select_from(stmt.subquery())
        total = await session.scalar(total_stmt)

        stmt = (
            stmt.order_by(nullslast(model.created_at))
            .offset(offset)
            .limit(limit)
            .options(orm.noload(model.media))
        )

        results = await session.scalars(stmt)
        posts = results.unique().all()

        return PaginatedResponse[dict[str, Any]](
            total=total, results=jsonable_encoder(posts)
        )
