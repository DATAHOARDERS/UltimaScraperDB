import re
from dataclasses import asdict, dataclass
from typing import Any, Generic, List, Literal, Optional, TypeVar

import ultima_scraper_api
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Security
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRouter
from pydantic import BaseModel, Field
from pydantic.generics import GenericModel
from sqlalchemy import Case, UnaryExpression, nullslast, or_, orm, select
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql import case, func, or_, select
from typing_extensions import Literal
from ultima_scraper_api.apis.auth_streamliner import datetime

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


class PaginatedResponse(GenericModel, Generic[T]):
    total: int
    results: List[T]


class Filters:
    def __init__(
        self,
        site_name: str,
        q: str = Query(""),
        order_by: Optional[str] = Query(None),
        order_direction: Literal["asc", "desc"] = Query("asc"),
        has_ppv: Optional[bool] = Query(None),
    ):
        self.site_name = site_name
        self.q = q
        self.order_by = order_by
        self.order_direction = order_direction
        self.has_ppv = has_ppv

    def as_dict(self, exclude_none: bool = False) -> dict[str, Any]:
        data = self.__dict__
        return (
            {k: v for k, v in data.items() if v is not None} if exclude_none else data
        )


@router.get("/users/{site_name}")
async def search_users(
    filters: Filters = Depends(),
    page: int = Query(1, ge=1),
    limit: int = Query(20, le=50),
    ua_client: UAClient = Depends(get_ua_client),
):
    site_name = filters.site_name
    q = filters.q
    order_by = filters.order_by or "id"
    order_direction = filters.order_direction
    has_ppv = filters.has_ppv

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
        last_post_date_subquery = (
            select(
                PostModel.user_id,
                func.max(PostModel.created_at).label("last_posted_date"),
            )
            .group_by(PostModel.user_id)
            .subquery()
        )

        stmt = (
            select(
                UserModel,
                (
                    func.coalesce(paid_posts_subquery.c.posts_ppv_count, 0)
                    + func.coalesce(paid_messages_subquery.c.messages_ppv_count, 0)
                ).label("ppv_count"),
                last_post_date_subquery.c.last_posted_date,
            )
            .outerjoin(
                paid_posts_subquery, UserModel.id == paid_posts_subquery.c.user_id
            )
            .outerjoin(
                paid_messages_subquery, UserModel.id == paid_messages_subquery.c.user_id
            )
            .outerjoin(UserAliasModel)
            .outerjoin(
                last_post_date_subquery,
                UserModel.id == last_post_date_subquery.c.user_id,
            )
            .where(
                or_(
                    UserModel.username.ilike(f"%{q}%"),
                    UserAliasModel.username.ilike(f"%{q}%"),
                )
            )
        )
        if has_ppv:
            # Only include users who have at least one paid post or paid message
            stmt = stmt.where(
                or_(
                    paid_posts_subquery.c.posts_ppv_count.isnot(None),
                    paid_messages_subquery.c.messages_ppv_count.isnot(None),
                )
            )

        # Always order by exact match first
        ordering: list[
            Case[Any]
            | UnaryExpression[datetime | int]
            | InstrumentedAttribute[datetime]
            | InstrumentedAttribute[int]
        ] = [case((func.lower(UserModel.username) == q.lower(), 0), else_=1)]

        # Then add field-based ordering if specified
        if order_by in ["downloaded_at", "size"]:
            field = (
                UserModel.downloaded_at
                if order_by == "downloaded_at"
                else UserInfoModel.size
            )
            if order_direction.lower() == "desc":
                ordering.append(nullslast(field.desc()))
            else:
                ordering.append(field)

        # Always order by id last
        ordering.append(UserModel.id)

        # Apply to query
        stmt = stmt.order_by(*ordering)

        total_stmt = select(func.count(func.distinct(stmt.subquery().c.id)))
        total = await site_db_api.get_session().scalar(total_stmt)
        stmt = (
            stmt.offset(offset)
            .limit(limit)
            .options(*restricted)
            .options(orm.selectinload(UserModel.user_info))
        )
        results = await site_db_api.get_session().execute(stmt)
        final_users: list[dict[str, Any]] = []
        accurate_username = False
        for user, ppv_count, last_posted_date in results.unique():
            print(
                f"Processing user: {user.username}, PPV Count: {ppv_count}, Last Post Date: {last_posted_date}"
            )
            temp_user = jsonable_encoder(user)
            if temp_user["user_info"]:
                temp_user["user_info"]["ppv_count"] = ppv_count
            temp_user["last_posted_at"] = last_posted_date
            if q.lower() == str(user.id) or q.lower() == user.username.lower():
                accurate_username = True
            final_users.append(temp_user)
        if not final_users or not accurate_username:
            async with site_api.login_context(guest=True) as authed:
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
                        if site_user.username != q:
                            _user_alias = await site_db_api.create_or_update_user_alias(
                                user, q
                            )
                        user.content_manager = None
                        await user.awaitable_attrs.aliases
                        await user.awaitable_attrs.user_info
                        await user.awaitable_attrs.remote_urls
                        results = await site_db_api.get_session().scalars(stmt)
                        # fix this, we get json encode max recursion error, why? I don't know. But it could be because of the awaitable_attrs
                        # _abc = results.all()
                        final_users = [
                            jsonable_encoder(
                                user, exclude={"user_info": "user", "aliases": "user"}
                            )
                            for user in results.all()
                        ]

        return PaginatedResponse[dict[str, Any]](
            total=total, results=jsonable_encoder(final_users)
        )


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
