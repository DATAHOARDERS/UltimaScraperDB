from typing import Any, Sequence

from fastapi import APIRouter, Depends, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from pydantic import BaseModel
from sqlalchemy import (  # Import func from sqlalchemy, not sqlalchemy.orm
    func,
    nullslast,
    or_,
    orm,
    select,
    update,
)
from sqlalchemy.orm import lazyload, selectinload
from sqlalchemy.sql import distinct
from ultima_scraper_api.apis.onlyfans.authenticator import OnlyFansAuthModel

from ultima_scraper_db.databases.ultima_archive.api.client import (
    UAClient,
    get_ua_client,
)
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    MediaModel,
    MessageModel,
    PostModel,
    UserInfoModel,
    UserModel,
)
from ultima_scraper_db.databases.ultima_archive.site_api import ContentManager

restricted = (
    lazyload(UserModel.user_auths_info),
    orm.defer(UserModel.performer),
    orm.defer(UserModel.favorite),
    orm.defer(UserModel.balance),
    orm.defer(UserModel.spend),
    orm.defer(UserModel.updated_at),
    orm.defer(UserModel.created_at),
)

router = APIRouter(
    prefix="/users",
    tags=["users"],
    responses={404: {"description": "Not found"}},
)


class Item(BaseModel):
    name: str
    description: str | None = None
    price: float
    tax: float | None = None


@router.get("/")
async def get_users(
    site_name: str,
    page: int,
    limit: int,
    order_by: str | None = None,
    order_direction: str = "asc",
    ua_client: UAClient = Depends(get_ua_client),
):
    database_api = ua_client.database_api

    # Ensure valid order direction
    if order_direction.lower() not in ["asc", "desc"]:
        raise HTTPException(
            status_code=400, detail="Invalid order_direction, must be 'asc' or 'desc'."
        )

    offset = max(0, (page - 1) * limit)
    async with database_api.create_site_api(site_name) as site_db_api:

        # First, create a subquery that counts paid posts for each user

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

        # Main query with joins to both subqueries
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
            .join(UserInfoModel, UserModel.id == UserInfoModel.user_id)
            .where(UserModel.performer.is_(True))
            .where(
                or_(
                    UserModel.active.is_(True), UserInfoModel.downloaded_at.is_not(None)
                )
            )
            .offset(offset)
            .limit(limit)
            .options(*restricted)
            .options(orm.selectinload(UserModel.user_info))
        )

        # Apply ordering
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

        # Execute the query
        results = await site_db_api.get_session().execute(stmt)

        # Process results
        final_users: list[dict[str, Any]] = []
        for user, ppv_count in results:
            temp_user = jsonable_encoder(user)
            temp_user["user_info"]["ppv_count"] = ppv_count
            final_users.append(temp_user)

        return final_users


class AdvancedOptions(BaseModel):
    content: bool = False
    subscribers: bool = False
    subscriptions: bool = False
    auths: bool = False
    bought_contents: bool = False


@router.get("/{site_name}/{identifier}")
async def read(
    site_name: str,
    identifier: str,
    options: AdvancedOptions = Depends(),
    ua_client: UAClient = Depends(get_ua_client),
):
    database_api = ua_client.database_api
    site_api = ua_client.select_site_api(site_name)

    async with database_api.create_site_api(site_name) as site_db_api:
        user = await site_db_api.get_user(
            identifier,
            load_aliases=True,
            load_user_info=True,
            load_remote_urls=True,
            extra_options=restricted,
        )
        if user:
            await user.awaitable_attrs.aliases
            await user.awaitable_attrs.user_info
        else:
            authed = await site_api.login(guest=True)
            if authed:
                site_user = await authed.get_user(identifier)
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
        if user:
            user_info = jsonable_encoder(user.user_info, exclude={"user"})
            user = jsonable_encoder(user, exclude={"user_info"})
            user["user_info"] = user_info
            if options:
                if options.content:
                    posts = await site_db_api.get_posts(user["id"], load_media=False)
                    user["posts"] = posts
                    messages = await site_db_api.get_messages(
                        user["id"], load_media=False
                    )
                    user["messages"] = messages
                    pass
        return user


@router.get("/bulk/{site_name}/{identifiers}")
async def read_bulk(
    site_name: str, identifiers: str, ua_client: UAClient = Depends(get_ua_client)
):
    database_api = ua_client.database_api

    async with database_api.create_site_api(site_name) as site_db_api:
        temp_identifiers = identifiers.split(",")
        final_identifiers = [int(x) if x.isdigit() else x for x in temp_identifiers]
        users = await site_db_api.get_users(
            final_identifiers,
            load_aliases=True,
            load_user_info=True,
            load_remote_urls=True,
            extra_options=restricted,
        )
        return users


@router.post("/advanced/{site_name}/{identifier}")
async def read_advanced(
    site_name: str,
    identifier: int | str,
    options: AdvancedOptions | None = None,
    ua_client: UAClient = Depends(get_ua_client),
):
    database_api = ua_client.database_api

    async with database_api.create_site_api(site_name) as site_db_api:
        user = await site_db_api.get_user(identifier, extra_options=restricted)
        if user:
            if options:
                if options.subscriptions:
                    await user.awaitable_attrs.subscriptions
                if options.subscribers:
                    await user.awaitable_attrs.subscribers
                if options.auths:
                    await user.awaitable_attrs.user_auths_info
                if options.bought_contents:
                    await user.awaitable_attrs.bought_contents
            await user.awaitable_attrs.aliases
            await user.awaitable_attrs.user_info
        return user


@router.get("/check/{site_name}/{identifier}")
async def check_user(
    site_name: str, identifier: int | str, ua_client: UAClient = Depends(get_ua_client)
):
    database_api = ua_client.database_api
    site_api = ua_client.select_site_api(site_name)
    has_active_subscription = False
    async with database_api.create_site_api(site_name) as site_db_api:
        user = await site_db_api.get_user(identifier)
        if user:
            await user.awaitable_attrs.subscribers
            db_users = [subscriber.user for subscriber in user.subscribers]
            if db_users:
                db_user = db_users[0]
                for db_auth in db_user.find_authed_buyers(
                    await db_user.find_buyers(
                        active=None if db_user.favorite else True,
                        active_user=True,
                    )
                ):
                    auth_details = db_auth.convert_to_auth_details(site_api.site_name)
                    authed = await site_api.login(auth_details.export())
                    if authed and authed.is_authed():
                        if isinstance(authed, OnlyFansAuthModel):
                            if authed.issues:
                                continue
                        subscription = await authed.get_subscriptions([user.id])
                        if subscription:
                            has_active_subscription = True
                            break
                    else:
                        await db_auth.deactivate()
    return {"has_active_subscription": has_active_subscription}


@router.get("/check/paid_content/{site_name}/{identifier}")
async def check_paid_content(
    site_name: str,
    identifier: int | str,
    only_downloaded: bool = False,
    ua_client: UAClient = Depends(get_ua_client),
):
    database_api = ua_client.database_api
    paid_content: Sequence[MediaModel] = []
    async with database_api.create_site_api(site_name) as site_db_api:
        db_user = await site_db_api.get_user(identifier)
        if db_user:
            db_buyers = await db_user.find_buyers(
                active=None if db_user.favorite else True,
                active_user=True,
            )
            if db_buyers:
                content_manager = ContentManager(db_user)
                paid_content = await content_manager.find_paid_contents(
                    only_downloaded=only_downloaded
                )

    return paid_content


@router.get("/auth/{site_name}/{identifier}")
async def get_auth(
    site_name: str, identifier: int | str, ua_client: UAClient = Depends(get_ua_client)
):
    database_api = ua_client.database_api
    async with database_api.create_site_api(site_name) as db_site:
        user = await db_site.get_user(identifier)
        user_auth_info = None
        if user:
            await user.awaitable_attrs.user_auths_info
            user_auth_info = user.user_auths_info
        return user_auth_info


@router.post("/update/sex/{site_name}/{identifier}/{sex}")
async def update_sex(
    site_name: str,
    identifier: int | str,
    sex: str,
    ua_client: UAClient = Depends(get_ua_client),
):
    database_api = ua_client.database_api
    async with database_api.create_site_api(site_name) as site_db_api:
        user = await site_db_api.get_user(identifier)
        if user:
            await user.awaitable_attrs.user_info
            final_sex = 0 if sex == "female" else 1
            user.user_info.sex = final_sex
