from typing import Sequence

import ultima_scraper_api
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy import nullslast, or_, orm, select
from sqlalchemy.orm import lazyload

from ultima_scraper_db.databases.ultima_archive.api.client import UAClient
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    MediaModel,
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
):
    database_api = UAClient.database_api

    site_api = database_api.get_site_api(site_name)
    async with site_api as site_api:
        # limit = 100 if limit > 100 else limit
        offset = max(0, (page - 1) * limit)
        stmt = (
            select(UserModel)
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

        if order_by == "downloaded_at":
            stmt = stmt.order_by(
                nullslast(UserInfoModel.downloaded_at.desc()), UserModel.id
            )
        else:
            stmt = stmt.order_by(UserModel.id)
        results = await site_api.get_session().scalars(stmt)
        users = results.all()
        return users


@router.get("/{site_name}/{identifier}")
async def read(site_name: str, identifier: int | str):
    database_api = UAClient.database_api

    site_db_api = database_api.get_site_api(site_name)
    async with site_db_api as site_db_api:
        user = await site_db_api.get_user(identifier, extra_options=restricted)
        if user:
            await user.awaitable_attrs.aliases
            await user.awaitable_attrs.user_info
        else:
            site_api = ultima_scraper_api.select_api(site_name)
            authed = await site_api.login(guest=True)
            if authed:
                site_user = await authed.get_user(identifier)
                if site_user:
                    user = await site_db_api.create_or_update_user(
                        site_user, None, performer_optimize=True
                    )
                    user.content_manager = None
                    await user.awaitable_attrs.aliases
                    await user.awaitable_attrs.user_info
                await authed.authenticator.close()
        if user:
            user_info = jsonable_encoder(user.user_info, exclude={"user"})
            user = jsonable_encoder(user, exclude={"user_info"})
            user["user_info"] = user_info
        return user


class AdvancedOptions(BaseModel):
    subscribers: bool = False


@router.post("/advanced/{site_name}/{identifier}")
async def read_advanced(
    site_name: str, identifier: int | str, options: AdvancedOptions | None = None
):
    database_api = UAClient.database_api

    site_api = database_api.get_site_api(site_name)
    async with site_api as site_api:
        user = await site_api.get_user(identifier, extra_options=restricted)
        if user:
            if options:
                if options.subscribers:
                    await user.awaitable_attrs.subscribers
            await user.awaitable_attrs.aliases
            await user.awaitable_attrs.user_info
        return user


from ultima_scraper_collection.config import UltimaScraperCollectionConfig
from ultima_scraper_collection.managers.datascraper_manager.datascraper_manager import (
    DataScraperManager,
)


@router.get("/check/{site_name}/{identifier}")
async def check_user(site_name: str, identifier: int | str):
    database_api = UAClient.database_api
    datascraper_manager = DataScraperManager(
        database_api.server_manager, UltimaScraperCollectionConfig()
    )
    has_active_subscription = False
    async with database_api.get_site_api(site_name) as db_site_api:
        user = await db_site_api.get_user(identifier)
        if user:
            datascraper = datascraper_manager.find_datascraper(site_name)
            assert datascraper
            site_api = datascraper.api
            await user.awaitable_attrs.subscribers
            for subscriber in user.subscribers:
                db_user = subscriber.user
                db_buyers = await db_user.find_buyers(
                    active=None if db_user.favorite else True,
                    active_user=True,
                )
                for db_buyer in db_buyers:
                    db_auth = db_buyer.find_auth()
                    if not db_auth:
                        continue
                    auth_details = db_auth.convert_to_auth_details(site_api.site_name)
                    authed = await site_api.login(auth_details.export())
                    if authed:
                        subscription = await authed.get_subscriptions([identifier])
                        if subscription:
                            has_active_subscription = True
                            break
    return {"has_active_subscription": has_active_subscription}


@router.get("/check/paid_content/{site_name}/{identifier}")
async def check_paid_content(
    site_name: str, identifier: int | str, only_downloaded: bool = False
):
    database_api = UAClient.database_api
    paid_content: Sequence[MediaModel] = []
    async with database_api.get_site_api(site_name) as db_site_api:
        db_user = await db_site_api.get_user(identifier)
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
