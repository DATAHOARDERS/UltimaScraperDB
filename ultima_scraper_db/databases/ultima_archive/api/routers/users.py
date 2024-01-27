from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import or_, orm, select
from sqlalchemy.orm import contains_eager, lazyload, sessionmaker

from ultima_scraper_db.databases.ultima_archive.api.client import UAClient
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import UserModel

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
):
    database_api = UAClient.database_api

    site_api = database_api.get_site_api(site_name)
    async with site_api as site_api:
        limit = 100 if limit > 100 else limit
        offset = max(0, (page - 1) * limit)
        stmt = (
            select(UserModel)
            .where(UserModel.performer.is_(True))
            .where(
                or_(UserModel.active.is_(True), UserModel.downloaded_at.is_not(None))
            )
            .offset(offset)
            .limit(limit)
            .order_by(UserModel.id)
            .options(*restricted)
        )
        users = await site_api.get_session().scalars(stmt)
        return users.all()


@router.get("/{identifier}")
async def read(site_name: str, identifier: int | str):
    database_api = UAClient.database_api

    site_api = database_api.get_site_api(site_name)
    async with site_api as site_api:
        user = await site_api.get_user(identifier, extra_options=restricted)
        if user:
            await user.awaitable_attrs.aliases
        return user
