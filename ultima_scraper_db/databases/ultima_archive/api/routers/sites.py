from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import func, nullslast, or_, orm, select, update
from sqlalchemy.orm import contains_eager, lazyload, sessionmaker

from ultima_scraper_db.databases.ultima_archive.api.client import (
    UAClient,
    get_ua_client,
)
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    SubscriptionModel,
    UserInfoModel,
    UserModel,
)

router = APIRouter(
    prefix="/sites",
    tags=["sites"],
    responses={404: {"description": "Not found"}},
)


@router.get("/")
async def get_sites(
    ua_client: UAClient = Depends(get_ua_client),
):
    database_api = ua_client.database_api

    async with database_api.create_management_api() as management_api:
        sites = await management_api.get_sites()
        return sites
