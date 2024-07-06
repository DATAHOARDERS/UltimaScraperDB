from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import or_, orm, select, update
from sqlalchemy.orm import contains_eager, lazyload, sessionmaker

from ultima_scraper_db.databases.ultima_archive.api.client import UAClient
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    FilePathModel,
)

router = APIRouter(
    prefix="/filepaths",
    tags=["filepaths"],
    responses={404: {"description": "Not found"}},
)


@router.get("/")
async def get_filepath(request: Request, site_name: str, media_id: int):
    database_api = UAClient.database_api
    async with database_api.create_site_api(site_name) as site_api:
        filepaths = await site_api.get_filepaths(media_id=media_id)
        return filepaths
