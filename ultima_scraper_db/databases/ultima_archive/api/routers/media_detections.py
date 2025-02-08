from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel
from sqlalchemy import or_, orm, select, update
from sqlalchemy.orm import contains_eager, lazyload, sessionmaker

from ultima_scraper_db.databases.ultima_archive.api.client import UAClient
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    MediaDetectionModel,
)

router = APIRouter(
    prefix="/media_detections",
    tags=["media_detections"],
    responses={404: {"description": "Not found"}},
)


@router.post("/")
async def get_detected_media(
    filters: list[MediaDetectionModel.MediaDetectionFilter],
    site_name: str,
    user_id: int | None = None,
    page: int = Query(1, alias="page", gt=0),
    limit: int = Query(10, alias="limit", gt=0),
    sex: int | None = None,
    category: str | None = None,
):

    database_api = UAClient.database_api
    async with database_api.create_site_api(site_name) as site_api:
        stmt = MediaDetectionModel().filter_stmt(
            filters, sex, user_id=user_id, category=category
        )
        offset = (page - 1) * limit
        temp = await site_api.get_session().scalars(stmt.offset(offset).limit(limit))
        media_detections = temp.all()
        # [await x.awaitable_attrs.media for x in media_detections]
        return media_detections
