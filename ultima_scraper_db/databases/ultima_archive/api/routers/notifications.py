from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import or_, orm, select, update
from sqlalchemy.orm import contains_eager, lazyload, sessionmaker

from ultima_scraper_db.databases.ultima_archive.api.client import UAClient
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    NotificationModel,
)

router = APIRouter(
    prefix="/notifications",
    tags=["notifications"],
    responses={404: {"description": "Not found"}},
)


@router.get("/{site_name}/unsent/{notification_site}")
async def get_notifications(
    request: Request,
    site_name: str,
    notification_site: Literal["discord", "telegram"],
    page: int = 1,
    limit: int = 100,
):
    database_api = UAClient.database_api
    async with database_api.create_site_api(site_name) as site_api:
        notifications = await site_api.get_notifications(
            notification_site, sent=False, page=page, limit=limit
        )
        return notifications


@router.patch("/{site_name}/{notification_site}/{notification_id}")
async def complete_notification(
    request: Request, site_name: str, notification_site: str, notification_id: int
):
    database_api = UAClient.database_api
    async with database_api.create_site_api(site_name) as site_api:
        db_session = site_api.get_session()
        stmt = update(NotificationModel).where(NotificationModel.id == notification_id)
        if notification_site == "telegram":
            stmt = stmt.values(sent_telegram=True)
        else:
            stmt = stmt.values(sent_discord=True)
        await db_session.execute(stmt)
        await db_session.commit()
        return Response(status_code=204)
