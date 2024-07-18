from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel

from ultima_scraper_db.databases.ultima_archive.api.client import UAClient

router = APIRouter(
    prefix="/remote_urls",
    tags=["remote_urls"],
    responses={404: {"description": "Not found"}},
)


class RemoteURLData(BaseModel):
    user_id: int
    host_id: int
    url: str
    uploaded_at: float


@router.get("/{site_name}/{user_id}")
async def get_remote_url(request: Request, site_name: str, user_id: int):
    database_api = UAClient.database_api

    site_api = database_api.get_site_api(site_name)
    async with site_api as site_api:
        remote_url = await site_api.get_remote_url(user_id)
        return remote_url


@router.patch("/{site_name}")
async def update_remote_url(
    request: Request,
    site_name: str,
    remote_url: RemoteURLData,
):
    database_api = UAClient.database_api

    site_api = database_api.get_site_api(site_name)
    async with site_api as site_api:
        remote_url_db = await site_api.create_or_update_remote_url(
            remote_url.user_id,
            host_id=remote_url.host_id,
            url=remote_url.url,
            uploaded_at=datetime.fromtimestamp(remote_url.uploaded_at),
        )
        return remote_url_db
