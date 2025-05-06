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
    part: int = 0
    uploaded_at: float


class Performer(BaseModel):
    identifier: int | str
    site_name: str


@router.get("/{site_name}/{user_id}")
async def get_remote_urls(request: Request, site_name: str, user_id: int):
    database_api = UAClient.database_api

    site_api = database_api.create_site_api(site_name)
    async with site_api as site_api:
        remote_urls = await site_api.get_remote_urls(user_id)
        return remote_urls


@router.patch("/{site_name}")
async def update_remote_url(
    request: Request,
    site_name: str,
    remote_url: RemoteURLData,
):
    database_api = UAClient.database_api

    site_api = database_api.create_site_api(site_name)
    async with site_api as site_api:
        remote_url_db = await site_api.create_or_update_remote_url(
            remote_url.user_id,
            host_id=remote_url.host_id,
            url=remote_url.url,
            part=remote_url.part,
            uploaded_at=datetime.fromtimestamp(remote_url.uploaded_at),
        )
        return remote_url_db


@router.post("/bulk")
async def get_remote_urls_bulk(request: Request, performers: list[Performer]):
    database_api = UAClient.database_api

    grouped_performers = {}
    for performer in performers:
        site_name = performer.site_name
        if site_name not in grouped_performers:
            grouped_performers[site_name] = []
        grouped_performers[site_name].append(performer)

    all_remote_urls = {}
    for site_name, site_performers in grouped_performers.items():
        performer_ids = [int(performer.identifier) for performer in site_performers]
        site_api = database_api.create_site_api(site_name)
        async with site_api as site_api:
            remote_urls = await site_api.get_bulk_remote_urls(performer_ids)
            if site_name not in all_remote_urls:
                all_remote_urls[site_name] = {}
            for performer in site_performers:
                performer_urls = [
                    url
                    for url in remote_urls
                    if url.user_id == int(performer.identifier)
                ]
                all_remote_urls[site_name][performer.identifier] = performer_urls
    return all_remote_urls
