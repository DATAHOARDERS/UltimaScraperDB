from fastapi import APIRouter, Response
from pydantic import BaseModel

from ultima_scraper_db.databases.ultima_archive.api.client import UAClient
from ultima_scraper_db.databases.ultima_archive.schemas.management import HostModel

router = APIRouter(
    prefix="/hosts",
    tags=["hosts"],
    responses={404: {"description": "Not found"}},
)


@router.get("/")
async def get_hosts():
    database_api = UAClient.database_api

    async with database_api.create_management_api() as management_api:
        hosts = await management_api.get_hosts()
        return hosts


class PyHost(BaseModel):
    name: str
    identifier: str
    password: str
    source: bool
    active: bool


@router.post("/create")
async def create_host(response: Response, host: PyHost):
    database_api = UAClient.database_api

    async with database_api.create_management_api() as management_api:
        db_host = HostModel(
            name=host.name,
            identifier=host.identifier,
            password=host.password,
            source=host.source,
            active=host.active,
        )
        await management_api.create_or_update_host(db_host)
        return db_host
