from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import or_, orm, select, update
from sqlalchemy.orm import contains_eager, lazyload, sessionmaker

from ultima_scraper_db.databases.ultima_archive.api.client import UAClient
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    FilePathModel,
    JobModel,
)

router = APIRouter(
    prefix="/client",
    tags=["client"],
    responses={404: {"description": "Not found"}},
)


@router.get("/whoami/{identifier}")
async def whoami(request: Request, identifier: int | str):
    database_api = UAClient.database_api
    async with database_api.create_management_api() as management_api:
        if isinstance(identifier, int):
            server = await management_api.get_server(
                server_id=identifier, server_name=None
            )
        else:
            if identifier.isdigit():
                server = await management_api.get_server(
                    server_id=int(identifier), server_name=None
                )
            else:
                server = await management_api.get_server(
                    server_id=None, server_name=identifier
                )
    return server


@router.post("/")
async def get_ip(request: Request):
    assert request.client
    client_host = request.client.host
    return {"client_ip": client_host}
