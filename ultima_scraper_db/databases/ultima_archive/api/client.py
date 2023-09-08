from typing import TYPE_CHECKING

from fastapi import APIRouter, FastAPI
from ultima_scraper_db.databases.rest_api import RestAPI

if TYPE_CHECKING:
    from ultima_scraper_db.databases.ultima_archive.database_api import ArchiveAPI


class UAClient(RestAPI):
    database_api: "ArchiveAPI"

    def __init__(self):
        from ultima_scraper_db.databases.ultima_archive.api.app import routers

        super().__init__()
        self.include_routers(routers)
