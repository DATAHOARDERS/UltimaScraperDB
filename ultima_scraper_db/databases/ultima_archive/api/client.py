from typing import TYPE_CHECKING

from fastapi import Request

from ultima_scraper_db.databases.rest_api import RestAPI

if TYPE_CHECKING:
    from ultima_scraper_collection.config import UltimaScraperCollectionConfig

    from ultima_scraper_db.databases.ultima_archive.database_api import ArchiveAPI


def get_ua_client(request: Request) -> "UAClient":
    return request.app


class UAClient(RestAPI):
    database_api: "ArchiveAPI"
    config: "UltimaScraperCollectionConfig"

    def __init__(self, database_api: "ArchiveAPI"):

        from ultima_scraper_db.databases.ultima_archive.api.app import routers

        super().__init__()
        self.include_routers(routers)
        self.database_api = database_api
        UAClient.database_api = self.database_api

    async def init(self, config: "UltimaScraperCollectionConfig"):
        from ultima_scraper_collection.managers.datascraper_manager.datascraper_manager import (
            DataScraperManager,
        )

        self.config = config
        self.datascraper_manager = DataScraperManager(
            self.database_api.server_manager, config
        )
        return self

    def select_site_api(self, site_name: str):
        datascraper = self.datascraper_manager.find_datascraper(site_name)
        assert datascraper is not None
        return datascraper.api
