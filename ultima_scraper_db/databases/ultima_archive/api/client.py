from typing import TYPE_CHECKING

from ultima_scraper_db.databases.rest_api import RestAPI

if TYPE_CHECKING:
    from ultima_scraper_collection.config import UltimaScraperCollectionConfig

    from ultima_scraper_db.databases.ultima_archive.database_api import ArchiveAPI


class UAClient(RestAPI):
    database_api: "ArchiveAPI"
    config: "UltimaScraperCollectionConfig"

    def __init__(self, database_api: "ArchiveAPI"):
        from ultima_scraper_db.databases.ultima_archive.api.app import routers

        super().__init__()
        self.include_routers(routers)
        UAClient.database_api = self.database_api = database_api

    def init(self, database_api: "ArchiveAPI", config: "UltimaScraperCollectionConfig"):
        UAClient.database_api = self.database_api = database_api
        UAClient.config = self.config = config
        return self
