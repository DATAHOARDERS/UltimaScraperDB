from ultima_scraper_db.databases.ultima_archive import SUPPORTED_SITES
from ultima_scraper_db.databases.ultima_archive.site_api import SiteAPI
from ultima_scraper_db.managers.database_manager import Database, DatabaseAPI_, Schema


class ArchiveAPI(DatabaseAPI_):
    def __init__(self, database: Database) -> None:
        super().__init__(database)

        self.management_schema: Schema = self.database.schemas["management"]
        self.site_apis: dict[str, SiteAPI] = {}
        for supported_site in SUPPORTED_SITES:
            supported_site = supported_site.lower()
            self.site_apis[supported_site] = SiteAPI(
                self.database.schemas[supported_site]
            )

    async def init(self):
        from ultima_scraper_collection.managers.server_manager import ServerManager

        self.server_manager = await ServerManager(self).init(self.database)
        return self

    def get_site_api(self, name: str):
        return self.site_apis[name.lower()]

    def find_site_api(self, name: str):
        return self.site_apis[name.lower()]
