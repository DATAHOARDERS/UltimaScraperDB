from ultima_scraper_api import SUPPORTED_SITES

from ultima_scraper_db.databases.ultima_archive.api.client import UAClient
from ultima_scraper_db.databases.ultima_archive.filters import AuthedInfoFilter
from ultima_scraper_db.databases.ultima_archive.site_api import SiteAPI
from ultima_scraper_db.managers.database_manager import Database, DatabaseAPI_, Schema

from ultima_scraper_db.databases.ultima_archive.management_api import ManagementAPI


class ArchiveAPI(DatabaseAPI_):
    def __init__(self, database: Database) -> None:
        super().__init__(database)

        self.management_api: ManagementAPI = ManagementAPI(
            self.database.schemas["management"]
        )
        self.management_schema: Schema = self.database.schemas["management"]
        self.site_apis: dict[str, SiteAPI] = {}
        for supported_site in SUPPORTED_SITES:
            supported_site = supported_site.lower()
            self.site_apis[supported_site] = SiteAPI(
                self.database.schemas[supported_site]
            )
        self.fast_api = UAClient(database_api=self)

    async def init(self):
        from ultima_scraper_collection.managers.server_manager import ServerManager

        self.server_manager = await ServerManager(self).init(self.database)
        return self

    def create_management_api(self):
        return ManagementAPI(self.database.schemas["management"])

    def create_site_api(self, name: str):
        return SiteAPI(self.database.schemas[name.lower()])

    def get_site_api(self, name: str):
        site_api = self.find_site_api(name)
        new_site_api = SiteAPI(
            Schema(
                site_api.schema.name,
                site_api.schema.engine,
                site_api.schema.sessionmaker(),
                site_api.schema.database,
            )
        )
        return new_site_api

    def find_site_api(self, name: str):
        return self.site_apis[name.lower()]

    async def get_sites(self):
        async with self.create_management_api() as management_api:
            sites = await management_api.get_sites()
            return sites

    async def update_authed_users(self):
        import ultima_scraper_api
        from ultima_scraper_api.helpers.main_helper import get_date_range_past_days

        from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
            UserModel,
        )

        for site_name in SUPPORTED_SITES:
            site_api = ultima_scraper_api.select_api(site_name)
            async with self.create_site_api(site_name) as db_site_api:
                authed_info_filter = AuthedInfoFilter(
                    exclude_between_dates=get_date_range_past_days(), active=True
                )
                db_users = await db_site_api.get_users(
                    authed_info_filter=authed_info_filter,
                    order_by=UserModel.last_checked_at.asc(),
                )
                for db_user in db_users:
                    for db_auth_info in db_user.find_auths():
                        auth_details = db_auth_info.convert_to_auth_details(site_name)
                        async with site_api.login_context(
                            auth_details.export()
                        ) as authed:
                            if not authed:
                                print(f"User {db_user.username} failed to login.")
                                await db_auth_info.deactivate()
                            else:
                                print(f"User {db_user.username} logged in.")
                                await db_site_api.create_or_update_user(
                                    authed.user, db_user, performer_optimize=True
                                )
                                break
                    await db_site_api.get_session().commit()
        return True
