import asyncio

from ultima_scraper_collection.config import UltimaScraperCollectionConfig

from ultima_scraper_db.databases.ultima_archive import merged_metadata
from ultima_scraper_db.databases.ultima_archive.api.client import UAClient
from ultima_scraper_db.databases.ultima_archive.database_api import ArchiveAPI
from ultima_scraper_db.managers.database_manager import Alembica, DatabaseManager


async def run(config: UltimaScraperCollectionConfig):

    db_manager = DatabaseManager()
    database = db_manager.create_database(
        **config.settings.databases[0].connection_info.model_dump(),
        alembica=Alembica(),
        metadata=merged_metadata
    )
    await database.init_db()
    ultima_archive_db_api = await ArchiveAPI(database).init()
    fast_api = UAClient(ultima_archive_db_api)
    ultima_archive_db_api.activate_api(fast_api, 2140)
    ultima_archive_db_api.server.join()


if __name__ == "__main__":
    config = UltimaScraperCollectionConfig()
    config = config.load_default_config()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(config))
