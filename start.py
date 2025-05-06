import asyncio

from ultima_scraper_collection.config import UltimaScraperCollectionConfig

from ultima_scraper_db.databases.ultima_archive import merged_metadata
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
    # current_rev = await database.generate_migration()
    # if isinstance(current_rev, Script):
    #     await database.run_migrations()
    ultima_archive_db_api = ArchiveAPI(database)
    await ultima_archive_db_api.init()
    await ultima_archive_db_api.fast_api.init(config)
    ultima_archive_db_api.activate_api(ultima_archive_db_api.fast_api, 2140)
    ultima_archive_db_api.server.join()


if __name__ == "__main__":
    config = UltimaScraperCollectionConfig()
    config = config.load_default_config()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(config))
