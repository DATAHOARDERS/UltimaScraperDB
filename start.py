import asyncio

from ultima_scraper_collection.config import UltimaScraperCollectionConfig
from ultima_scraper_collection.projects.ultima_archive import UltimaArchiveProject


async def run(config: UltimaScraperCollectionConfig):

    UAP = await UltimaArchiveProject("ultima_archive").init(config)
    UAP.ultima_archive_db_api.activate_api(UAP.fast_api, 2140)
    UAP.ultima_archive_db_api.server.join()


if __name__ == "__main__":
    config = UltimaScraperCollectionConfig()
    config = config.load_default_config()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(config))
