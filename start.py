import argparse
import asyncio
from pathlib import Path
from typing import Any

from alembic.script.base import Script
from ultima_scraper_collection.config import UltimaScraperCollectionConfig

import ultima_scraper_db
from ultima_scraper_db import ALEMBICA_PATH
from ultima_scraper_db.databases.ultima_archive import merged_metadata
from ultima_scraper_db.databases.ultima_archive.database_api import ArchiveAPI
from ultima_scraper_db.managers.database_manager import Alembica, DatabaseManager

MIDDLEWARE: list[tuple[type, dict[str, Any]]] = []

parser = argparse.ArgumentParser()
parser.add_argument("--dev", action="store_true", help="Enable dev mode")


async def run(
    config: UltimaScraperCollectionConfig,
    args: argparse.Namespace = parser.parse_args(),
):
    ultima_scraper_db.dev_mode = args.dev
    db_manager = DatabaseManager()
    db_config = config.settings.databases[0].connection_info.model_dump()
    # alembica_path = (
    #     Path("ultima_scraper_db/databases/ultima_archive").resolve().as_posix()
    # )
    database = db_manager.create_database(
        **db_config, alembica=Alembica(ALEMBICA_PATH), metadata=merged_metadata
    )
    await database.init_db()
    # current_rev = await database.generate_migration()
    # if isinstance(current_rev, Script):
    #     await database.run_migrations()
    ultima_archive_db_api = ArchiveAPI(database)
    await ultima_archive_db_api.init()
    await ultima_archive_db_api.activate_fast_api(
        await database.clone(),
        config,
        "127.0.0.1" if not args.dev else "0.0.0.0",
        2140,
    )
    ultima_archive_db_api.server.join()


if __name__ == "__main__":
    config = UltimaScraperCollectionConfig()
    config = config.load_or_create_default_config()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(config))
