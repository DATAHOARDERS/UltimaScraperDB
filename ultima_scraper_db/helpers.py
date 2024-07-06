from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy_utils.functions.database import _set_url_database  # type: ignore
from sqlalchemy_utils.functions.database import _sqlite_file_exists  # type: ignore
from sqlalchemy_utils.functions.database import make_url  # type: ignore
from sqlalchemy_utils.functions.orm import quote  # type: ignore

if TYPE_CHECKING:
    from pathlib import Path

    from ultima_scraper_db.databases.ultima_archive.site_api import MediaManager
TIMESTAMPTZ = sa.TIMESTAMP(timezone=True)


def selectin_relationship(
    *args: str,
    back_populates: str | None = None,
    **kwargs: Mapped[int] | Mapped[str] | Mapped[int | None],
):
    return relationship(*args, lazy="selectin", back_populates=back_populates, **kwargs)


def subquery_relationship(
    *args: str, back_populates: str | None = None, **kwargs: Mapped[int] | Mapped[str]
):
    return relationship(*args, lazy="subquery", back_populates=back_populates, **kwargs)


async def _get_scalar_result(engine: AsyncEngine, sql: sa.TextClause):
    try:
        async with engine.connect() as conn:
            return await conn.scalar(sql)
    except Exception as _e:
        return False


async def database_exists(url: str):
    url_obj = make_url(url)
    database = url_obj.database
    dialect_name = url_obj.get_dialect().name
    engine = None
    try:
        if dialect_name == "postgresql":
            text = "SELECT 1 FROM pg_database WHERE datname='%s'" % database
            for db in (database, "postgres", "template1", "template0", None):
                url_obj = _set_url_database(url_obj, database=db)
                engine = create_async_engine(url_obj)
                try:
                    return bool(await _get_scalar_result(engine, sa.text(text)))
                except (ProgrammingError, OperationalError):
                    pass
            return False

        elif dialect_name == "mysql":
            url_obj = _set_url_database(url_obj, database=None)
            engine = create_async_engine(url_obj)
            text = (
                "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME = '%s'" % database
            )
            return bool(await _get_scalar_result(engine, sa.text(text)))

        elif dialect_name == "sqlite":
            url_obj = _set_url_database(url_obj, database=None)
            engine = create_async_engine(url_obj)
            if database:
                return database == ":memory:" or _sqlite_file_exists(database)
            else:
                # The default SQLAlchemy database is in memory, and :memory: is
                # not required, thus we should support that use case.
                return True
        else:
            text = "SELECT 1"
            try:
                engine = create_async_engine(url_obj)
                return bool(await _get_scalar_result(engine, sa.text(text)))
            except (ProgrammingError, OperationalError):
                return False
    finally:
        if engine:
            await engine.dispose()


async def create_database(
    url: str, encoding: str = "utf8", template: str | None = None
):
    url_obj = make_url(url)
    database = url_obj.database
    dialect_name = url_obj.get_dialect().name
    dialect_driver = url_obj.get_dialect().driver

    if dialect_name == "postgresql":
        url_obj = _set_url_database(url_obj, database="postgres")
    elif dialect_name == "mssql":
        url_obj = _set_url_database(url_obj, database="master")
    elif dialect_name == "cockroachdb":
        url_obj = _set_url_database(url_obj, database="defaultdb")
    elif not dialect_name == "sqlite":
        url_obj = _set_url_database(url_obj, database=None)

    if (dialect_name == "mssql" and dialect_driver in {"pymssql", "pyodbc"}) or (
        dialect_name == "postgresql"
        and dialect_driver in {"asyncpg", "pg8000", "psycopg2", "psycopg2cffi"}
    ):
        engine = create_async_engine(url_obj, isolation_level="AUTOCOMMIT")
    else:
        engine = create_async_engine(url_obj)

    if dialect_name == "postgresql":
        if not template:
            template = "template1"

        async with engine.begin() as conn:
            text = "CREATE DATABASE {} ENCODING '{}' TEMPLATE {}".format(
                quote(conn, database), encoding, quote(conn, template)  # type: ignore
            )
            await conn.execute(sa.text(text))

    elif dialect_name == "mysql":
        async with engine.begin() as conn:
            text = "CREATE DATABASE {} CHARACTER SET = '{}'".format(
                quote(conn, database), encoding  # type: ignore
            )
            await conn.execute(sa.text(text))

    elif dialect_name == "sqlite" and database != ":memory:":
        if database:
            async with engine.begin() as conn:
                await conn.execute(sa.text("CREATE TABLE DB(id int)"))
                await conn.execute(sa.text("DROP TABLE DB"))

    else:
        async with engine.begin() as conn:
            text = f"CREATE DATABASE {quote(conn, database)}"
            await conn.execute(sa.text(text))

    await engine.dispose()


def find_matching_filepaths(
    media_manager: "MediaManager", temp_filepaths: list["Path"]
):
    """
    Find matching file paths for media items without detections.

    Args:
        media_manager: The media manager from the content manager.
        temp_filepaths: List of temporary file paths to search for matches.

    Returns:
        List of found file paths.
    """
    filepaths: list["Path"] = []

    for _key, item in media_manager.medias.items():
        if not item.media_detections:
            found_filepath: "Path" | None = None
            for db_filepath in item.filepaths:
                for temp_filepath in temp_filepaths:
                    if temp_filepath.name in db_filepath.filepath:
                        found_filepath = temp_filepath
                        break
            if found_filepath:
                filepaths.append(found_filepath)

    return filepaths


def has_detected_media(media_manager: "MediaManager"):
    for _key, item in media_manager.medias.items():
        if item.media_detections:
            return True
    return False
