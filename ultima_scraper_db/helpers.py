import sqlalchemy as sa
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy_utils.functions.database import _set_url_database  # type: ignore
from sqlalchemy_utils.functions.database import _sqlite_file_exists  # type: ignore
from sqlalchemy_utils.functions.database import make_url  # type: ignore
from sqlalchemy_utils.functions.orm import quote  # type: ignore
from ultima_scraper_api import user_types
from ultima_scraper_api.apis.onlyfans.classes.user_model import (
    create_user as OFUserModel,
)

from ultima_scraper_db.databases.ultima.schemas.templates.site import UserModel


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


async def is_valuable(user: UserModel | user_types):
    # Checks if performer has active subscription or has supplied content to a buyer
    # We can add a "valid flag on get_supplied_content to return active authed users"
    if isinstance(user, UserModel):
        if await user.has_active_subscription() or await user.get_supplied_content():
            return True
        else:
            return False
    else:
        if user.isPerformer:
            if isinstance(user, OFUserModel):
                if (
                    user.subscribedIsExpiredNow == False
                    or await user.get_paid_contents()
                ):
                    return True
                else:
                    return False
            else:
                # We need to add paid_content checker
                if user.following:
                    return True
                else:
                    return False
        else:
            return False
