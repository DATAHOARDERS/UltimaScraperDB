import itertools
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations.ops import MigrationScript
from alembic.util import CommandError  # type: ignore
from sqlalchemy import Connection, MetaData, inspect
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sshtunnel import SSHTunnelForwarder  # type: ignore

from ultima_scraper_db.helpers import create_database, database_exists


class Alembica:
    def __init__(
        self, database_path: str | Path | None = None, generate: bool = False
    ) -> None:
        if not database_path:
            self.database_path = Path(__file__).parent.parent.joinpath(
                "databases/ultima"
            )
        else:
            self.database_path = Path(database_path)
        assert self.database_path.exists(), "Invalid database directory"
        self.migration_directory = self.database_path.joinpath("alembic")
        self.config = Config(self.database_path.joinpath("alembic.ini"))
        self.config.set_main_option(
            "script_location",
            self.migration_directory.as_posix(),
        )
        self.is_generate = generate


class SSHConnection:
    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        private_key: bytes,
        private_key_password: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.private_key = private_key
        self.password = private_key_password


class Schema:
    def __init__(
        self,
        name: str,
        engine: AsyncEngine,
        session: AsyncSession,
        database: "Database",
    ) -> None:
        self.name = name
        self.engine = engine
        self.session = session
        self.sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        self.database = database


def show_schemas(conn: Connection):
    inspector = inspect(conn)
    schemas = inspector.get_schema_names()
    return schemas


class Database:
    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        username: str,
        password: str,
        ssh: SSHTunnelForwarder | None,
        metadata: MetaData | None = None,
        alembica: Alembica | None = None,
    ) -> None:
        self.name = name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        if ssh:
            ssh.start()
        self.ssh: SSHTunnelForwarder | None = ssh
        self.schemas: dict[str, Schema] = {}
        self.metadata = metadata
        self.alembica = alembica

    async def init_db(self):
        if self.ssh:
            connection_string = f"postgresql+asyncpg://{self.username}:{self.password}@{self.ssh.local_bind_host}:{self.ssh.local_bind_port}/{self.name}"  # type: ignore
        else:
            connection_string = f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.name}"
        engine = create_async_engine(
            connection_string,
            echo=True,
        )
        self.engine = engine

        if not await database_exists(connection_string):
            await self.setup()
        from sqlalchemy.schema import CreateSchema

        async with engine.connect() as conn:

            def create_schemas(session: Connection):
                if not self.metadata:
                    return
                for schema_name in self.metadata._schemas:  # type: ignore
                    session.execute(CreateSchema(schema_name, if_not_exists=True))
                session.commit()

            await conn.run_sync(create_schemas)
            await self.resolve_schemas()
        return self

    async def resolve_schemas(self):
        connection_string = str(self.engine.url.render_as_string(hide_password=False))
        async with self.engine.connect() as conn:
            schema_strings: list[str] = await conn.run_sync(show_schemas)
            assert schema_strings
            for schema_string in schema_strings:
                engine = create_async_engine(
                    connection_string,
                    echo=True,
                    execution_options={"schema_translate_map": {None: schema_string}},
                    pool_size=20,
                )
                async_session = AsyncSession(engine, expire_on_commit=False)
                schema_obj = Schema(schema_string, engine, async_session, self)

                self.schemas[schema_obj.name] = schema_obj
            return self.schemas

    async def create(self):
        await create_database(self.engine.url.render_as_string(hide_password=False))

    def get_schema(self, name: str):
        return self.schemas[name.lower()]

    async def setup(self):
        await self.create()

    async def generate_migration(self):
        def get_current_revision(connection: Connection):
            context = MigrationContext.configure(connection)
            current_rev = context.get_current_revision()
            return current_rev

        def process_revision_directives(
            context: MigrationContext,
            revision: tuple[str, str],
            directives: list[Any],
        ):
            revisions: list[MigrationScript] = directives
            # Prevent actually generating a migration
            has_upgrades = list(
                itertools.chain.from_iterable(
                    op.as_diffs()
                    for script in revisions
                    for op in script.upgrade_ops_list
                )
            )
            if not has_upgrades:
                directives[:] = []

        def run_revision(
            connection: Connection,
            cfg: Config,
            autogenerate: bool,
            local_metadata: MetaData,
        ):
            cfg.attributes["connection"] = connection  # type:ignore
            revision = command.revision(
                alembic_cfg,
                autogenerate=autogenerate,
                process_revision_directives=process_revision_directives,
            )
            return revision

        async with self.engine.connect() as conn:
            current_rev = await conn.run_sync(get_current_revision)
            assert self.alembica
            alembic_cfg = self.alembica.config
            if not current_rev:
                try:
                    _revision = await conn.run_sync(
                        run_revision, alembic_cfg, True, self.metadata
                    )
                    pass
                except CommandError as _e:
                    await self.run_migrations()
                    pass
            else:
                _revision = await conn.run_sync(
                    run_revision, alembic_cfg, True, self.metadata
                )
                pass
            pass

    async def run_migrations(self) -> None:
        def run_upgrade(connection: Connection, cfg: Config):
            cfg.attributes["connection"] = connection  # type:ignore
            command.upgrade(alembic_cfg, "head")
            return True

        while True:
            try:
                async with self.engine.connect() as conn:
                    assert self.alembica
                    alembic_cfg = self.alembica.config
                    _upgraded = await conn.run_sync(run_upgrade, alembic_cfg)
                    break
            except Exception as e:
                print(e)
                pass


class DatabaseManager:
    def __init__(self) -> None:
        self.databases: dict[str, Database] = {}

    def create_database(
        self,
        name: str,
        host: str,
        port: int,
        username: str,
        password: str,
        ssh: SSHTunnelForwarder,
        metadata: MetaData | None = None,
        alembica: Alembica | None = None,
    ):
        database = Database(
            name, host, port, username, password, ssh, metadata, alembica
        )
        return database

    def add_database(self, database: Database):
        self.databases[database.name] = database
        return database

    def resolve_database(self, name: str):
        return self.databases[name]
