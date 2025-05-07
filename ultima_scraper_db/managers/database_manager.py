import itertools
import random
from pathlib import Path
from typing import TYPE_CHECKING, Any

import paramiko
import uvicorn
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations.ops import MigrationScript
from alembic.runtime.environment import EnvironmentContext
from alembic.script.base import ScriptDirectory
from alembic.util import CommandError
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Connection, MetaData, inspect
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sshtunnel import SSHTunnelForwarder  # type: ignore

from ultima_scraper_db.databases.rest_api import RestAPI
from ultima_scraper_db.helpers import create_database, database_exists

if TYPE_CHECKING:
    from ultima_scraper_db.databases.ultima_archive.api.client import UAClient


class Alembica:
    def __init__(
        self,
        database_path: str | Path | None = None,
        generate: bool = False,
        migrate: bool = False,
    ) -> None:
        if not database_path:
            self.database_path = Path(__file__).parent.parent.joinpath(
                "databases/ultima_archive"
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
        self.is_migrate = migrate


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
        ssh: SSHTunnelForwarder | dict[str, Any] | None,
        metadata: MetaData | None = None,
        alembica: Alembica | None = None,
    ) -> None:
        self.name = name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        if isinstance(ssh, dict):
            ssh = self.handle_ssh(ssh, host, port)
        if ssh:
            ssh.start()
        self.ssh: SSHTunnelForwarder | None = ssh
        self.schemas: dict[str, Schema] = {}
        self.metadata = metadata
        self.alembica = alembica
        self._echo = True

    async def clone(self):
        database = Database(
            self.name,
            self.host,
            self.port,
            self.username,
            self.password,
            self.ssh,
            self.metadata,
            self.alembica,
        )
        return await database.init_db(self._echo)

    def handle_ssh(
        self, ssh_auth_info: dict[str, Any], local_host: str, local_port: int
    ):
        if ssh_auth_info["host"]:
            private_key_filepath = ssh_auth_info["private_key_filepath"]
            ssh_private_key_password = ssh_auth_info["private_key_password"]
            private_key = (
                paramiko.RSAKey.from_private_key_file(
                    private_key_filepath, ssh_private_key_password
                ).key
                if private_key_filepath
                else None
            )
            random_port = random.randint(6000, 6999)
            ssh_obj = SSHTunnelForwarder(
                (ssh_auth_info["host"], ssh_auth_info["port"]),
                ssh_username=ssh_auth_info["username"],
                ssh_pkey=private_key,
                ssh_private_key_password=ssh_private_key_password,
                remote_bind_address=(local_host, local_port),
                local_bind_address=(local_host, random_port),
            )
            return ssh_obj
        else:
            return None

    async def init_db(self, echo: bool = False):
        self._echo = echo
        if self.ssh:
            connection_string = f"postgresql+asyncpg://{self.username}:{self.password}@{self.ssh.local_bind_host}:{self.ssh.local_bind_port}/{self.name}"  # type: ignore
        else:
            connection_string = f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.name}"
        engine = create_async_engine(
            connection_string,
            echo=echo,
            pool_size=30,
            max_overflow=20,
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
        assert self.alembica
        if self.alembica.is_generate:
            await self.generate_migration()
            await self.resolve_schemas()
        if self.alembica.is_migrate:
            await self.run_migrations()
        await engine.dispose()
        return self

    async def resolve_schemas(self):
        connection_string = str(self.engine.url.render_as_string(hide_password=False))
        async with self.engine.connect() as conn:
            schema_strings: list[str] = await conn.run_sync(show_schemas)
        assert schema_strings
        for schema_string in schema_strings:
            engine = create_async_engine(
                connection_string,
                echo=self._echo,
                execution_options={
                    "application_name": schema_string,
                    "schema_translate_map": {None: schema_string},
                },
                pool_size=30,
                max_overflow=20,
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
                    revision = await conn.run_sync(
                        run_revision, alembic_cfg, True, self.metadata
                    )
                    return revision
                except CommandError as _e:
                    await self.run_migrations()
                    pass
            else:
                revision = await conn.run_sync(
                    run_revision, alembic_cfg, True, self.metadata
                )
                return revision
            pass

    async def run_migrations(self) -> None:
        if not self.alembica:
            raise RuntimeError("Alembic configuration is missing.")

        alembic_cfg = self.alembica.config
        script = ScriptDirectory.from_config(alembic_cfg)

        def run_upgrade(connection: Connection, cfg: Config):
            cfg.attributes["connection"] = connection  # type:ignore
            command.upgrade(cfg, "head")
            return True

        try:
            head_rev = script.get_current_head()
            if head_rev:
                current_rev = script.get_revision(head_rev)
                with open(current_rev.path) as migration_file:
                    migration_content = migration_file.read().split(
                        "def downgrade() -> None:"
                    )[0]
                    if "drop_" in migration_content:
                        if (
                            input(
                                "Database migration contains drop statements. Continue? (y/n): "
                            ).lower()
                            != "y"
                        ):
                            raise RuntimeError("Migration aborted by user.")

            async with self.engine.connect() as conn:
                await conn.run_sync(run_upgrade, alembic_cfg)

        except Exception as e:
            print(f"Migration error: {e}")
            raise

    async def run_downgrade(self, version: str) -> None:
        def downgrade(connection: Connection, cfg: Config):
            cfg.attributes["connection"] = connection
            command.downgrade(alembic_cfg, version)

        async with self.engine.connect() as conn:
            assert self.alembica
            alembic_cfg = self.alembica.config
            await conn.run_sync(downgrade, alembic_cfg)


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
        ssh: SSHTunnelForwarder | dict[str, Any],
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


def thread_function(fast_api: "UAClient", port: int):
    uvicorn.run(  # type: ignore
        fast_api,
        host="0.0.0.0",
        port=port,
        log_level="debug",
    )


class DatabaseAPI_:
    def __init__(self, database: Database) -> None:
        self.database = database

    def activate_api(self, fast_api: "RestAPI", port: int):
        from multiprocessing import Process

        origins = [
            "*",
        ]
        fast_api.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        x = Process(
            target=thread_function,
            args=(
                fast_api,
                port,
            ),
            daemon=True,
        )
        x.start()
        self.server = x

    def find_schema(self, name: str):
        return self.database.schemas[name]
