from typing import TYPE_CHECKING

from sqlalchemy import ScalarResult, Select, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ultima_scraper_db.databases.ultima_archive.schemas.management import (
    HostModel,
    ServerModel,
    SiteModel,
)
from ultima_scraper_db.managers.database_manager import Schema

if TYPE_CHECKING:
    from ultima_scraper_collection import datascraper_types


class ManagementAPI:
    def __init__(
        self,
        schema: Schema,
        datascraper: "datascraper_types | None" = None,
    ) -> None:
        self.database = schema.database
        self.schema = schema
        self.datascraper = datascraper

    async def __aenter__(self):
        self._session: AsyncSession = self.schema.sessionmaker()
        return self

    async def __aexit__(self, exc_type: None, exc_value: None, traceback: None):
        await self._session.commit()
        await self._session.aclose()

    def resolve_session(self):
        assert self._session, "Session has not been set"
        return self._session

    def get_session(self):
        return self.resolve_session()

    async def get_sites(self):
        stmt = select(SiteModel)
        result = await self.get_session().execute(stmt)
        return result.scalars().all()

    async def get_server(self, server_id: int | None, server_name: str | None):
        stmt = select(ServerModel).where(
            or_(
                ServerModel.id == server_id,
                ServerModel.name == server_name,
            )
        )
        result = await self.get_session().scalar(stmt)
        return result

    async def get_hosts(self, host_id: int | None = None, host_name: str | None = None):
        stmt = select(HostModel)
        if host_id:
            stmt = stmt.where(HostModel.id == host_id)
        if host_name:
            stmt = stmt.where(HostModel.name == host_name)
        result = await self.get_session().scalars(stmt)
        return result.all()

    async def create_or_update_host(self, db_host: HostModel):
        stmt = (
            select(HostModel)
            .where(HostModel.name == db_host.name)
            .where(HostModel.identifier == db_host.identifier)
        )
        result = await self.get_session().scalar(stmt)
        if not result:
            self.get_session().add(db_host)
        return db_host
