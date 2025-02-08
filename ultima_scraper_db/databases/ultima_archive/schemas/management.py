from sqlalchemy import BigInteger, Boolean, SmallInteger, Text, orm, select
from sqlalchemy.orm import Mapped, mapped_column
from ultima_scraper_db.databases.ultima_archive import ManagementTemplate
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    JobModel,
    UserModel,
)
from ultima_scraper_db.managers.database_manager import Schema


class ServerModel(ManagementTemplate):
    __tablename__ = "servers"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    ip: Mapped[str] = mapped_column(Text)
    mac_address: Mapped[str] = mapped_column(Text)
    job_limit: Mapped[int] = mapped_column(SmallInteger, server_default="10")
    active: Mapped[int] = mapped_column(SmallInteger, server_default="1")


class SiteModel(ManagementTemplate):
    __tablename__ = "sites"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    db_name: Mapped[str] = mapped_column(Text)
    url: Mapped[str] = mapped_column(Text)
    active: Mapped[bool] = mapped_column(Boolean, server_default="true")
    size: Mapped[int] = mapped_column(BigInteger, server_default="0")
    user_id_checkpoint: Mapped[int] = mapped_column(BigInteger, nullable=True)


class HostModel(ManagementTemplate):
    __tablename__ = "hosts"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    identifier: Mapped[str] = mapped_column(Text, nullable=True)
    password: Mapped[str] = mapped_column(Text, nullable=True)
    source: Mapped[bool] = mapped_column(Boolean, server_default="True")
    active: Mapped[bool] = mapped_column(Boolean, server_default="True")


default_sites = [
    SiteModel(name="OnlyFans", db_name="onlyfans", url="https://onlyfans.com"),
    SiteModel(name="Fansly", db_name="fansly", url="https://fansly.com"),
]
