from typing import TYPE_CHECKING, Any

from sqlalchemy import BigInteger, ForeignKey, MetaData, inspect
from sqlalchemy.dialects.postgresql.base import PGCompiler
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.compiler import compiles  # type: ignore
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import expression

if TYPE_CHECKING:
    from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
        MassMessageModel,
        MessageModel,
        PostModel,
        StoryModel,
    )

    content_model_types = StoryModel | PostModel | MessageModel | MassMessageModel


class CustomFuncs:
    class utcnow(expression.FunctionElement):  # type: ignore
        from sqlalchemy.types import DateTime

        type = DateTime()
        inherit_cache = True

    @compiles(utcnow, "postgresql")
    def pg_utcnow(element, compiler: PGCompiler, **kw: Any):
        return "TIMEZONE('utc', CURRENT_TIMESTAMP)"

    @compiles(utcnow, "mssql")
    def ms_utcnow(element, compiler: PGCompiler, **kw: Any):
        return "GETUTCDATE()"


class DefaultContentTypes(AsyncAttrs):
    __mapper_args__ = {"eager_defaults": True}
    story_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("x_stories.id"), nullable=True
    )
    post_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("x_posts.id"), nullable=True
    )
    message_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("x_messages.id"), nullable=True
    )
    mass_message_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("x_mass_messages.id"), nullable=True
    )
    media_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("media.id"))

    @declared_attr
    def story(self) -> Mapped["StoryModel"]:
        return relationship("StoryModel")

    @declared_attr
    def post(self) -> Mapped["PostModel"]:
        return relationship("PostModel")

    @declared_attr
    def message(self) -> Mapped["MessageModel"]:
        return relationship("MessageModel")

    @declared_attr
    def mass_message(self) -> Mapped["MassMessageModel"]:
        return relationship("MassMessageModel")

    async def get_content(self):
        from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
            ContentMediaAssoModel,
        )

        exclusions = ["media"]

        await self.awaitable_attrs.story
        await self.awaitable_attrs.post
        await self.awaitable_attrs.message
        await self.awaitable_attrs.mass_message
        for key, _ in inspect(ContentMediaAssoModel).relationships.items():
            if any(x for x in exclusions if x == key):
                continue
            final_value: content_model_types = getattr(self, key)
            if final_value:
                return final_value
        raise Exception("Content not set")

    async def get_content_media_asso(self):
        from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
            ContentMediaAssoModel,
        )

        exclusions = ["media"]

        for key, _ in inspect(ContentMediaAssoModel).relationships.items():
            if any(x for x in exclusions if x == key):
                continue
            final_key = f"{key}_id"
            final_value: int = getattr(self, final_key)
            if final_value:
                return final_key, final_value
        raise Exception("Content not set")

    def set_content(self, content: "content_model_types"):
        match type(content).__name__:
            case "StoryModel":
                self.story = content
            case "PostModel":
                self.post = content
            case "MessageModel":
                self.message = content
            case "MassMessageModel":
                self.mass_message = content
            case _:
                raise Exception("Content type not found")


class UltimaBase(AsyncAttrs, DeclarativeBase):
    __mapper_args__ = {"eager_defaults": True}


class ManagementTemplate(UltimaBase):
    __abstract__ = True
    __table_args__ = {"schema": "management"}


class SiteTemplate(UltimaBase):
    __abstract__ = True


from ultima_scraper_api import SUPPORTED_SITES

# Used for alembic upgrades
from ultima_scraper_db.databases.ultima_archive.schemas import management as _
from ultima_scraper_db.databases.ultima_archive.schemas.templates import site as _


def create_merged_metadata(base: UltimaBase | Any, remove_this: dict[str, Any] = {}):
    site_metadatas: list[MetaData] = []
    for supp_site in SUPPORTED_SITES:
        site_metadatas.append(MetaData(schema=supp_site.lower()))
    merged_metadata = MetaData()
    for _k, t in base.metadata.tables.items():
        if not t.schema:
            for site_metadata in site_metadatas:

                def referred_schema_fn(
                    table: Any,
                    to_schema: Any,
                    constraint: Any,
                    referred_schema: str | None,
                ):
                    if referred_schema is not None:
                        return referred_schema
                    return site_metadata.schema

                new_table = t.to_metadata(site_metadata)
                new_table.to_metadata(
                    merged_metadata, referred_schema_fn=referred_schema_fn
                )
        else:
            if remove_this:
                if t.key == remove_this["table"]:
                    my_col_instance = [
                        col for col in t._columns if col.name == remove_this["column"]
                    ][0]
                    t._columns.remove(my_col_instance)
            t.to_metadata(merged_metadata)
    return merged_metadata


merged_metadata = create_merged_metadata(UltimaBase)
