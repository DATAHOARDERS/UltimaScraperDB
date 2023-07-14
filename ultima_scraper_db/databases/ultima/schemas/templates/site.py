from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    Float,
    ForeignKey,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    and_,
    select,
)
from sqlalchemy.ext.asyncio import async_object_session
from sqlalchemy.ext.compiler import compiles  # type: ignore
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ultima_scraper_db.databases.ultima import (
    CustomFuncs,
    DefaultContentTypes,
    SiteTemplate,
)
from ultima_scraper_db.helpers import TIMESTAMPTZ, selectin_relationship

if TYPE_CHECKING:
    from ultima_scraper_db.databases.ultima.schemas.management import SiteModel


class UserModel(SiteTemplate):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    username: Mapped[str] = mapped_column(Text, nullable=True)
    balance: Mapped[float] = mapped_column(Float, server_default="0")
    spend: Mapped[bool] = mapped_column(Boolean, server_default="false")
    performer: Mapped[bool] = mapped_column(Boolean, server_default="false")
    favorite: Mapped[bool] = mapped_column(Boolean, server_default="false")
    active: Mapped[bool] = mapped_column(Boolean, server_default="true")
    downloaded_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=True)
    last_checked_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=True)
    join_date: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMPTZ, server_default=CustomFuncs.utcnow()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMPTZ, onupdate=CustomFuncs.utcnow(), nullable=True
    )
    user_auth_info: Mapped["UserAuthModel"] = selectin_relationship()
    user_infos: Mapped[list["UserInfoModel"]] = relationship(
        foreign_keys="UserInfoModel.user_id", back_populates="user"
    )

    subscriptions: Mapped[list["SubscriptionModel"]] = relationship(
        "SubscriptionModel",
        foreign_keys="SubscriptionModel.subscriber_id",
        back_populates="subscriber",
    )

    subscribers: Mapped[list["SubscriptionModel"]] = relationship(
        "SubscriptionModel",
        foreign_keys="SubscriptionModel.user_id",
    )

    socials: Mapped[list["SocialModel"]] = relationship(back_populates="user")
    _stories: Mapped[list["StoryModel"]] = relationship()
    _posts: Mapped[list["PostModel"]] = relationship()
    _messages: Mapped[list["MessageModel"]] = relationship(
        primaryjoin="or_(UserModel.id==MessageModel.user_id, "
        "UserModel.id==MessageModel.receiver_id)",
    )
    aliases: Mapped[list["UserAliasModel"]] = relationship(back_populates="user")
    jobs: Mapped[list["JobModel"]] = relationship(back_populates="user")

    bought_contents: Mapped[list["BoughtContentModel"]] = relationship(
        foreign_keys="BoughtContentModel.buyer_id"
    )
    supplied_contents: Mapped[list["BoughtContentModel"]] = relationship(
        foreign_keys="BoughtContentModel.supplier_id"
    )

    @property
    def content_manager(self):
        from ultima_scraper_db.managers.site_db import ContentManager

        return ContentManager(self)

    async def find_content(self):
        pass

    async def add_alias(self, username: str):
        if self.username != username:
            alias = await self.find_aliases(self.username)
            if not alias:
                alias = UserAliasModel(username=self.username)
                self.aliases.append(alias)
            self.username = username
            return alias

    async def find_aliases(self, username: str):
        await self.awaitable_attrs.aliases
        for alias in self.aliases:
            if alias.username == username:
                return alias

    async def find_subscription(self, user_id: int):
        await self.awaitable_attrs.subscriptions
        for subscription in self.subscriptions:
            if subscription.user_id == user_id:
                return subscription

    async def has_active_subscription(self):
        await self.awaitable_attrs.subscribers
        return bool(
            [x for x in self.subscribers if datetime.now().astimezone() < x.expires_at]
        )

    async def find_user_info(self, username: str):
        await self.awaitable_attrs.user_infos
        for user_info in self.user_infos:
            if user_info.username == username:
                return user_info

    async def add_socials(self, socials: list[dict[str, Any]]):
        for social in socials:
            site_name: str = social["socialMedia"]
            identifier = social["username"]
            found_social = await self.find_socials(site_name)
            if not found_social:
                if not identifier:
                    match site_name.lower():
                        case "amazon" | "etsy" | "discord":
                            identifier = social["link"]
                        case "instagram":
                            identifier = social["url"]
                        case _:
                            raise Exception("social identifier not found")
                    pass
                db_social = SocialModel(site=site_name, identifier=identifier)
                self.socials.append(db_social)
                pass

    async def find_socials(self, site_name: str, identifier: str | None = None):
        await self.awaitable_attrs.socials
        for social in self.socials:
            if social.site == site_name:
                if social.identifier == identifier:
                    return social
                if identifier is None:
                    return social

    async def add_job(
        self, server_id: int, category: str, site: "SiteModel", skippable: bool = False
    ):
        # Need to create algo to resolve server_id by how many videos user has
        await self.awaitable_attrs.jobs
        job = JobModel(
            site_id=site.id,
            user_username=self.username,
            category=category,
            server_id=server_id,
            skippable=skippable,
        )
        self.jobs.append(job)
        return job

    async def find_job(self, category: str):
        found_job = None
        await self.awaitable_attrs.jobs
        for job in self.jobs:
            if job.category == category:
                found_job = job
        return found_job

    async def find_bought_content(self, performer_id: int):
        await self.awaitable_attrs.bought_contents
        for bought_content in self.bought_contents:
            if bought_content.supplier_id == performer_id:
                return bought_content

    async def get_supplied_content(self, active: bool = False):
        final_supplied_content: list[BoughtContentModel] = []
        await self.awaitable_attrs.supplied_contents
        if active:
            for supplied_content in self.supplied_contents:
                await supplied_content.awaitable_attrs.buyer
                if not supplied_content.buyer.user_auth_info.active:
                    continue
                final_supplied_content.append(supplied_content)
        else:
            final_supplied_content = self.supplied_contents
        return final_supplied_content

    async def find_buyers(self, active: bool = False):
        temp_buyers: set[UserModel] = set()
        for content in await self.get_supplied_content(active=active):
            temp_buyers.add(await content.awaitable_attrs.buyer)
        await self.awaitable_attrs.subscribers
        session = async_object_session(self)
        assert session
        query = (
            select(UserModel)
            .join(SubscriptionModel.subscriber)
            .where(SubscriptionModel.user_id == self.id)
        )
        if active:
            query = query.where(SubscriptionModel.expires_at >= datetime.now()).where(
                UserModel.user_auth_info.has(active=True)
            )
        subscribers = await session.scalars(query)
        for subscriber in subscribers:
            temp_buyers.add(subscriber)
        final_buyers = temp_buyers
        return list(final_buyers)

    async def activate(self):
        self.user_auth_info.active = True
        await self.awaitable_attrs.subscriptions
        for subscription in self.subscriptions:
            await subscription.awaitable_attrs.user
            if subscription.user.active:
                subscription.active = True
            else:
                pass
        pass

    async def deactivate(self):
        await self.awaitable_attrs.subscriptions
        self.user_auth_info.active = False
        for subscription in self.subscriptions:
            subscription.active = False


class UserAuthModel(SiteTemplate):
    __tablename__ = "user_auths"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    cookie: Mapped[str] = mapped_column(Text, nullable=True)
    x_bc: Mapped[str] = mapped_column(Text, nullable=True)
    authorization: Mapped[str] = mapped_column(Text, nullable=True)
    user_agent: Mapped[str] = mapped_column(Text)
    email: Mapped[str] = mapped_column(Text, nullable=True)
    password: Mapped[str] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, server_default="true")

    def update(self, info: dict[str, Any]):
        for k, v in info.items():
            if k == "id":
                continue
            setattr(self, k, v)


class UserInfoModel(SiteTemplate):
    __tablename__ = "user_infos"

    __table_args__ = (UniqueConstraint("user_id", "username"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    username: Mapped[str] = mapped_column(Text, nullable=True)
    name: Mapped[str] = mapped_column(Text, nullable=True)
    price: Mapped[int] = mapped_column(Float, server_default="0")
    description: Mapped[str] = mapped_column(Text, nullable=True)
    promotion: Mapped[bool] = mapped_column(
        Boolean, server_default="true", default="true"
    )
    sex: Mapped[int] = mapped_column(SmallInteger, nullable=True)
    post_count: Mapped[int] = mapped_column(Integer, server_default="0")
    image_count: Mapped[int] = mapped_column(Integer, server_default="0")
    video_count: Mapped[int] = mapped_column(Integer, server_default="0")
    audio_count: Mapped[int] = mapped_column(Integer, server_default="0")
    favourited_count: Mapped[int] = mapped_column(Integer, server_default="0")
    size: Mapped[int] = mapped_column(BigInteger, server_default="0")
    location: Mapped[str] = mapped_column(Text, nullable=True)
    website: Mapped[str] = mapped_column(Text, nullable=True)
    downloaded_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=True)
    uploaded_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=True)
    user: Mapped["UserModel"] = relationship(back_populates="user_infos")


class ContentMediaAssoModel(DefaultContentTypes, SiteTemplate):
    __tablename__ = "content_media_asso"
    __table_args__ = (
        UniqueConstraint("story_id", "post_id", "message_id", "media_id"),
    )
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    media: Mapped["MediaModel"] = relationship(
        "MediaModel", back_populates="content_media_assos"
    )

    def get_key(self, match_value: str):
        from sqlalchemy import inspect

        for key in inspect(ContentMediaAssoModel).columns.keys():
            if match_value.lower() in key.lower():
                return key
        pass

    def get_user(self):
        from sqlalchemy import inspect

        exclusions = ["media"]

        for key, _ in inspect(ContentMediaAssoModel).relationships.items():
            if any(x for x in exclusions if x == key):
                continue
            final_value = getattr(self, key)
            if final_value:
                user: UserModel = final_value.user
                return user
        raise Exception("User not set")


class MediaModel(SiteTemplate):
    __tablename__ = "media"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    url: Mapped[str] = mapped_column(Text, nullable=True)
    size: Mapped[int] = mapped_column(BigInteger, server_default="0", default="0")
    category: Mapped[str] = mapped_column(String, nullable=True)
    preview: Mapped[bool] = mapped_column(Boolean, server_default="false")
    created_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)
    filepaths: Mapped[list["FilePathModel"]] = relationship(
        "FilePathModel",
        # back_populates="media",
    )

    content_media_assos: Mapped[list[ContentMediaAssoModel]] = relationship(
        back_populates="media"
    )

    async def find_filepath(self, content_id: int, content_type: str):
        content_type = content_type if "Stories" != content_type else "Story"
        await self.awaitable_attrs.filepaths
        for filepath in self.filepaths:
            fp_content_id = getattr(
                filepath, f"{content_type.lower().removesuffix('s')}_id"
            )
            if fp_content_id == content_id:
                return filepath


class ContentTemplate(SiteTemplate):
    __abstract__ = True


class StoryModel(ContentTemplate):
    __tablename__ = "x_stories"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)

    user: Mapped["UserModel"] = relationship(
        foreign_keys=user_id,
    )

    media: Mapped[list["MediaModel"]] = relationship(
        "MediaModel",
        secondary=ContentMediaAssoModel.__table__,
        primaryjoin=and_(
            ContentMediaAssoModel.story_id == id,
        ),
        secondaryjoin=ContentMediaAssoModel.media_id == MediaModel.id,
        backref="stories",
    )

    async def find_media(self, media_id: int):
        await self.awaitable_attrs.media
        for media in self.media:
            if media.id == media_id:
                return media


class PostModel(ContentTemplate):
    __tablename__ = "x_posts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    text: Mapped[str] = mapped_column(Text, nullable=True)
    price: Mapped[float] = mapped_column(Float, server_default="0")
    deleted: Mapped[bool] = mapped_column(Boolean, server_default="false")
    archived: Mapped[bool] = mapped_column(Boolean, server_default="false")
    paid: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)

    user: Mapped["UserModel"] = relationship(
        foreign_keys=user_id,
    )

    media: Mapped[list["MediaModel"]] = relationship(
        "MediaModel",
        secondary=ContentMediaAssoModel.__table__,
        primaryjoin=and_(
            ContentMediaAssoModel.post_id == id,
        ),
        secondaryjoin=ContentMediaAssoModel.media_id == MediaModel.id,
        backref="posts",
    )

    async def find_media(self, media_id: int):
        await self.awaitable_attrs.media
        for media in self.media:
            if media.id == media_id:
                return media


class MessageModel(ContentTemplate):
    __tablename__ = "x_messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    receiver_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id"), nullable=True
    )
    text: Mapped[str] = mapped_column(Text, nullable=True)
    price: Mapped[float] = mapped_column(Float, server_default="0")
    deleted: Mapped[bool] = mapped_column(Boolean, server_default="false")
    paid: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    verified: Mapped[bool] = mapped_column(Boolean, server_default="false")
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)

    user: Mapped["UserModel"] = relationship(
        foreign_keys=user_id,
    )
    media: Mapped[list["MediaModel"]] = relationship(
        "MediaModel",
        secondary=ContentMediaAssoModel.__table__,
        primaryjoin=and_(
            ContentMediaAssoModel.message_id == id,
        ),
        secondaryjoin=ContentMediaAssoModel.media_id == MediaModel.id,
        backref="messages",
    )

    async def find_media(self, media_id: int):
        await self.awaitable_attrs.media
        for media in self.media:
            if media.id == media_id:
                return media


class CommentModel(SiteTemplate):
    __tablename__ = "comments"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    post_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("x_posts.id"))
    reply_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("comments.id"), nullable=True
    )
    user_id: Mapped[int] = mapped_column(BigInteger)
    giphy_id: Mapped[str | None] = mapped_column(String, server_default=None)
    text: Mapped[str | None] = mapped_column(String, server_default=None)
    likes_count: Mapped[int] = mapped_column(Integer, server_default="0")
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)


class SubscriptionModel(SiteTemplate):
    __tablename__ = "subscriptions"
    __table_args__ = (UniqueConstraint("user_id", "subscriber_id"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    subscriber_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    paid_content: Mapped[bool] = mapped_column(Boolean, server_default="0")
    active: Mapped[bool] = mapped_column(Boolean, server_default="true")
    downloaded_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=True)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)

    user: Mapped["UserModel"] = relationship(
        "UserModel",
        foreign_keys="SubscriptionModel.user_id",
    )
    subscriber: Mapped["UserModel"] = relationship(
        "UserModel",
        back_populates="subscribers",
        foreign_keys="SubscriptionModel.subscriber_id",
    )


class FilePathModel(DefaultContentTypes, SiteTemplate):
    __tablename__ = "filepaths"
    __table_args__ = (
        UniqueConstraint("story_id", "post_id", "message_id", "media_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    filepath: Mapped[str] = mapped_column(String(255))

    preview: Mapped[bool] = mapped_column(Boolean, server_default="false")
    downloaded: Mapped[bool] = mapped_column(Boolean, server_default="false")
    # media: Mapped["MediaModel"] = relationship(
    #     "MediaModel",
    #     # back_populates="filepaths",
    # )


class UserAliasModel(SiteTemplate):
    __tablename__ = "user_aliases"
    __table_args__ = (UniqueConstraint("user_id", "username"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    username: Mapped[str] = mapped_column(String(50))
    user: Mapped["UserModel"] = relationship(back_populates="aliases")


from sqlalchemy import event
from sqlalchemy.orm.attributes import AttributeEventToken


@event.listens_for(SubscriptionModel.downloaded_at, "set", propagate=True)
def update_downloaded_at(
    subscription: SubscriptionModel,
    value: datetime,
    oldvalue: datetime,
    initiator: AttributeEventToken,
):
    if value != oldvalue:
        subscription.user.downloaded_at = value


class JobModel(SiteTemplate):
    __tablename__ = "jobs"
    __table_args__ = (UniqueConstraint("site_id", "user_id", "category"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    site_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("management.sites.id"))
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    user_username: Mapped[str] = mapped_column(Text)
    category: Mapped[str] = mapped_column(Text)
    server_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("management.servers.id")
    )
    skippable: Mapped[bool] = mapped_column(Boolean, server_default="false")
    active: Mapped[bool] = mapped_column(Boolean, server_default="true")
    completed_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ, nullable=True)
    user: Mapped[UserModel] = relationship(back_populates="jobs")
    site: Mapped["SiteModel"] = relationship(foreign_keys=site_id)


class NotificationModel(SiteTemplate):
    __tablename__ = "notifications"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id"), nullable=True
    )
    category: Mapped[str] = mapped_column(String)
    sent_discord: Mapped[bool] = mapped_column(Boolean, server_default="false")
    sent_telegram: Mapped[bool] = mapped_column(Boolean, server_default="false")
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMPTZ, server_default=CustomFuncs.utcnow()
    )

    user: Mapped["UserModel"] = selectin_relationship()


class SocialModel(SiteTemplate):
    __tablename__ = "socials"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    site: Mapped[str] = mapped_column(String)
    identifier: Mapped[str] = mapped_column(String)
    user: Mapped[UserModel] = relationship(back_populates="socials")


class BoughtContentModel(SiteTemplate):
    __tablename__ = "bought_content"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    supplier_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    buyer_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))

    supplier: Mapped[UserModel] = relationship(foreign_keys=supplier_id)
    buyer: Mapped[UserModel] = relationship(foreign_keys=buyer_id)
