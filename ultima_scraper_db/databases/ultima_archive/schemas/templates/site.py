from datetime import datetime
from typing import TYPE_CHECKING, Any

from inflection import singularize, underscore
from pydantic import BaseModel
from sqlalchemy import (
    BigInteger,
    Boolean,
    Float,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    and_,
    event,
    exists,
    func,
    select,
)
from sqlalchemy.ext.asyncio import async_object_session
from sqlalchemy.ext.compiler import compiles  # type: ignore
from sqlalchemy.orm import Mapped, backref, mapped_column, relationship
from sqlalchemy.orm.attributes import AttributeEventToken
from ultima_scraper_api import SITE_LITERALS
from ultima_scraper_api.apis.fansly.classes.extras import (
    AuthDetails as FanslyAuthDetails,
)
from ultima_scraper_api.apis.onlyfans.classes.extras import (
    AuthDetails as OnlyFansAuthDetails,
)

from ultima_scraper_db.databases.ultima_archive import (
    CustomFuncs,
    DefaultContentTypes,
    SiteTemplate,
)
from ultima_scraper_db.helpers import TIMESTAMPTZ, selectin_relationship

if TYPE_CHECKING:
    from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
        ContentMetadata,
    )

    from ultima_scraper_db.databases.ultima_archive.schemas.management import (
        HostModel,
        SiteModel,
    )
    from ultima_scraper_db.databases.ultima_archive.site_api import ContentManager


standard_unique_constraints = (
    UniqueConstraint(
        "story_id",
        "media_id",
    ),
    UniqueConstraint(
        "post_id",
        "media_id",
    ),
    UniqueConstraint(
        "message_id",
        "media_id",
    ),
    UniqueConstraint(
        "mass_message_id",
        "media_id",
    ),
)


class UserModel(SiteTemplate):
    __tablename__ = "users"
    __allow_unmapped__ = True
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    username: Mapped[str] = mapped_column(Text, nullable=True)
    balance: Mapped[float] = mapped_column(Float, server_default="0")
    spend: Mapped[bool] = mapped_column(Boolean, server_default="false")
    performer: Mapped[bool] = mapped_column(Boolean, server_default="false")
    favorite: Mapped[bool] = mapped_column(Boolean, server_default="false")
    active: Mapped[bool] = mapped_column(Boolean, server_default="true")
    downloaded_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ, nullable=True)
    join_date: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMPTZ, server_default=CustomFuncs.utcnow()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMPTZ, onupdate=CustomFuncs.utcnow(), nullable=True
    )
    user_auths_info: Mapped[list["UserAuthModel"]] = selectin_relationship(
        back_populates="user"
    )
    user_info: Mapped["UserInfoModel"] = relationship(
        foreign_keys="UserInfoModel.user_id", back_populates="user"
    )
    histo_user_infos: Mapped[list["HistoUserInfoModel"]] = relationship(
        foreign_keys="HistoUserInfoModel.user_id", back_populates="user"
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
    favorited: Mapped["FavoriteUserModel"] = relationship(back_populates="user")
    socials: Mapped[list["SocialModel"]] = relationship(back_populates="user")
    mass_message_stats: Mapped[list["MassMessageStatModel"]] = relationship(
        back_populates="user"
    )
    _stories: Mapped[list["StoryModel"]] = relationship()
    _posts: Mapped[list["PostModel"]] = relationship()
    _messages: Mapped[list["MessageModel"]] = relationship(
        primaryjoin="or_(UserModel.id==MessageModel.user_id, "
        "UserModel.id==MessageModel.receiver_id)",
    )
    _mass_messages: Mapped[list["MassMessageModel"]] = relationship(
        back_populates="user"
    )
    medias: Mapped[list["MediaModel"]] = relationship(
        "MediaModel",
        back_populates="user",
    )
    aliases: Mapped[list["UserAliasModel"]] = relationship(back_populates="user")
    jobs: Mapped[list["JobModel"]] = relationship(back_populates="user")

    bought_contents: Mapped[list["BoughtContentModel"]] = relationship(
        foreign_keys="BoughtContentModel.buyer_id"
    )
    supplied_contents: Mapped[list["BoughtContentModel"]] = relationship(
        foreign_keys="BoughtContentModel.supplier_id"
    )
    notifications: Mapped[list["NotificationModel"]] = relationship(
        foreign_keys="NotificationModel.user_id"
    )
    remote_urls: Mapped[list["RemoteURLModel"]] = relationship()
    content_manager: "ContentManager | None" = None

    def get_content_manager(self):
        assert self.content_manager, "Content manager not set"
        return self.content_manager

    def find_auths(self, active: bool | None = True):
        for auth in self.user_auths_info:
            if active is not None and auth.active == active:
                yield auth

    def find_auth(self, active: bool | None = True):
        auths = list(self.find_auths(active))
        if auths:
            return auths[-1]

    async def last_subscription_downloaded_at(self):
        session = async_object_session(self)
        assert session
        stmt = (
            select(SubscriptionModel)
            .where(
                and_(
                    SubscriptionModel.user_id == self.id,
                    SubscriptionModel.downloaded_at.is_not(None),
                )
            )
            .order_by(SubscriptionModel.downloaded_at.desc())
        )
        return await session.scalar(stmt)

    async def update_username(self, username: str):
        await self.awaitable_attrs.aliases
        u_username = f"u{self.id}"
        final_aliases = [
            x for x in self.aliases if x.id is not None and x.username != u_username
        ]
        if username == u_username:
            if self.aliases:
                aliases_sorted_by_id = [x for x in self.aliases if x.id is None]
                aliases_sorted_by_id.extend(
                    sorted(final_aliases, key=lambda x: x.id, reverse=True)
                )
                if not aliases_sorted_by_id:
                    return
                username = aliases_sorted_by_id[0].username
        if self.username != username:
            old_username = self.username
            self.username = username
            await self.add_alias(old_username)

    async def add_alias(self, username: str):
        if self.username != username:
            alias = await self.find_aliases(username)
            if not alias:
                alias = UserAliasModel(username=username)
                self.aliases.append(alias)
            return alias

    async def find_username(self, username: str):
        if self.username == username:
            return self
        alias = await self.find_aliases(username)
        if alias:
            return self

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
        time_now = datetime.now().astimezone()
        valid_subscribers = []
        for x in self.subscribers:
            await x.awaitable_attrs.subscriber
            db_auth = x.subscriber.find_auth()
            if db_auth and db_auth.active and time_now < x.expires_at:
                valid_subscribers.append(x)
        return bool(valid_subscribers)

    async def add_socials(self, socials: list[dict[str, Any]]):
        for social in socials:
            site_name: str = social["socialMedia"]
            identifier = social["username"]
            found_social = await self.find_socials(site_name)
            if not found_social:
                if not identifier:
                    match site_name.lower():
                        case "amazon" | "bereal" | "etsy" | "discord" | "vsco":
                            identifier = social["link"]
                        case "instagram":
                            identifier = social["url"]
                        case _:
                            if site_name == "youtube":
                                if social["username"] or social["link"]:
                                    identifier = social["username"]
                                    if not identifier:
                                        identifier = social["link"]
                                        if not identifier:
                                            pass
                                        else:
                                            pass
                                    else:
                                        pass
                                    pass
                            else:
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
        self,
        server_id: int,
        category: str,
        site: "SiteModel",
        priority: bool = False,
        skippable: bool = False,
    ):
        # Need to create algo to resolve server_id by how many videos user has
        await self.awaitable_attrs.jobs
        job = JobModel(
            site_id=site.id,
            user_username=self.username,
            category=category,
            server_id=server_id,
            priority=priority,
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

    async def find_mass_message(self, identifier: int):
        await self.awaitable_attrs._mass_messages
        for mass_message in self._mass_messages:
            if mass_message.id == identifier:
                return mass_message

    async def find_bought_content(self, performer_id: int):
        await self.awaitable_attrs.bought_contents
        db_bought_contents: list[BoughtContentModel] = []
        for bought_content in self.bought_contents:
            if bought_content.supplier_id == performer_id:
                db_bought_contents.append(bought_content)
        return db_bought_contents

    async def get_supplied_content(self, active: bool | None = None):
        final_supplied_content: list[BoughtContentModel] = []
        await self.awaitable_attrs.supplied_contents
        if active is not None:
            for supplied_content in self.supplied_contents:
                await supplied_content.awaitable_attrs.buyer
                if not supplied_content.buyer.find_auths():
                    continue
                final_supplied_content.append(supplied_content)
        else:
            final_supplied_content = self.supplied_contents
        return final_supplied_content

    async def find_buyers(
        self,
        active: bool | None = None,
        active_user: bool | None = None,
        active_subscription: bool | None = None,
        identifiers: list[int | str] = [],
    ):
        # if active is not None:
        #     active_user = active
        #     active_subscription = active
        temp_buyers: set[UserModel] = set()
        [temp_buyers.add(x.user) for x in self.find_auths()]
        for content in await self.get_supplied_content(active=active_user):
            temp_buyers.add(await content.awaitable_attrs.buyer)
        await self.awaitable_attrs.subscribers
        session = async_object_session(self)
        assert session
        query = (
            select(UserModel)
            .join(SubscriptionModel.subscriber)
            .where(SubscriptionModel.user_id == self.id)
        )
        if active_subscription:
            query = query.where(SubscriptionModel.expires_at >= datetime.now())
        if active_user:
            query = query.where(UserModel.user_auths_info.any(active=active_user))
        subscribers = await session.scalars(query)
        for subscriber in subscribers:
            temp_buyers.add(subscriber)
        for identifier in identifiers:
            for temp_buyer in temp_buyers.copy():
                if isinstance(identifier, int) and temp_buyer.id != identifier:
                    temp_buyers.remove(temp_buyer)
                elif isinstance(identifier, str) and temp_buyer.username != identifier:
                    temp_buyers.remove(temp_buyer)

        final_buyers = list(temp_buyers)
        final_buyers.sort(key=lambda buyer: (buyer.id != self.id, buyer.id))
        return final_buyers

    def find_authed_buyers(self, db_buyers: list["UserModel"]):
        for db_buyer in db_buyers:
            db_auth = db_buyer.find_auth()
            if not db_auth:
                continue
            yield db_auth


class UserAuthModel(SiteTemplate):
    __tablename__ = "user_auths"
    __table_args__ = (UniqueConstraint("cookie", "authorization"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    cookie: Mapped[str] = mapped_column(Text, nullable=True)
    x_bc: Mapped[str] = mapped_column(Text, nullable=True)
    authorization: Mapped[str] = mapped_column(Text, nullable=True)
    user_agent: Mapped[str] = mapped_column(Text)
    email: Mapped[str] = mapped_column(Text, nullable=True)
    password: Mapped[str] = mapped_column(Text, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, server_default="true")
    user: Mapped[UserModel] = selectin_relationship(back_populates="user_auths_info")

    def convert_to_auth_details(self, site_name: SITE_LITERALS):
        if site_name.lower() == "OnlyFans".lower():
            return OnlyFansAuthDetails(
                id=self.user_id,
                username=self.user.username,
                cookie=self.cookie,
                x_bc=self.x_bc,
                user_agent=self.user_agent,
                email=self.email,
                password=self.password,
                active=self.user.active,
            )
        else:
            return FanslyAuthDetails(
                id=self.user_id,
                username=self.user.username,
                authorization=self.authorization,
                user_agent=self.user_agent,
                email=self.email,
                password=self.password,
                active=self.user.active,
            )

    def update(self, info: dict[str, Any]):
        for k, v in info.items():
            if k == "id":
                continue
            setattr(self, k, v)

    async def activate(self):
        self.active = True

    async def deactivate(self):
        self.active = False


class UserInfoModel(SiteTemplate):
    __tablename__ = "user_infos"

    __table_args__ = (UniqueConstraint("user_id"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(Text, nullable=True)
    price: Mapped[int] = mapped_column(Float, server_default="0", default=0)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    promotion: Mapped[bool] = mapped_column(
        Boolean, server_default="true", default=True
    )
    sex: Mapped[int] = mapped_column(SmallInteger, nullable=True, index=True)
    post_count: Mapped[int] = mapped_column(Integer, server_default="0", default=0)
    media_count: Mapped[int] = mapped_column(Integer, server_default="0", default=0)
    image_count: Mapped[int] = mapped_column(Integer, server_default="0", default=0)
    video_count: Mapped[int] = mapped_column(Integer, server_default="0", default=0)
    audio_count: Mapped[int] = mapped_column(Integer, server_default="0", default=0)
    stream_count: Mapped[int] = mapped_column(Integer, server_default="0", default=0)
    archived_post_count: Mapped[int] = mapped_column(
        Integer, server_default="0", default=0
    )
    private_archived_post_count: Mapped[int] = mapped_column(
        Integer, server_default="0", default=0
    )
    favourited_count: Mapped[int] = mapped_column(
        Integer, server_default="0", default=0
    )
    favourites_count: Mapped[int] = mapped_column(
        Integer, server_default="0", default=0
    )
    subscribers_count: Mapped[int] = mapped_column(
        Integer, server_default="0", default=0
    )
    size: Mapped[int] = mapped_column(BigInteger, server_default="0", default=0)
    location: Mapped[str | None] = mapped_column(Text, nullable=True)
    website: Mapped[str | None] = mapped_column(Text, nullable=True)
    downloaded_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=True)
    first_downloaded_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=True)
    uploaded_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=True)
    user: Mapped["UserModel"] = relationship(back_populates="user_info")


class HistoUserInfoModel(SiteTemplate):
    __tablename__ = "histo_user_infos"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    name: Mapped[str] = mapped_column(Text, nullable=True)
    price: Mapped[int] = mapped_column(Float, server_default="0")
    description: Mapped[str] = mapped_column(Text, nullable=True)
    promotion: Mapped[bool] = mapped_column(
        Boolean, server_default="true", default=True
    )
    post_count: Mapped[int] = mapped_column(Integer, server_default="0")
    media_count: Mapped[int] = mapped_column(Integer, server_default="0")
    image_count: Mapped[int] = mapped_column(Integer, server_default="0")
    video_count: Mapped[int] = mapped_column(Integer, server_default="0")
    audio_count: Mapped[int] = mapped_column(Integer, server_default="0")
    stream_count: Mapped[int] = mapped_column(Integer, server_default="0")
    archived_post_count: Mapped[int] = mapped_column(Integer, server_default="0")
    private_archived_post_count: Mapped[int] = mapped_column(
        Integer, server_default="0"
    )
    favourited_count: Mapped[int] = mapped_column(Integer, server_default="0")
    favourites_count: Mapped[int] = mapped_column(Integer, server_default="0")
    subscribers_count: Mapped[int] = mapped_column(Integer, server_default="0")
    size: Mapped[int] = mapped_column(BigInteger, server_default="0")
    location: Mapped[str] = mapped_column(Text, nullable=True)
    website: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, server_default=func.now())
    user: Mapped["UserModel"] = relationship(back_populates="histo_user_infos")


class ContentMediaAssoModel(DefaultContentTypes, SiteTemplate):
    __tablename__ = "content_media_asso"
    __table_args__ = standard_unique_constraints + (
        Index("ix_content_media_story_id", "story_id"),
        Index("ix_content_media_post_id", "post_id"),
        Index("ix_content_media_message_id", "message_id"),
        Index("ix_content_media_mass_message_id", "mass_message_id"),
        Index("ix_content_media_media_id", "media_id"),
    )
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    media: Mapped["MediaModel"] = relationship(
        "MediaModel", back_populates="content_media_assos"
    )

    def get_key(self, match_value: str):
        from sqlalchemy import inspect

        for key in inspect(ContentMediaAssoModel).columns.keys():
            final_match_value = singularize(underscore(match_value)).lower()
            if final_match_value in key.lower():
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
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    url: Mapped[str] = mapped_column(Text, nullable=True)
    size: Mapped[int] = mapped_column(BigInteger, server_default="0", default="0")
    category: Mapped[str] = mapped_column(String, nullable=True)
    preview: Mapped[bool] = mapped_column(Boolean, server_default="false")
    created_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ)
    user: Mapped["UserModel"] = relationship(back_populates="medias")
    filepaths: Mapped[list["FilePathModel"]] = relationship(
        "FilePathModel",
        back_populates="media",
    )
    media_detections: Mapped[list["MediaDetectionModel"]] = selectin_relationship(
        "MediaDetectionModel",
        back_populates="media",
    )

    content_media_assos: Mapped[list[ContentMediaAssoModel]] = relationship(
        back_populates="media"
    )

    async def get_contents(self):
        await self.awaitable_attrs.content_media_assos
        db_contents = [await x.get_content() for x in self.content_media_assos]
        return db_contents

    async def find_content(self, api_type: str, content_id: int):
        content_type = api_type if api_type != "Stories" else "Story"
        await self.awaitable_attrs.content_media_assos
        for content_media_asso in self.content_media_assos:
            try:
                key = content_media_asso.get_key(content_type)
                assert key
                value = getattr(content_media_asso, key)
                if value and value != content_id:
                    continue
                if value:
                    return await content_media_asso.get_content()
            except Exception as _e:
                return

    def find_filepath(self, content_info: tuple[int, str] | None = None):
        if content_info:
            content_id, content_type = content_info
            content_type = content_type if content_type != "Stories" else "Story"
            temp_type = singularize(underscore(content_type)).lower()

            for filepath in self.filepaths:
                fp_content_id = getattr(filepath, f"{temp_type}_id", None)
                if fp_content_id == content_id:
                    return filepath
        else:
            from ultima_scraper_collection.managers.content_manager import (
                DefaultCategorizedContent,
            )

            content_types = DefaultCategorizedContent()
            for temp_db_filepath in self.filepaths:
                valid = all(
                    not hasattr(
                        temp_db_filepath, f"{singularize(underscore(key)).lower()}_id"
                    )
                    or not getattr(
                        temp_db_filepath, f"{singularize(underscore(key)).lower()}_id"
                    )
                    for key, _ in content_types
                )

                if valid:
                    return temp_db_filepath


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
        for media in self.media:
            if media.id == media_id:
                return media


class LinkedPostModel(ContentTemplate):
    __tablename__ = "linked_posts"
    id: Mapped[int] = mapped_column(primary_key=True)
    post_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("x_posts.id"))
    linked_post_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("x_posts.id"))
    post: Mapped["PostModel"] = relationship("PostModel", foreign_keys=post_id)
    linked_post: Mapped["PostModel"] = relationship(
        "PostModel",
        foreign_keys=linked_post_id,
    )


class LinkedUserModel(ContentTemplate):
    __tablename__ = "linked_users"
    id: Mapped[int] = mapped_column(primary_key=True)
    post_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("x_posts.id"))
    linked_user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    post: Mapped["PostModel"] = relationship(
        "PostModel",
        foreign_keys=post_id,
    )
    linked_user: Mapped["UserModel"] = relationship(
        "UserModel",
        foreign_keys=linked_user_id,
    )


class PostModel(ContentTemplate):
    __tablename__ = "x_posts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    text: Mapped[str] = mapped_column(Text, nullable=True)
    price: Mapped[float] = mapped_column(Float, server_default="0")
    deleted: Mapped[bool] = mapped_column(Boolean, server_default="false")
    archived: Mapped[bool] = mapped_column(Boolean, server_default="false")
    paid: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    media_count = mapped_column(Integer, server_default="0", default=0)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)

    user: Mapped["UserModel"] = relationship(
        foreign_keys=user_id,
    )
    comments: Mapped[list["CommentModel"]] = relationship()

    media = relationship(
        "MediaModel",
        secondary=ContentMediaAssoModel.__table__,
        backref=backref("posts", lazy="joined"),
        lazy="joined",
    )

    # Relationship for posts linked by the current post
    linked_posts: Mapped[list["PostModel"]] = relationship(
        "PostModel",
        secondary=LinkedPostModel.__table__,
        primaryjoin=and_(
            LinkedPostModel.post_id == id,
        ),
        secondaryjoin=LinkedPostModel.linked_post_id == id,
    )

    # Relationship for posts to which the current post is linked
    linked_by_posts: Mapped[list["PostModel"]] = relationship(
        "PostModel",
        secondary=LinkedPostModel.__table__,
        primaryjoin=and_(
            LinkedPostModel.linked_post_id == id,
        ),
        secondaryjoin=LinkedPostModel.post_id == id,
    )
    linked_users: Mapped[list["UserModel"]] = relationship(
        "UserModel",
        secondary=LinkedUserModel.__table__,
        primaryjoin=and_(
            LinkedUserModel.post_id == id,
        ),
        secondaryjoin=LinkedUserModel.linked_user_id == UserModel.id,
    )

    async def find_media(self, media_id: int):
        for media in self.media:
            if media.id == media_id:
                return media

    def update(self, content: "ContentMetadata"):
        pass


class MessageModel(ContentTemplate):
    __tablename__ = "x_messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    receiver_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id"), nullable=True
    )
    queue_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("x_mass_messages.id"), nullable=True
    )
    text: Mapped[str] = mapped_column(Text, nullable=True)
    price: Mapped[float] = mapped_column(Float, server_default="0")
    deleted: Mapped[bool] = mapped_column(Boolean, server_default="false")
    paid: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    verified: Mapped[bool] = mapped_column(Boolean, server_default="false")
    media_count = mapped_column(Integer, server_default="0", default=0)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)

    user: Mapped["UserModel"] = relationship(
        foreign_keys=user_id,
    )
    media = relationship(
        "MediaModel",
        secondary=ContentMediaAssoModel.__table__,
        backref=backref("messages", lazy="joined"),
        lazy="joined",
    )

    async def find_media(self, media_id: int):
        for media in self.media:
            if media.id == media_id:
                return media

    def update(self, content: "ContentMetadata"):
        assert content.user_id
        self.user_id = content.user_id
        self.receiver_id = content.receiver_id
        if content.__soft__.is_mass_message():
            self.queue_id = content.queue_id


class MassMessageStatModel(SiteTemplate):
    __tablename__ = "mass_message_stats"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    media_count: Mapped[int] = mapped_column(SmallInteger)
    buyer_count: Mapped[int] = mapped_column(SmallInteger)
    sent_count: Mapped[int] = mapped_column(Integer)
    view_count: Mapped[int] = mapped_column(Integer)
    user: Mapped[UserModel] = relationship(back_populates="mass_message_stats")


class MassMessageModel(ContentTemplate):
    __tablename__ = "x_mass_messages"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    statistic_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("mass_message_stats.id"), nullable=True
    )
    text: Mapped[str] = mapped_column(Text, nullable=True)
    price: Mapped[float] = mapped_column(Float)
    media_count = mapped_column(Integer, server_default="0", default=0)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)

    user: Mapped[UserModel] = relationship(back_populates="_mass_messages")
    mass_message_stat: Mapped["MassMessageStatModel"] = relationship(
        foreign_keys=statistic_id,
    )

    media: Mapped[list["MediaModel"]] = relationship(
        "MediaModel",
        secondary=ContentMediaAssoModel.__table__,
        primaryjoin=and_(
            ContentMediaAssoModel.mass_message_id == id,
        ),
        secondaryjoin=ContentMediaAssoModel.media_id == MediaModel.id,
        backref="mass_messages",
    )

    async def find_media(self, media_id: int):
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
    downloaded_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ, nullable=True)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)
    renewed_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)

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
    __table_args__ = standard_unique_constraints

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    filepath: Mapped[str] = mapped_column(String(255))

    preview: Mapped[bool] = mapped_column(Boolean, server_default="false")
    downloaded: Mapped[bool] = mapped_column(Boolean, server_default="false")
    media: Mapped["MediaModel"] = relationship(
        "MediaModel",
        back_populates="filepaths",
    )


class UserAliasModel(SiteTemplate):
    __tablename__ = "user_aliases"
    __table_args__ = (UniqueConstraint("user_id", "username"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    username: Mapped[str] = mapped_column(String(50))
    user: Mapped["UserModel"] = relationship(back_populates="aliases")


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
    host_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("management.hosts.id"), nullable=True
    )
    skippable: Mapped[bool] = mapped_column(Boolean, server_default="false")

    priority: Mapped[bool] = mapped_column(Boolean, server_default="false")
    active: Mapped[bool] = mapped_column(Boolean, server_default="true")
    completed_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ, nullable=True)
    user: Mapped[UserModel] = relationship(back_populates="jobs")
    site: Mapped["SiteModel"] = selectin_relationship(foreign_keys=site_id)
    host: Mapped["HostModel"] = selectin_relationship(foreign_keys=host_id)


class FavoriteUserModel(SiteTemplate):
    __tablename__ = "favorite_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id"), unique=True
    )
    notification: Mapped[bool] = mapped_column(Boolean, server_default="true")
    user: Mapped[UserModel] = relationship(back_populates="favorited")


class NotificationModel(SiteTemplate):
    __tablename__ = "notifications"
    __table_args__ = (UniqueConstraint("user_id", "authed_user_id", "category"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("users.id"), nullable=True
    )
    authed_user_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("users.id"), nullable=True
    )
    category: Mapped[str] = mapped_column(String)
    sent_discord: Mapped[bool] = mapped_column(Boolean, server_default="false")
    sent_telegram: Mapped[bool] = mapped_column(Boolean, server_default="false")
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMPTZ, server_default=CustomFuncs.utcnow()
    )

    user: Mapped["UserModel"] = relationship(foreign_keys=user_id)
    authed_user: Mapped["UserModel"] = relationship(foreign_keys=authed_user_id)


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


class RemoteURLModel(SiteTemplate):
    __tablename__ = "remote_urls"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    host_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("management.hosts.id"))
    root_id: Mapped[int | None] = mapped_column(
        Text, nullable=True, server_default=None
    )
    url: Mapped[str] = mapped_column(Text, nullable=True)
    part: Mapped[int] = mapped_column(Integer, server_default="0")
    exists: Mapped[bool] = mapped_column(Boolean, server_default="False")
    uploaded_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ)
    downloaded_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=True)
    user: Mapped[UserModel] = relationship(back_populates="remote_urls")


class MediaDetectionModel(SiteTemplate):
    __tablename__ = "media_detections"
    id: Mapped[int] = mapped_column(primary_key=True)

    media_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("media.id"))
    label: Mapped[str] = mapped_column(Text)

    # Female-specific labels
    face_female: Mapped[bool] = mapped_column(Boolean, server_default="false")
    female_genitalia_covered: Mapped[bool] = mapped_column(
        Boolean, server_default="false"
    )
    female_genitalia_exposed: Mapped[bool] = mapped_column(
        Boolean, server_default="false"
    )
    female_breast_covered: Mapped[bool] = mapped_column(Boolean, server_default="false")
    female_breast_exposed: Mapped[bool] = mapped_column(Boolean, server_default="false")

    # Male-specific labels
    face_male: Mapped[bool] = mapped_column(Boolean, server_default="false")
    male_genitalia_exposed: Mapped[bool] = mapped_column(
        Boolean, server_default="false"
    )
    male_breast_exposed: Mapped[bool] = mapped_column(Boolean, server_default="false")

    # Ambiguous labels
    buttocks_covered: Mapped[bool] = mapped_column(Boolean, server_default="false")
    buttocks_exposed: Mapped[bool] = mapped_column(Boolean, server_default="false")
    anus_covered: Mapped[bool] = mapped_column(Boolean, server_default="false")
    anus_exposed: Mapped[bool] = mapped_column(Boolean, server_default="false")
    feet_covered: Mapped[bool] = mapped_column(Boolean, server_default="false")
    feet_exposed: Mapped[bool] = mapped_column(Boolean, server_default="false")
    belly_covered: Mapped[bool] = mapped_column(Boolean, server_default="false")
    belly_exposed: Mapped[bool] = mapped_column(Boolean, server_default="false")
    armpits_covered: Mapped[bool] = mapped_column(Boolean, server_default="false")
    armpits_exposed: Mapped[bool] = mapped_column(Boolean, server_default="false")

    # Other labels
    score: Mapped[int] = mapped_column(Float, server_default="0")
    x_coord: Mapped[int | None] = mapped_column(Integer)
    y_coord: Mapped[int | None] = mapped_column(Integer)
    width: Mapped[int | None] = mapped_column(Integer)
    height: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMPTZ, server_default=CustomFuncs.utcnow()
    )
    media: Mapped["MediaModel"] = relationship()

    class MediaDetectionFilter(BaseModel):
        label: str
        score: float

    def filter_stmt(
        self,
        filters: list[MediaDetectionFilter],
        sex: int | None,
        user_id: int | None = None,
        category: str | None = None,
    ):
        # Start with a base query
        base_query = (
            select(FilePathModel)
            .join(FilePathModel.media)
            .join(MediaModel.user)
            .join(UserModel.user_info)
        )

        # Add conditions for filtering by user_id and sex
        if user_id is not None:
            base_query = base_query.where(UserModel.id == user_id)
        if sex is not None:
            base_query = base_query.where(UserInfoModel.sex == sex)
        if category is not None:
            base_query = base_query.where(MediaModel.category == category)
        # Construct the filter conditions
        for media_filter in filters:
            subquery = (
                exists()
                .where(
                    and_(
                        FilePathModel.media_id == MediaDetectionModel.media_id,
                        MediaDetectionModel.label.contains(media_filter.label),
                        MediaDetectionModel.score >= media_filter.score,
                    )
                )
                .correlate(FilePathModel)
            )
            base_query = base_query.where(subquery)

        # Order the query
        stmt = base_query.distinct().order_by(FilePathModel.media_id)

        return stmt


content_models = [StoryModel, PostModel, MessageModel, MassMessageModel]
