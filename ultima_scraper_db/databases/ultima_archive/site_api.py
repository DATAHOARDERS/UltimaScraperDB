import asyncio
import gc
import weakref
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Sequence, Type
from urllib.parse import ParseResult

import ultima_scraper_api
from alive_progress import alive_bar  # type: ignore
from inflection import underscore
from sqlalchemy import ScalarResult, Select, UnaryExpression, delete, or_, orm, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.session import async_object_session
from sqlalchemy.orm import joinedload, noload, selectinload
from sqlalchemy.sql import and_, exists, func, select, union_all
from ultima_scraper_api.apis.fansly.classes.extras import (
    AuthDetails as FanslyAuthDetails,
)
from ultima_scraper_api.apis.onlyfans.classes.auth_model import OnlyFansAuthModel
from ultima_scraper_api.apis.onlyfans.classes.comment_model import (
    CommentModel as OFCommentModel,
)
from ultima_scraper_api.apis.onlyfans.classes.extras import (
    AuthDetails as OnlyFansAuthDetails,
)
from ultima_scraper_api.apis.onlyfans.classes.user_model import (
    UserModel as OFUserModel,
)
from ultima_scraper_api.helpers.main_helper import split_string
from ultima_scraper_collection.helpers.main_helper import is_notif_valuable
from ultima_scraper_collection.managers.aio_pika_wrapper import (
    AioPikaWrapper,
    create_notification,
)

from ultima_scraper_db.databases.ultima_archive.filters import AuthedInfoFilter
from ultima_scraper_db.databases.ultima_archive.schemas.management import SiteModel
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    BoughtContentModel,
    CommentModel,
    ContentMediaAssoModel,
    ContentTemplate,
    FilePathModel,
    JobModel,
    MassMessageModel,
    MassMessageStatModel,
    MediaModel,
    MessageModel,
    NotificationModel,
    PostModel,
    RemoteURLModel,
    StoryModel,
    SubscriptionModel,
    UserAliasModel,
    UserAuthModel,
    UserInfoModel,
    UserModel,
)
from ultima_scraper_db.managers.database_manager import Schema

content_model_types = StoryModel | PostModel | MessageModel | MassMessageModel
from ultima_scraper_api import post_types

if TYPE_CHECKING:
    from ultima_scraper_collection import datascraper_types
    from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
        ContentMetadata,
        MediaMetadata,
    )

from sqlalchemy import inspect
from sqlalchemy.orm.strategy_options import _AbstractLoad  # type: ignore


def create_options(
    alias: bool = False,
    user_info: bool = False,
    content: bool = False,
    media: bool = False,
    notifications: bool = False,
    remote_urls: bool = False,
):
    joined_options: list[_AbstractLoad] = []
    if alias:
        stmt = selectinload(UserModel.aliases)
        joined_options.append(stmt)
    if content:
        inspector = inspect(UserModel)
        for relationship_name, relationship in inspector.mapper.relationships.items():
            if issubclass(relationship.mapper.class_, ContentTemplate):
                relationship_attribute = getattr(UserModel, relationship_name)
                inspector2 = inspect(relationship_attribute)
                content_relationship = inspector2.mapper.relationships
                stmt = selectinload(relationship)  # type: ignore
                if hasattr(content_relationship, "media"):
                    stmt = stmt.selectinload(
                        content_relationship["media"]
                    ).selectinload(MediaModel.filepaths)
                joined_options.append(stmt)
    if media:
        stmt = selectinload(UserModel.medias).options(
            selectinload(MediaModel.filepaths),
            selectinload(MediaModel.content_media_assos),
        )
        joined_options.append(stmt)
    if user_info:
        stmt = selectinload(UserModel.user_info)
        joined_options.append(stmt)
    if notifications:
        stmt = selectinload(UserModel.notifications)
        joined_options.append(stmt)
    if remote_urls:
        stmt = selectinload(UserModel.remote_urls)
        joined_options.append(stmt)
    return joined_options


def fix_missing_paid_content(
    db_content: PostModel | MessageModel,
    content: "ContentMetadata",
    datascraper: "datascraper_types",
    api_performer: ultima_scraper_api.user_types,
):
    if bool(db_content.paid) == False and content.price and content.price > 0:
        fmu = datascraper.filesystem_manager.get_file_manager(api_performer.id)
        valid_local_media_count = 0
        for file in fmu.files:
            remote_media_date = content.__soft__.created_at
            remote_post_date = content.__soft__.created_at
            stat = file.stat()
            mt = stat.st_mtime
            local_media_date = datetime.fromtimestamp(mt).replace(microsecond=0)
            local_post_date = datetime.fromtimestamp(mt).replace(microsecond=0)
            if (
                local_media_date == remote_media_date
                or local_post_date == remote_post_date
            ):
                valid_local_media_count += 1
        if valid_local_media_count == content.__soft__.media_count:
            db_content.paid = True


class FilePathManager:
    def __init__(self, content_manager: "ContentManager") -> None:
        self.content_manager = content_manager
        self.filepaths: dict[int, FilePathModel] = {
            filepath.id: filepath
            for media in content_manager.media_manager.medias.values()
            for filepath in media.filepaths
        }
        self._filepaths_str: dict[str, FilePathModel] = {
            Path(item.filepath).name: item for item in self.filepaths.values()
        }

    def resolve_filepath(self, identifier: int | str):
        filepath = self.filepaths.get(identifier)
        if not filepath:
            filepath = self._filepaths_str.get(identifier)
        return filepath


class MediaManager:
    def __init__(self, content_manager: "ContentManager") -> None:
        self.content_manager = content_manager
        self.medias: dict[int, MediaModel] = {}

    async def init(self, media_ids: list[int] = []):
        content_manager = self.content_manager
        user = content_manager.__user__
        for media in user.medias:
            self.medias[media.id] = media
        # remove duplicate ids from media_ids by self.media_ids
        final_media_ids = [x for x in media_ids if x not in self.medias]
        if media_ids:
            stmt = (
                select(MediaModel)
                .where(MediaModel.id.in_(final_media_ids))
                .options(
                    joinedload(MediaModel.filepaths),
                    joinedload(MediaModel.content_media_assos),
                )
            )
            assert content_manager.session
            found_medias = await content_manager.session.scalars(stmt)
            for media in found_medias.unique():
                self.medias[media.id] = media
        self.filepath_manager = FilePathManager(content_manager)

    def find_media(self, media_id: int):
        try:
            return self.medias[media_id]
        except KeyError:
            return

    def add_media(self, media_model: MediaModel):
        self.medias[media_model.id] = media_model


class ContentManager:
    def __init__(
        self,
        user: "UserModel",
    ):
        self.__user__ = user
        self.session = async_object_session(user)
        self.lock = asyncio.Lock()
        self.media_manager = MediaManager(self)

    async def init(self, load_media: bool = False, media_ids: list[int] = []):
        awaitables = [
            self.__user__.awaitable_attrs._stories,
            self.__user__.awaitable_attrs._posts,
            self.__user__.awaitable_attrs._messages,
            self.__user__.awaitable_attrs._mass_messages,
        ]
        await asyncio.gather(*awaitables)

        if not self.session:
            self.session = async_object_session(self.__user__)

        media_manager = self.media_manager
        self.stories: list["StoryModel"] = self.__user__._stories  # type: ignore
        stories_media = [content.awaitable_attrs.media for content in self.stories]

        self.posts: list["PostModel"] = self.__user__._posts  # type: ignore
        posts_media = [content.awaitable_attrs.media for content in self.posts]

        self.messages: list["MessageModel"] = self.__user__._messages  # type: ignore
        messages_media = [content.awaitable_attrs.media for content in self.messages]

        self.mass_messages: list["MassMessageModel"] = self.__user__._mass_messages  # type: ignore
        mass_messages_media = [
            content.awaitable_attrs.media for content in self.mass_messages
        ]

        if load_media:
            stories_media_items = await asyncio.gather(*stories_media)
            for media_list in stories_media_items:
                for item in media_list:
                    media_manager.add_media(item)
            posts_media_items = await asyncio.gather(*posts_media)
            for media_list in posts_media_items:
                for item in media_list:
                    media_manager.add_media(item)
            messages_media_items = await asyncio.gather(*messages_media)
            for media_list in messages_media_items:
                for item in media_list:
                    media_manager.add_media(item)
            mass_messages_media_items = await asyncio.gather(*mass_messages_media)
            for media_list in mass_messages_media_items:
                for item in media_list:
                    media_manager.add_media(item)
            await media_manager.init(media_ids=media_ids)
        return self

    def get_media_manager(self):
        return self.media_manager

    def get_filepath_manager(self):
        return self.get_media_manager().filepath_manager

    async def get_contents(self, content_type: str | None = None):
        if content_type:
            # if not hasattr(self, content_type.lower()):
            #     empty_list: content_model_types = []
            #     return empty_list
            result: list[content_model_types] = getattr(
                self, underscore(content_type).lower()
            )
            return result
        return self.stories + self.posts + self.messages + self.mass_messages

    async def add_content(self, content: "ContentMetadata"):
        match content.api_type:
            case "Stories":
                content_model = StoryModel(
                    id=content.content_id,
                    user_id=content.user_id,
                    created_at=content.__soft__.created_at,
                )
                self.stories.append(content_model)
            case "Posts" | "Archived/Posts":
                content_model = PostModel(
                    id=content.content_id,
                    user_id=content.user_id,
                    text=content.text,
                    price=content.price,
                    paid=int(bool(content.paid)),
                    created_at=content.__soft__.created_at,
                )
                self.posts.append(content_model)
            case "Messages":
                queue_id = (
                    content.queue_id if content.__soft__.is_mass_message() else None
                )
                content_model = MessageModel(
                    id=content.content_id,
                    user_id=content.user_id,
                    receiver_id=content.receiver_id,
                    text=content.text,
                    price=content.price,
                    paid=int(bool(content.paid)),
                    verified=True,
                    queue_id=queue_id,
                    created_at=content.__soft__.created_at,
                )
                self.messages.append(content_model)
                assert content.receiver_id
                content_model.receiver_id = content.receiver_id
                pass
            case "MassMessages":
                stat = content.get_mass_message_stat()
                content_model = MassMessageModel(
                    id=content.content_id,
                    user_id=content.user_id,
                    statistic_id=stat.id if stat else None,
                    text=content.text,
                    price=content.price,
                    expires_at=content.__soft__.expires_at,
                    created_at=content.__soft__.created_at,
                )
                self.mass_messages.append(content_model)
            case _:
                raise Exception("Content not assigned")
        return content_model

    async def find_content(
        self,
        content_id: int,
        content_type: str | None = None,
    ):
        temp_contents = await self.get_contents(content_type=content_type)
        for content in temp_contents:
            if content.id == content_id:
                return content

    async def find_paid_contents(
        self, only_downloaded: bool = False
    ) -> Sequence[MediaModel]:
        session = self.session
        assert session

        # Adjust common conditions based on only_downloaded parameter
        if only_downloaded:
            common_conditions = and_(
                MediaModel.filepaths.any(
                    FilePathModel.downloaded == True
                ),  # Assuming MediaFilePath is the model for filepaths
                MediaModel.preview == False,
            )
        else:
            common_conditions = and_(
                MediaModel.filepaths.any(), MediaModel.preview == False
            )

        # PostModel conditions
        post_conditions = and_(
            PostModel.user_id == self.__user__.id,
            PostModel.price > 0,
            PostModel.paid == True,
        )

        # MessageModel conditions
        message_conditions = and_(
            MessageModel.user_id == self.__user__.id,
            MessageModel.price > 0,
            MessageModel.paid == True,
        )

        stmt1 = (
            select(MediaModel)
            .join(PostModel.media)
            .options(selectinload(MediaModel.filepaths))
            .where(post_conditions, common_conditions)
        )

        stmt2 = (
            select(MediaModel)
            .join(MessageModel.media)
            .options(selectinload(MediaModel.filepaths))
            .where(message_conditions, common_conditions)
        )

        union_stmt = union_all(stmt1, stmt2)
        orm_stmt = select(MediaModel).from_statement(union_stmt)
        result = await session.scalars(orm_stmt)
        return result.unique().all()

    async def size_sum(self):
        session = self.session
        assert session

        stmt = select(func.sum(MediaModel.size)).where(
            MediaModel.user_id == self.__user__.id
        )

        result = await session.scalar(stmt)
        final_sum = int(result or 0)
        return final_sum

    async def media_sum(self, category: str):
        session = async_object_session(self.__user__)
        assert session

        stmt1 = (
            select(func.count())
            .where(MediaModel.category == category)
            .where(MediaModel.filepaths.any())
            .join(StoryModel.media)
            .where(StoryModel.user_id == self.__user__.id)
        )
        stmt2 = (
            select(func.count())
            .where(MediaModel.category == category)
            .where(MediaModel.filepaths.any())
            .join(PostModel.media)
            .where(PostModel.user_id == self.__user__.id)
        )
        stmt3 = (
            select(func.count())
            .where(MediaModel.category == category)
            .where(MediaModel.filepaths.any())
            .join(MessageModel.media)
            .where(MessageModel.user_id == self.__user__.id)
        )
        result1 = await session.scalar(stmt1)
        result2 = await session.scalar(stmt2)
        result3 = await session.scalar(stmt3)
        final_sum = sum((result1 or 0, result2 or 0, result3 or 0))
        return final_sum


class StatementBuilder:
    def __init__(self, model: Type[UserModel | UserInfoModel]) -> None:
        self.model = model
        self.statement: Select[Any] = select(model)

    def filter_by_user_identifiers(
        self, identifiers: list[int | str] | int | str | None
    ):
        if not identifiers:
            return self
        if isinstance(identifiers, str):
            identifiers = identifiers.replace("@", "")
            identifiers = identifiers.replace(" ", ",")
            final_identifiers = split_string(identifiers)
        else:
            if isinstance(identifiers, int):
                identifiers = [identifiers]
            final_identifiers = [str(x) for x in identifiers]
        template_query = [
            UserModel.username.in_([x for x in final_identifiers if not x.isdigit()]),
            UserModel.id.in_([int(x) for x in final_identifiers if x.isdigit()]),
        ]
        if self.model == UserModel:
            self.statement = self.statement.filter(or_(*template_query))
        else:
            self.statement = self.statement.join(UserModel).filter(or_(*template_query))

        return self

    def filter_by_description(self, description: str | None):
        if not description:
            return self
        description = description.lower()
        if self.model == UserModel:
            self.statement = self.statement.join(UserInfoModel).filter(
                UserInfoModel.description.ilike(f"%{description}%")
            )
        else:
            self.statement = self.statement.join(UserModel).filter(
                UserInfoModel.description.ilike(f"%{description}%")
            )
        return self


class SiteAPI:
    def __init__(
        self,
        schema: Schema,
        datascraper: "datascraper_types | None" = None,
    ) -> None:
        self.database = schema.database
        self.schema = schema
        self.datascraper = datascraper
        self.content_managers: dict[int, "weakref.ref[ContentManager]"] = {}
        self.aio_pika_wrapper: AioPikaWrapper | None = None

    async def __aenter__(self):
        self._session: AsyncSession = self.schema.sessionmaker()
        return self

    async def __aexit__(self, exc_type: None, exc_value: None, traceback: None):
        await self._session.commit()
        await self._session.aclose()
        self.content_managers.clear()
        gc.collect()

    def resolve_session(self):
        assert self._session, "Session has not been set"
        return self._session

    def get_session(self):
        return self.resolve_session()

    def set_session(self, session: AsyncSession):
        self._session = session

    def get_session_maker(self):
        return self.schema.sessionmaker()

    def resolve_content_manager(self, user: UserModel):
        content_manager = self.content_managers.get(user.id)
        if content_manager:
            content_manager = content_manager()
        if not content_manager:
            content_manager = user.content_manager
            if not content_manager:
                content_manager = ContentManager(user)
                user.content_manager = content_manager
            self.content_managers[user.id] = weakref.ref(content_manager)

        return content_manager

    def get_user_query(
        self,
        identifiers: list[int | str] | int | str | None = None,
        description: str | None = None,
        performer: bool | None = None,
        has_paid_content: bool | None = None,
        load_aliases: bool = False,
        load_user_info: bool = False,
        load_content: bool = False,
        load_media: bool = False,
        load_notifications: bool = False,
        load_remote_urls: bool = False,
        limit: int | None = None,
        extra_options: Any = (),
    ):
        options = create_options(
            content=load_content,
            media=load_media,
            user_info=load_user_info,
            alias=load_aliases,
            notifications=load_notifications,
            remote_urls=load_remote_urls,
        )
        options += extra_options
        stmt_builder = (
            StatementBuilder(UserModel)
            .filter_by_user_identifiers(identifiers)
            .filter_by_description(description)
        )
        stmt = stmt_builder.statement.options(*options).distinct().limit(limit)
        if performer is not None:
            stmt = stmt.where(UserModel.performer == performer)
        if has_paid_content is not None:
            stmt = (
                stmt.where(UserModel.supplied_contents.any())
                .join(UserModel.supplied_contents)
                .where(UserAuthModel.active == True)
            )
        return stmt

    async def get_users(
        self,
        identifiers: list[int | str] | str | None = None,
        description: str | None = None,
        performer: bool | None = None,
        has_paid_content: bool | None = None,
        load_aliases: bool = False,
        load_user_info: bool = True,
        load_content: bool = False,
        load_media: bool = False,
        load_notifications: bool = False,
        load_remote_urls: bool = False,
        limit: int | None = None,
        extra_options: Any = (),
        authed_info_filter: AuthedInfoFilter | None = None,
        order_by: UnaryExpression[Any] | None = None,
    ):
        if identifiers:
            if isinstance(identifiers, str):
                identifiers = [identifiers]
            for i, identifier in enumerate(identifiers):
                if (
                    identifier
                    and isinstance(identifier, str)
                    and identifier[0].lower() == "u"
                ):
                    temp_identifier = identifier[1:]
                    if temp_identifier.isdigit():
                        identifiers[i] = int(temp_identifier)
        stmt = self.get_user_query(
            identifiers,
            description,
            performer,
            has_paid_content,
            load_aliases,
            load_user_info,
            load_content,
            load_media,
            load_notifications,
            load_remote_urls,
            limit,
            extra_options,
        )
        if authed_info_filter:
            if authed_info_filter.exclude_between_dates:
                stmt = stmt.where(
                    ~UserModel.user_auths_info.any(
                        UserModel.last_checked_at.between(
                            *authed_info_filter.exclude_between_dates
                        )
                    )
                )
            if authed_info_filter.active is not None:
                stmt = stmt.where(
                    UserModel.user_auths_info.any(active=authed_info_filter.active)
                )
        if order_by is not None:
            stmt = stmt.order_by(order_by)
        session = self.get_session()
        result: ScalarResult[UserModel] = await session.scalars(stmt)
        db_users = result.all()
        db_users = list(set(db_users))
        for db_user in db_users:
            if db_user and load_content:
                await self.resolve_content_manager(db_user).init(load_media=load_media)
        if isinstance(identifiers, list):
            invalid_identifiers: list[str] = []
            for identifier in identifiers:
                if isinstance(identifier, str):
                    found_db_user = None
                    for db_user in db_users:
                        found_db_user = await db_user.find_username(identifier)
                        if found_db_user:
                            break
                    if not found_db_user:
                        invalid_identifiers.append(identifier)
            for invalid_identifier in invalid_identifiers:
                stmt = select(UserAliasModel).where(
                    UserAliasModel.username == invalid_identifier
                )
                db_alias = await session.scalar(stmt)
                if db_alias:
                    db_user = await self.get_user(
                        db_alias.user_id,
                        load_user_info=load_user_info,
                        load_aliases=load_aliases,
                        load_content=load_content,
                        load_remote_urls=load_remote_urls,
                        extra_options=extra_options,
                    )
                    if db_user:
                        db_users.append(db_user)
        return db_users

    async def get_user(
        self,
        identifier: int | str | None,
        description: str | None = None,
        performer: bool | None = None,
        has_paid_content: bool | None = None,
        load_aliases: bool = False,
        load_user_info: bool = False,
        load_content: bool = False,
        load_media: bool = False,
        load_notifications: bool = False,
        load_remote_urls: bool = False,
        limit: int | None = None,
        extra_options: Any = (),
    ) -> UserModel | None:
        if identifier and isinstance(identifier, str) and identifier[0].lower() == "u":
            temp_identifier = identifier[1:]
            if temp_identifier.isdigit():
                identifier = int(temp_identifier)
        stmt = self.get_user_query(
            identifier,
            description,
            performer,
            has_paid_content,
            load_aliases,
            load_user_info,
            load_content,
            load_media,
            load_notifications,
            load_remote_urls,
            limit,
            extra_options,
        )
        session = self.get_session()
        result: ScalarResult[UserModel] = await session.scalars(stmt)
        db_user = result.first()
        if db_user and load_content:
            await self.resolve_content_manager(db_user).init(load_media=load_media)

        if not db_user and isinstance(identifier, str):
            stmt = select(UserAliasModel).where(UserAliasModel.username == identifier)
            db_alias = await session.scalar(stmt)
            if db_alias:
                db_user = await self.get_user(
                    db_alias.user_id,
                    load_user_info=load_user_info,
                    load_aliases=load_aliases,
                    load_content=load_content,
                    load_remote_urls=load_remote_urls,
                    extra_options=extra_options,
                )
        return db_user

    async def get_subscription(self, user_id: int, subscriber_id: int):
        session = self.get_session()
        stmt = select(SubscriptionModel).filter_by(
            user_id=user_id, subscriber_id=subscriber_id
        )
        found_subscription = await session.scalar(stmt)
        return found_subscription

    async def get_post(self, post_id: int):
        stmt = select(PostModel).where(PostModel.id == post_id)

        found_post = await self.get_session().scalars(stmt)
        return found_post.first()

    async def get_posts(
        self,
        user_id: int | None = None,
        post_id: int | None = None,
        text: str | None = None,
        ppv: bool | None = None,
        paid: bool | None = None,
        load_media: bool = True,
    ):
        stmt = select(PostModel)
        if user_id is not None:
            stmt = stmt.where(PostModel.user_id == user_id)
        if ppv is not None:
            stmt = stmt.where(PostModel.price > 0)
        if paid is not None:
            stmt = stmt.where(PostModel.paid == paid)
        if text is not None:
            text_list = text.split(",")
            for text_item in text_list:
                text_item = text_item.strip()
                stmt = stmt.where(PostModel.text.icontains(text_item))

        options: list[_AbstractLoad] = [joinedload(PostModel.user)]
        if not load_media:
            options.append(noload(PostModel.media))
        stmt = stmt.options(*options)
        found_posts = await self.get_session().scalars(stmt)
        return found_posts.unique().all()

    async def get_messages(
        self,
        user_id: int | None = None,
        message_id: int | None = None,
        text: str | None = None,
        ppv: bool | None = None,
        paid: bool | None = None,
        load_media: bool = True,
    ):
        stmt = select(MessageModel)
        if user_id is not None:
            stmt = stmt.where(MessageModel.user_id == user_id)
        if ppv is not None:
            stmt = stmt.where(MessageModel.price > 0)
        if paid is not None:
            stmt = stmt.where(MessageModel.paid == paid)
        if text is not None:
            text_list = text.split(",")
            for text_item in text_list:
                text_item = text_item.strip()
                stmt = stmt.where(MessageModel.text.icontains(text_item))
        options: list[_AbstractLoad] = [joinedload(MessageModel.user)]
        if not load_media:
            options.append(noload(MessageModel.media))
        stmt = stmt.options(*options)
        found_contents = await self.get_session().scalars(stmt)
        return found_contents.all()

    async def get_media(self, media_id: int | None = None, url: str | None = None):
        found_media = None
        stmt = select(MediaModel)
        if media_id:
            stmt = stmt.where(MediaModel.id == media_id)
        if url:
            stmt = stmt.where(MediaModel.url.contains(url))
        found_media = await self.get_session().scalar(stmt)
        if found_media:
            await found_media.awaitable_attrs.filepaths
        return found_media

    async def get_medias(self, user_id: int, media_ids: list[int] | None = None):
        stmt = (
            select(MediaModel)
            .where(MediaModel.user_id == user_id)
            .options(
                joinedload(MediaModel.filepaths),
                joinedload(MediaModel.content_media_assos),
            )
        )
        if media_ids is not None:
            stmt = stmt.where(MediaModel.id.in_(media_ids))
        found_media = await self.get_session().scalars(stmt)
        return found_media.unique().all()

    async def get_filepaths(
        self,
        identifier: int | str | None = None,
        media_id: int | None = None,
        post_id: int | None = None,
    ):
        stmt = select(FilePathModel)

        if identifier is not None:
            if isinstance(identifier, int):
                stmt = stmt.where(FilePathModel.id == identifier)
            else:
                stmt = stmt.where(FilePathModel.filepath.contains(identifier))

        if media_id is not None:
            stmt = stmt.where(FilePathModel.media_id == media_id)

        if post_id is not None:
            stmt = stmt.join(MediaModel).join(PostModel).where(PostModel.id == post_id)

        found_filepaths = await self.get_session().scalars(stmt)
        return found_filepaths.all()

    async def get_mass_message(self, mass_message_id: int):
        stmt = select(MassMessageModel).where(MassMessageModel.id == mass_message_id)
        found_mass_message = await self.get_session().scalars(stmt)
        return found_mass_message.first()

    async def get_site(self):
        stmt = select(SiteModel).filter_by(db_name=self.schema.name)
        found_site = await self.get_session().scalar(stmt)
        assert found_site
        return found_site

    async def get_notifications(
        self,
        platform: Literal["discord", "telegram"] | None = None,
        sent: bool | None = None,
        page: int = 1,
        limit: int = 100,
    ):
        stmt = select(NotificationModel).options(
            joinedload(NotificationModel.user).joinedload(UserModel.user_info),
            joinedload(NotificationModel.user).joinedload(UserModel.aliases),
        )
        if platform and sent is not None:
            if platform == "telegram":
                stmt = stmt.where(NotificationModel.sent_telegram == sent)
            else:
                stmt = stmt.where(NotificationModel.sent_discord == sent)
        # Calculate offset
        offset = (page - 1) * limit

        # Apply limit and offset for pagination
        stmt = stmt.limit(limit).offset(offset)

        found_notifications = await self.get_session().scalars(stmt)
        return found_notifications.unique().all()

    async def get_jobs(
        self,
        server_id: int | None = None,
        performer_id: int | None = None,
        user_id: int | None = None,
        category: str | None = None,
        priority: bool | None = None,
        active: bool | None = None,
        page: int = 1,
        limit: int | None = 100,
    ):
        session = self.get_session()
        db_site = await self.get_site()

        stmt = select(JobModel).filter_by(site_id=db_site.id)
        if server_id:
            stmt = stmt.filter_by(server_id=server_id)
        if performer_id:
            stmt = stmt.filter_by(user_id=performer_id)
        if user_id:
            stmt = stmt.filter_by(user_id=user_id)
        if category:
            stmt = stmt.filter_by(category=category)
        if priority is not None:
            stmt = stmt.filter_by(priority=priority)
        if active is not None:
            stmt = stmt.filter_by(active=active)

        stmt = (
            stmt.join(JobModel.user)
            .order_by(JobModel.priority.desc())
            .order_by(JobModel.id.asc())
            .order_by(UserModel.downloaded_at.desc())
            .options(orm.contains_eager(JobModel.user))
        )
        if limit:
            offset = max(0, (page - 1) * limit)
            stmt = stmt.offset(offset).limit(limit)

        found_jobs = await session.scalars(stmt)
        return found_jobs.all()

    async def create_or_update_job(
        self,
        db_user: UserModel,
        category: str,
        server_id: int = 1,
        host_id: int | None = None,
        priority: bool = False,
        skippable: bool = False,
    ):
        session = self.get_session()
        db_jobs = await self.get_jobs(user_id=db_user.id, category=category)
        db_site = await self.get_site()
        if db_jobs:
            db_job = db_jobs[0]
            db_job.site_id = db_site.id
            db_job.user_id = db_user.id
            db_job.user_username = db_user.username
            db_job.category = category
            db_job.server_id = server_id
            db_job.priority = priority
            db_job.host_id = host_id
            db_job.skippable = skippable
            db_job.active = True
        else:
            db_job = JobModel(
                site_id=db_site.id,
                user_id=db_user.id,
                user_username=db_user.username,
                category=category,
                server_id=server_id,
                host_id=host_id,
                skippable=skippable,
                priority=priority,
            )
            session.add(db_job)
        await session.commit()
        return db_job

    async def get_remote_urls(self, user_id: int):
        stmt = select(RemoteURLModel).where(RemoteURLModel.user_id == user_id)
        found_remote_urls = await self.get_session().scalars(stmt)
        return found_remote_urls.all()

    async def get_bulk_remote_urls(self, user_ids: list[int]):
        stmt = select(RemoteURLModel).where(RemoteURLModel.user_id.in_(user_ids))
        found_remote_urls = await self.get_session().scalars(stmt)
        return found_remote_urls.all()

    async def create_or_update_remote_url(
        self, user_id: int, host_id: int, url: str, part: int, uploaded_at: datetime
    ):
        session = self.get_session()
        stmt = (
            select(RemoteURLModel)
            .where(RemoteURLModel.user_id == user_id)
            .where(RemoteURLModel.part == part)
        )
        found_remote_url = await session.scalar(stmt)
        if not found_remote_url:
            found_remote_url = RemoteURLModel(
                user_id=user_id,
                host_id=host_id,
                url=url,
                part=part,
                uploaded_at=uploaded_at,
                exists=True,
            )
            session.add(found_remote_url)
        else:
            found_remote_url.host_id = host_id
            found_remote_url.url = url
            found_remote_url.uploaded_at = uploaded_at
            found_remote_url.exists = True
        await session.commit()
        return found_remote_url

    async def update_user(
        self, api_user: ultima_scraper_api.user_types, found_db_user: UserModel | None
    ):
        assert self.datascraper
        assert found_db_user

        content_manager = self.datascraper.resolve_content_manager(api_user)
        for media in content_manager.media_manager.medias.values():
            _db_media = await self.create_or_update_media(found_db_user, media)
            _db_filepath = await self.create_or_update_filepaths(found_db_user, media)
        _db_user = await self.create_or_update_user(
            api_user, existing_user=found_db_user, performer_optimize=True
        )
        current_job = api_user.get_current_job()
        if current_job:
            assert current_job
            current_job.done = True

    async def create_or_update_user(
        self,
        api_user: ultima_scraper_api.user_types,
        existing_user: UserModel | None,
        performer_optimize: bool = False,
        update_socials: bool = True,
        skip_subscriptions: bool = False,
        skip_paid_content: bool = False,
    ):
        session = self.get_session()
        db_user = existing_user or UserModel()
        db_user.id = api_user.id
        db_user.username = api_user.username
        db_user.balance = api_user.credit_balance or 0
        db_user.performer = api_user.is_performer()
        db_user.join_date = (
            datetime.fromisoformat(api_user.join_date) if api_user.join_date else None
        )
        if not existing_user:
            session.add(db_user)
            await self.resolve_content_manager(db_user).init()
        try:
            await session.commit()
        except Exception as e:
            pass
        await db_user.awaitable_attrs.medias
        await self.create_or_update_user_info(api_user, db_user)
        await db_user.update_username(api_user.username)
        status = False
        if not existing_user:
            await db_user.awaitable_attrs.user_auths_info
            if await is_notif_valuable(api_user):
                status = True
        if existing_user:
            await db_user.awaitable_attrs.subscribers
            if not db_user.subscribers:
                if await is_notif_valuable(api_user):
                    status = True
        if (
            api_user.is_authed_user()
            and api_user.is_performer()
            and not db_user.user_auths_info
        ):
            status = True

        if status:
            if db_user.last_checked_at == None:
                if self.aio_pika_wrapper:
                    message = create_notification(
                        "new_performer", api_user.get_api().site_name, db_user
                    )
                    await self.aio_pika_wrapper.publish_notification(message)
                else:
                    await db_user.awaitable_attrs.notifications
                    notification_exists = [
                        x
                        for x in db_user.notifications
                        if x.category == "new_performer"
                    ]
                    if not notification_exists:
                        notification = NotificationModel(
                            user_id=api_user.id, category="new_performer"
                        )
                        db_user.notifications.append(notification)
        if api_user.is_authed_user():
            api_authed = api_user.get_authed()
            db_auth_info = await self.create_or_update_auth_info(api_authed, db_user)
            if api_authed.is_authed():
                await db_auth_info.activate()
                if api_authed.user.is_performer():
                    if isinstance(api_authed, OnlyFansAuthModel):
                        mass_message_stats = await api_authed.get_mass_message_stats()
                        mass_message_stat_collection: dict[
                            int, MassMessageStatModel
                        ] = {}
                        for mass_message_stat in mass_message_stats:
                            found_mass_message = await db_user.find_mass_message(
                                mass_message_stat.id
                            )
                            if not found_mass_message:
                                media_types = mass_message_stat.media_types
                                media_count = (
                                    sum(media_types.values()) if media_types else 0
                                )
                                purchased_count = mass_message_stat.purchased_count
                                price = mass_message_stat.price
                                db_mass_message_stat = mass_message_stat_collection.get(
                                    mass_message_stat.id
                                )
                                if not db_mass_message_stat:
                                    db_mass_message_stat = MassMessageStatModel(
                                        id=mass_message_stat.id,
                                        user_id=db_user.id,
                                        media_count=media_count,
                                        buyer_count=purchased_count,
                                        sent_count=mass_message_stat.sent_count,
                                        view_count=mass_message_stat.viewed_count,
                                    )
                                db_mass_message = MassMessageModel(
                                    id=db_mass_message_stat.id,
                                    user_id=db_user.id,
                                    text=mass_message_stat.text,
                                    price=price,
                                    expires_at=mass_message_stat.expires_at,
                                    created_at=mass_message_stat.created_at,
                                    mass_message_stat=db_mass_message_stat,
                                )
                                db_user._mass_messages.append(db_mass_message)
                            pass
                if not skip_paid_content:
                    await self.create_or_update_paid_content(api_authed, db_user, [])
            if not skip_subscriptions:
                await self.process_subscriptions(
                    api_authed, db_user, performer_optimize
                )
        if isinstance(api_user, OFUserModel):
            status = True
            if performer_optimize and api_user.is_performer() or not update_socials:
                status = False
            if status:
                socials = await api_user.get_socials()
                await db_user.add_socials(socials)

                spotify = await api_user.get_spotify()
                if spotify:
                    spotify["socialMedia"] = "spotify"
                    spotify["username"] = spotify["displayName"]
                    await db_user.add_socials([spotify])
        await self.process_content(db_user, api_user)
        db_user.last_checked_at = datetime.now()
        await session.commit()
        return db_user

    async def create_or_update_auth_info(
        self, api_authed: ultima_scraper_api.auth_types, db_user: UserModel
    ):
        await db_user.awaitable_attrs.user_auths_info
        db_auth_info: UserAuthModel | None = None

        auth_info = api_authed.get_auth_details()
        for db_auth_info in db_user.user_auths_info:
            AD = db_auth_info.convert_to_auth_details(api_authed.get_api().site_name)
            if isinstance(auth_info, OnlyFansAuthDetails):
                assert isinstance(AD, OnlyFansAuthDetails)
                if auth_info.cookie.sess == AD.cookie.sess:
                    break
            else:
                FYD = db_auth_info.convert_to_auth_details(
                    api_authed.get_api().site_name
                )
                assert isinstance(FYD, FanslyAuthDetails)
                if auth_info.authorization == FYD.authorization:
                    break

        if not db_auth_info:
            exported_auth_details = api_authed.get_auth_details().export(UserAuthModel)
            db_auth_info = UserAuthModel(**exported_auth_details)
            db_auth_info.active = db_auth_info.active
            db_user.user_auths_info.append(db_auth_info)
            pass
        else:
            exported_auth_details = api_authed.get_auth_details().export()
            db_auth_info.update(exported_auth_details)
            pass
        return db_auth_info

    async def create_or_update_subscription(
        self,
        subscription: ultima_scraper_api.subscription_types,
        db_authed: "UserModel",
        performer_optimize: bool = False,
    ):
        db_sub_user = await self.get_user(
            subscription.user.id, load_content=True, load_media=True
        )
        db_sub_user = await self.create_or_update_user(
            subscription.user,
            db_sub_user,
            performer_optimize=performer_optimize,
        )
        subscription_user = subscription.user
        db_subscription = await self.get_subscription(
            subscription_user.id, db_authed.id
        )

        authed = subscription_user.get_authed()

        if not db_subscription:
            db_subscription = SubscriptionModel()
            await db_sub_user.awaitable_attrs.subscribers
            db_sub_user.subscribers.append(db_subscription)

        # Common lines of code for both if and else cases
        db_subscription.user_id = subscription_user.id
        db_subscription.subscriber_id = authed.id
        if isinstance(subscription_user, OFUserModel):
            db_subscription.paid_content = bool(
                await subscription_user.get_paid_contents()
            )
        db_subscription.created_at = datetime.fromisoformat(
            subscription.subscribed_by_data["subscribeAt"]
        )
        db_subscription.renewed_at = (
            datetime.fromisoformat(subscription.subscribed_by_data["renewedAt"])
            if subscription.subscribed_by_data["renewedAt"]
            else None
        )
        db_subscription.expires_at = subscription.resolve_expires_at()
        db_subscription.active = subscription.is_active()
        await self.get_session().commit()
        return db_subscription

    async def create_or_update_user_info(
        self,
        subscription_user: ultima_scraper_api.user_types,
        db_user: "UserModel",
    ):
        from ultima_scraper_api.apis.onlyfans.classes.user_model import UserModel

        await db_user.awaitable_attrs.user_info
        if not db_user.user_info:
            db_user.user_info = UserInfoModel()
        user_info = db_user.user_info
        user_info.name = subscription_user.name
        user_info.description = subscription_user.about
        user_info.price = await subscription_user.subscription_price() or 0
        user_info.post_count = subscription_user.posts_count
        user_info.media_count = subscription_user.medias_count
        user_info.image_count = subscription_user.photos_count
        user_info.video_count = subscription_user.videos_count
        user_info.audio_count = subscription_user.audios_count
        if isinstance(subscription_user, UserModel):
            user_info.stream_count = subscription_user.finished_streams_count
        user_info.archived_post_count = subscription_user.archived_posts_count
        if isinstance(subscription_user, UserModel):
            user_info.private_archived_post_count = (
                subscription_user.private_archived_posts_count
            )
        user_info.favourited_count = subscription_user.favorited_count
        if isinstance(subscription_user, UserModel):
            user_info.favourites_count = subscription_user.favorites_count
        user_info.subscribers_count = subscription_user.subscribers_count or 0
        user_info.location = subscription_user.location
        user_info.website = subscription_user.website

        user_info.promotion = (
            bool(await subscription_user.get_promotions())
            if isinstance(subscription_user, UserModel)
            else False
        )
        user_info.location = subscription_user.location
        user_info.website = subscription_user.website
        return user_info

    async def process_subscriptions(
        self,
        api_authed: ultima_scraper_api.auth_types,
        db_user: UserModel,
        performer_optimize: bool = False,
    ):
        if performer_optimize:
            api_subscriptions = await api_authed.get_subscriptions(filter_by="paid")
        else:
            api_subscriptions = await api_authed.get_subscriptions()
        with alive_bar(len(api_subscriptions)) as bar:
            for api_subscription in api_subscriptions:
                bar.title(
                    f"Processing Subscription: {api_subscription.user.username} ({api_subscription.user.id})"
                )
                await self.create_or_update_subscription(
                    api_subscription,
                    db_user,
                    performer_optimize=performer_optimize,
                )
                bar()

    async def process_content(
        self, db_user: UserModel, api_user: ultima_scraper_api.user_types
    ):
        if self.datascraper:
            session = self.get_session()
            content_manager = self.datascraper.resolve_content_manager(api_user)
            db_content_manager = self.resolve_content_manager(db_user)

            async def process_content_async(
                site_api: SiteAPI, db_user: UserModel, content: Any
            ):
                try:
                    await site_api.create_or_update_content(db_user, content)
                except Exception as _e:
                    breakpoint()
                    print(_e)

            async def process_media_async(
                site_api: SiteAPI, db_user: UserModel, media: "MediaMetadata"
            ):
                try:
                    await site_api.create_or_update_media(db_user, media)
                except Exception as _e:
                    breakpoint()
                    print(_e)

            async def process_filepath_async(
                site_api: SiteAPI, db_user: UserModel, media: "MediaMetadata"
            ):
                try:
                    await site_api.create_or_update_filepaths(db_user, media)
                except Exception as _e:
                    breakpoint()
                    print(_e)

            for _key, contents in content_manager.categorized.__dict__.items():
                _result = await asyncio.gather(
                    *[
                        process_content_async(self, db_user, content)
                        for content in contents.values()
                    ],
                    return_exceptions=True,
                )
                # await session.commit()
                for content in contents.values():
                    db_content = content.__db_content__
                    await db_content.awaitable_attrs.media
                    if (
                        isinstance(db_content, MassMessageModel)
                        and content.api_type == "Messages"
                    ):
                        stmt = delete(FilePathModel).where(
                            FilePathModel.mass_message_id == db_content.id
                        )

                        await session.execute(stmt)
                        pass
                        stmt = delete(ContentMediaAssoModel).where(
                            ContentMediaAssoModel.mass_message_id == db_content.id
                        )
                        await session.execute(stmt)
                        await session.delete(db_content)
                        await session.commit()

                _result2 = await asyncio.gather(
                    *[
                        process_media_async(self, db_user, media)
                        for content in contents.values()
                        for media in content.medias
                    ],
                    return_exceptions=True,
                )
                # await session.commit()
                _result2 = await asyncio.gather(
                    *[
                        process_filepath_async(self, db_user, media)
                        for content in contents.values()
                        for media in content.medias
                    ],
                    return_exceptions=True,
                )
                for _, content in contents.items():
                    await self.create_or_update_comment(content)
                session = self.get_session()
                await session.commit()
                for content in contents.values():
                    db_content = content.__db_content__
                    await db_content.awaitable_attrs.media
            size_sum = await db_content_manager.size_sum()
            if size_sum > 0:
                db_user.user_info.size = size_sum
            await session.commit()

    async def create_or_update_content(
        self, db_performer: UserModel, content: "ContentMetadata"
    ):
        api_performer = content.__soft__.get_author()
        receiver_id = content.receiver_id
        content_manager = db_performer.get_content_manager()
        found_db_content = await content_manager.find_content(
            content.content_id, content_type=content.api_type
        )
        if (
            isinstance(found_db_content, MessageModel)
            and found_db_content.receiver_id != api_performer.id
            and found_db_content.receiver_id != receiver_id
        ):
            raise Exception("Message receiver id mismatch")
        db_content = found_db_content or await content_manager.add_content(content)

        content.__db_content__ = db_content
        if isinstance(db_content, PostModel | MessageModel):
            db_content.update(content)
            if not db_content.paid:
                db_content.paid = True if content.paid else False

                assert self.datascraper
                # fix_missing_paid_content(
                #     db_content,
                #     content,
                #     datascraper=self.datascraper,
                #     api_performer=api_performer,
                # )
            db_content.price = content.price or 0
        if content.preview_media_ids:
            if "poll" not in content.preview_media_ids:
                pass
            pass
        if isinstance(db_content, MessageModel):
            db_content.verified = True
        db_content.created_at = content.created_at
        if not isinstance(db_content, StoryModel):
            db_content.media_count = content.__soft__.media_count

    async def create_or_update_media(self, db_user: UserModel, media: "MediaMetadata"):
        assert media.id
        db_content = None
        content_metadata = media.__content_metadata__
        if content_metadata:
            db_content = content_metadata.__db_content__
        media_manager = db_user.get_content_manager().media_manager
        found_media = media_manager.find_media(media.id)
        media_url = media.urls[0] if media.urls else None
        if not media_url and media.drm == False:
            return
        if not found_media:
            db_media = MediaModel(
                id=media.id,
                user_id=media.user_id,
                url=media_url,
                size=0,
                preview=media.preview,
                created_at=media.created_at,
            )
            try:
                db_user.medias.append(db_media)
            except Exception as _e:
                pass
            media_manager.add_media(db_media)
        else:
            db_media = found_media
            db_media.user_id = media.user_id
            db_media.created_at = media.created_at
        if not media.preview:
            db_media.url = media_url
        if db_content:
            found_media = await db_content.find_media(db_media.id)
            if not found_media:
                try:
                    db_content.media.append(db_media)
                except Exception as _e:
                    pass
        return db_media

    async def create_or_update_filepaths(
        self, db_user: UserModel, media: "MediaMetadata"
    ):
        assert media.id
        db_media = db_user.get_content_manager().media_manager.find_media(media.id)
        if not db_media:
            return
        db_content = None
        content_metadata = media.get_content_metadata()
        if content_metadata:
            content_info = (content_metadata.content_id, content_metadata.api_type)

            await db_media.awaitable_attrs.filepaths
            db_filepath = db_media.find_filepath(content_info)
            db_content = content_metadata.__db_content__
        else:
            db_filepath = db_media.find_filepath()
        if not db_filepath and media.filename:
            filepath = media.get_filepath()
            db_filepath = FilePathModel(
                filepath=filepath.as_posix(), preview=media.preview
            )
            if db_content:
                db_filepath.set_content(db_content)
            db_media.filepaths.append(db_filepath)
        return db_filepath

    async def create_or_update_comment(self, content: "ContentMetadata"):
        session = self.get_session()
        if isinstance(content.__soft__, post_types):
            if len(content.__soft__.comments) > 1:
                pass
            db_content = content.__db_content__
            assert db_content
            for comment in content.__soft__.comments:
                found_db_comment = await session.scalar(
                    select(CommentModel).where(CommentModel.id == comment.id)
                )
                if not found_db_comment:
                    giphy_id = None
                    reply_id = None
                    if isinstance(comment, OFCommentModel):
                        giphy_id = comment.giphy_id
                    else:
                        if comment.reply_id != comment.reply_root_id:
                            reply_id = comment.reply_id
                    db_comment = CommentModel(
                        id=comment.id,
                        post_id=db_content.id,
                        reply_id=reply_id,
                        user_id=db_content.user_id,
                        giphy_id=giphy_id,
                        text=comment.text,
                        likes_count=comment.likes_count,
                        created_at=comment.created_at,
                    )
                    session.add(db_comment)

    async def create_or_update_paid_content(
        self,
        api_authed: ultima_scraper_api.auth_types,
        db_user: UserModel,
        print_filter: list[str] = [],
    ):
        unique_user_ids: set[int] = set()
        paid_contents = await api_authed.get_paid_content()
        grouped_paid_contents = {}
        # Group paid_contents by author.id
        for paid_content in paid_contents:
            if isinstance(paid_content, dict):
                continue
            author_id = paid_content.get_author().id
            if author_id not in grouped_paid_contents:
                grouped_paid_contents[author_id] = []
            grouped_paid_contents[author_id].append(paid_content)
        with alive_bar(len(paid_contents)) as bar:
            for author_id, contents in grouped_paid_contents.items():
                supplier = await api_authed.get_user(author_id)
                local_user = await self.get_user(
                    supplier.id, load_content=True, load_media=True
                )
                await self.create_or_update_user(supplier, local_user)
                found_bought_content = await db_user.find_bought_content(supplier.id)
                if not found_bought_content:
                    bought_content = BoughtContentModel(supplier_id=supplier.id)
                    db_user.bought_contents.append(bought_content)
                unique_user_ids.add(supplier.id)
                for paid_content in contents:
                    if isinstance(paid_content, dict):
                        continue
                    bar.title(
                        f"Processing Paid Content: {db_user.username} ({db_user.id})"
                    )
                    if any(x in paid_content.text for x in print_filter):
                        urls: list[str] = []
                        for x in paid_content.media:
                            url: ParseResult | None = paid_content.url_picker(x)
                            if isinstance(url, ParseResult):
                                urls.append(url.geturl())
                        if urls:
                            print(urls, f"{paid_content.id}\n")
                    bar()
        await self.get_session().commit()
        for supplier_user_id in unique_user_ids:
            if self.aio_pika_wrapper:
                supplier_user = await self.get_user(supplier_user_id)

                message = create_notification(
                    "paid_content", api_authed.get_api().site_name, supplier_user
                )
                await self.aio_pika_wrapper.publish_notification(message)
            else:
                await self.create_or_update_notification(
                    "paid_content", supplier_user_id, api_authed
                )
        await self.get_session().commit()

    async def create_or_update_notification(
        self,
        category: str,
        supplier_user_id: int,
        api_authed: ultima_scraper_api.auth_types,
    ):

        db_supplier = await self.get_user(supplier_user_id, load_notifications=True)
        assert db_supplier
        found_notification = None

        for notification in db_supplier.notifications:
            if notification.category == category:
                if notification.authed_user_id == api_authed.id:
                    found_notification = notification
                    break
                elif notification.authed_user_id is None and not found_notification:
                    found_notification = notification

        if not found_notification:
            found_notification = NotificationModel(
                user_id=db_supplier.id,
                authed_user_id=api_authed.id,
                category=category,
            )
            db_supplier.notifications.append(found_notification)
        else:
            if not found_notification.authed_user:
                found_notification.sent_discord = False
                found_notification.sent_telegram = False
            found_notification.authed_user_id = api_authed.id
        return found_notification
