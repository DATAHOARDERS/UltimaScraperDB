import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Type
from urllib.parse import ParseResult

import ultima_scraper_api
from sqlalchemy import (
    BigInteger,
    Boolean,
    ScalarResult,
    Select,
    SmallInteger,
    Text,
    UnaryExpression,
    or_,
    orm,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.session import async_object_session
from sqlalchemy.orm import joinedload, selectinload, subqueryload
from sqlalchemy.sql import and_, exists, func, select, union_all
from ultima_scraper_api.apis.onlyfans.classes.auth_model import AuthModel
from ultima_scraper_api.apis.onlyfans.classes.comment_model import (
    CommentModel as OFCommentModel,
)
from ultima_scraper_api.apis.onlyfans.classes.user_model import (
    create_user as OFUserModel,
)
from ultima_scraper_api.helpers.main_helper import date_between_cur_month, split_string
from ultima_scraper_collection.helpers.main_helper import is_notif_valuable, is_valuable
from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
    ContentMetadata,
)

from ultima_scraper_db.databases.ultima_archive.filters import AuthedInfoFilter
from ultima_scraper_db.databases.ultima_archive.schemas.management import SiteModel
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    BoughtContentModel,
    CommentModel,
    ContentTemplate,
    FilePathModel,
    JobModel,
    MassMessageModel,
    MassMessageStatModel,
    MediaModel,
    MessageModel,
    NotificationModel,
    PostModel,
    StoryModel,
    SubscriptionModel,
    UserAliasModel,
    UserAuthModel,
    UserInfoModel,
    UserModel,
    content_models,
)
from ultima_scraper_db.managers.database_manager import Schema

content_model_types = StoryModel | PostModel | MessageModel | MassMessageModel
from ultima_scraper_api import post_types

if TYPE_CHECKING:
    from ultima_scraper_collection import datascraper_types

from sqlalchemy import inspect
from sqlalchemy.orm.strategy_options import _AbstractLoad


def create_options(alias: bool = False, user_info: bool = False, content: bool = False):
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
    if user_info:
        stmt = selectinload(UserModel.user_info)
        joined_options.append(stmt)
    return joined_options


class FilePathManager:
    def __init__(self, content_manager: "ContentManager") -> None:
        self.content_manager = content_manager
        self.filepaths: dict[int, FilePathModel] = {
            filepath.id: filepath
            for media in self.content_manager.media_manager.medias.values()
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

    async def init(self):
        for item in self.medias.values():
            await item.awaitable_attrs.filepaths
        self.filepath_manager = FilePathManager(self.content_manager)

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
        if not self.session:
            pass
        self.lock = asyncio.Lock()
        self.media_manager = MediaManager(self)

    async def init(self):
        await self.__user__.awaitable_attrs._stories
        await self.__user__.awaitable_attrs._posts
        await self.__user__.awaitable_attrs._messages
        await self.__user__.awaitable_attrs._mass_messages
        if not self.session:
            self.session = async_object_session(self.__user__)
        media_manager = self.media_manager
        self.stories: list["StoryModel"] = self.__user__._stories  # type: ignore
        [
            media_manager.add_media(item)
            for content in self.stories
            for item in await content.awaitable_attrs.media
        ]

        self.posts: list["PostModel"] = self.__user__._posts  # type: ignore
        if self.posts:
            pass
        [
            media_manager.add_media(item)
            for content in self.posts
            for item in await content.awaitable_attrs.media
        ]
        self.messages: list["MessageModel"] = self.__user__._messages  # type: ignore
        [
            media_manager.add_media(item)
            for content in self.messages
            for item in await content.awaitable_attrs.media
        ]
        self.mass_messages: list["MessageModel"] = self.__user__._mass_messages  # type: ignore
        [
            media_manager.add_media(item)
            for content in self.mass_messages
            for item in await content.awaitable_attrs.media
        ]
        await media_manager.init()
        return self

    def get_media_manager(self):
        return self.media_manager

    def get_filepath_manager(self):
        return self.media_manager.filepath_manager

    async def get_contents(self, content_type: str | None = None):
        if content_type:
            # if not hasattr(self, content_type.lower()):
            #     empty_list: content_model_types = []
            #     return empty_list
            result: list[content_model_types] = getattr(self, content_type.lower())
            return result
        return self.stories + self.posts + self.messages + self.mass_messages

    async def add_content(self, content: ContentMetadata):
        match content.api_type:
            case "Stories":
                content_model = StoryModel(
                    id=content.content_id,
                    user_id=content.user_id,
                    created_at=content.__soft__.created_at,
                )
                self.stories.append(content_model)
            case "Posts":
                content_model = PostModel(
                    id=content.content_id,
                    user_id=content.user_id,
                    text=content.text,
                    price=content.price,
                    paid=int(content.paid),
                    created_at=content.__soft__.created_at,
                )
                self.posts.append(content_model)
            case "Messages":
                content_model = MessageModel(
                    id=content.content_id,
                    user_id=content.user_id,
                    receiver_id=content.receiver_id,
                    text=content.text,
                    price=content.price,
                    paid=int(content.paid),
                    verified=True,
                    created_at=content.__soft__.created_at,
                )
                self.messages.append(content_model)
                assert content.receiver_id
                content_model.receiver_id = content.receiver_id
                pass
            case "MassMessages":
                stat = content.get_mass_message_stat()
                assert stat
                content_model = MassMessageModel(
                    id=content.content_id,
                    user_id=content.user_id,
                    statistic_id=stat.id,
                    text=content.text,
                    price=content.price,
                    expires_at=content.__soft__.expires_at,
                    created_at=content.__soft__.created_at,
                )
                pass
            case _:
                raise Exception("Content not assigned")
        return content_model

    async def find_content(
        self,
        content_id: int,
    ):
        temp_contents = await self.get_contents()
        for content in temp_contents:
            if content.id == content_id:
                return content

    async def find_paid_contents(self):
        from sqlalchemy.ext.asyncio import async_object_session

        session = self.session
        assert session
        stmt1 = (
            select(MediaModel)
            .join(PostModel.media)
            .options(joinedload(MediaModel.filepaths))
            .where(
                and_(
                    PostModel.user_id == self.__user__.id,
                    PostModel.price > 0,
                    PostModel.paid == True,
                    exists(MediaModel).where(MediaModel.filepaths.any()),
                    MediaModel.preview == False,
                )
            )
        )
        stmt2 = (
            select(MediaModel)
            .join(MessageModel.media)
            .options(joinedload(MediaModel.filepaths))
            .where(
                and_(
                    MessageModel.user_id == self.__user__.id,
                    MessageModel.price > 0,
                    MessageModel.paid == True,
                    exists(MediaModel).where(MediaModel.filepaths.any()),
                    MediaModel.preview == False,
                )
            )
        )
        union_stmt = union_all(stmt1, stmt2)
        orm_stmt = select(MediaModel).from_statement(union_stmt)
        result_3 = await session.scalars(orm_stmt)
        result_3 = result_3.unique().all()
        pass
        return result_3

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

    async def __aenter__(self):
        self._session: AsyncSession = self.schema.sessionmaker()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._session.commit()
        await self._session.aclose()

    def get_session(self):
        assert self._session, "Session has not been set"
        return self._session

    def set_session(self, session: AsyncSession):
        self._session = session

    def get_session_maker(self):
        return self.schema.sessionmaker()

    def get_user_query(
        self,
        identifiers: list[int | str] | int | str | None = None,
        description: str | None = None,
        performer: bool | None = None,
        has_paid_content: bool | None = None,
        load_aliases: bool = False,
        load_user_info: bool = False,
        load_content: bool = False,
        limit: int | None = None,
        extra_options: Any = (),
    ):
        options = create_options(
            content=load_content, user_info=load_user_info, alias=load_aliases
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
        authed_info_filter: AuthedInfoFilter | None = None,
        limit: int | None = None,
        order_by: UnaryExpression[Any] | None = None,
        extra_options: Any = (),
    ):
        stmt = self.get_user_query(
            identifiers,
            description,
            performer,
            has_paid_content,
            load_aliases,
            load_user_info,
            load_content,
            limit,
            extra_options,
        )
        if authed_info_filter:
            if authed_info_filter.exclude_between_dates:
                stmt = stmt.where(
                    ~UserModel.user_auth_info.has(
                        UserModel.last_checked_at.between(
                            *authed_info_filter.exclude_between_dates
                        )
                    )
                )
            if authed_info_filter.active is not None:
                stmt = stmt.where(
                    UserModel.user_auth_info.has(active=authed_info_filter.active)
                )
        if order_by is not None:
            stmt = stmt.order_by(order_by)
        session = self.get_session()
        result: ScalarResult[UserModel] = await session.scalars(stmt)
        db_users = result.all()
        for db_user in db_users:
            if db_user and load_content:
                await db_user.content_manager.init()
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
        limit: int | None = None,
        extra_options: Any = (),
    ) -> UserModel | None:
        stmt = self.get_user_query(
            identifier,
            description,
            performer,
            has_paid_content,
            load_aliases,
            load_user_info,
            load_content,
            limit,
            extra_options,
        )
        session = self.get_session()

        result: ScalarResult[UserModel] = await session.scalars(stmt)
        db_user = result.first()
        if db_user and load_content:
            await db_user.content_manager.init()
        if not db_user and isinstance(identifier, str):
            stmt = select(UserAliasModel).where(UserAliasModel.username == identifier)
            db_alias = await session.scalar(stmt)
            if db_alias:
                db_user = await self.get_user(
                    db_alias.user_id,
                    load_aliases=load_aliases,
                    load_content=load_content,
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

    async def get_media(self, media_id: int):
        stmt = select(MediaModel).where(MediaModel.id == media_id)
        found_media = await self.get_session().scalars(stmt)
        return found_media.first()

    async def get_filepaths(self, identifier: int | str):
        stmt = select(FilePathModel)
        if isinstance(identifier, int):
            stmt = stmt.where(FilePathModel.id == identifier)
        else:
            stmt = stmt.where(FilePathModel.filepath.contains(identifier))

        found_filepath = await self.get_session().scalars(stmt)
        return found_filepath

    async def get_mass_message(self, mass_message_id: int):
        stmt = select(MassMessageModel).where(MassMessageModel.id == mass_message_id)
        found_mass_message = await self.get_session().scalars(stmt)
        return found_mass_message.first()

    async def get_site(self):
        stmt = select(SiteModel).filter_by(db_name=self.schema.name)
        found_site = await self.get_session().scalar(stmt)
        assert found_site
        return found_site

    async def get_jobs(
        self,
        server_id: int | None = None,
        user_id: int | None = None,
        category: str | None = None,
        active: bool | None = None,
        page: int = 1,
        limit: int | None = 100,
    ):
        session = self.get_session()
        db_site = await self.get_site()

        stmt = select(JobModel).filter_by(site_id=db_site.id)
        if server_id:
            stmt = stmt.filter_by(server_id=server_id)
        if user_id:
            stmt = stmt.filter_by(user_id=user_id)
        if category:
            stmt = stmt.filter_by(category=category)
        if active is not None:
            stmt = stmt.filter_by(active=active)

        stmt = (
            stmt.join(JobModel.user)
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
        self, db_user: UserModel, category: str, server_id: int = 1
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
            db_job.active = True
        else:
            db_job = JobModel(
                site_id=db_site.id,
                user_id=db_user.id,
                user_username=db_user.username,
                category=category,
                server_id=server_id,
            )
            session.add(db_job)
        await session.commit()
        return db_job

    async def update_user(
        self, api_user: ultima_scraper_api.user_types, found_db_user: UserModel | None
    ):
        _db_user = await self.create_or_update_user(
            api_user, existing_user=found_db_user
        )

        current_job = api_user.get_current_job()
        if current_job:
            assert current_job
            current_job.done = True

    async def create_or_update_user(
        self,
        api_user: ultima_scraper_api.user_types,
        existing_user: UserModel | None,
        optimize: bool = False,
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
            await db_user.content_manager.init()
        await session.commit()

        db_user_info = await self.create_or_update_user_info(api_user, db_user)
        _alias = await db_user.add_alias(api_user.username)
        if not existing_user:
            if await is_notif_valuable(api_user):
                notification = NotificationModel(
                    user_id=api_user.id, category="new_performer"
                )
                session.add(notification)
        if api_user.is_authed_user():
            api_authed = api_user.get_authed()
            await self.create_or_update_auth_info(api_authed, db_user)
            if api_authed.is_authed():
                await db_user.activate()
                if api_authed.user.is_performer():
                    if isinstance(api_authed, AuthModel):
                        mass_message_stats = await api_authed.get_mass_message_stats()
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
                                db_mass_message_stat = MassMessageStatModel(
                                    id=mass_message_stat.id,
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
                                session.add(db_mass_message)
                            pass
                await self.create_or_update_paid_content(api_authed, db_user, [])
            api_subscriptions = await api_authed.get_subscriptions()
            for api_subscription in api_subscriptions:
                await self.create_or_update_subscription(
                    api_subscription, db_user, optimize=True
                )
                pass
        if isinstance(api_user, OFUserModel):
            status = True
            is_performer = api_user.is_performer()
            if optimize and is_performer == False:
                status = False
            if status:
                socials = await api_user.get_socials()
                await db_user.add_socials(socials)

                spotify = await api_user.get_spotify()
                if spotify:
                    spotify["socialMedia"] = "spotify"
                    spotify["username"] = spotify["displayName"]
                    await db_user.add_socials([spotify])
        for _key, contents in api_user.content_manager.categorized.__dict__.items():

            async def test(site_api: SiteAPI, db_user: UserModel, content: Any):
                try:
                    await site_api.create_or_update_content(db_user, content)
                except Exception as _e:
                    breakpoint()
                    print(_e)

            async def test2(site_api: SiteAPI, content: Any):
                try:
                    await site_api.create_or_update_media(content)
                except Exception as _e:
                    print(_e)
                    breakpoint()

            if contents:
                pass
            await db_user.content_manager.init()
            _result = await asyncio.gather(
                *[test(self, db_user, content) for content in contents.values()],
                return_exceptions=True,
            )
            await session.commit()

            await db_user.content_manager.init()
            _result2 = await asyncio.gather(
                *[test2(self, content) for content in contents.values()],
                return_exceptions=True,
            )
            for _, content in contents.items():
                await self.create_or_update_comment(content)
            await session.commit()
        db_user_info.size = await db_user.content_manager.size_sum()
        db_user.last_checked_at = datetime.now()
        await session.commit()
        return db_user

    async def create_or_update_auth_info(
        self, api_authed: ultima_scraper_api.auth_types, db_user: UserModel
    ):
        await db_user.awaitable_attrs.user_auth_info
        db_user_auth_info = db_user.user_auth_info
        if not db_user_auth_info:
            exported_auth_details = api_authed.get_auth_details().export(UserAuthModel)
            user_auth_model = UserAuthModel(**exported_auth_details)
            user_auth_model.active = user_auth_model.active
            db_user.user_auth_info = user_auth_model
            pass
        else:
            exported_auth_details = api_authed.get_auth_details().export()
            db_user.user_auth_info.update(exported_auth_details)
            pass
        db_user.user_auth_info.email = api_authed.user.email
        return db_user.user_auth_info

    async def create_or_update_subscription(
        self,
        subscription: ultima_scraper_api.subscription_types,
        db_authed: "UserModel",
        optimize: bool = False,
    ):
        db_sub_user = await self.get_user(subscription.user.id, load_content=True)
        db_sub_user = await self.create_or_update_user(
            subscription.user, db_sub_user, optimize=optimize
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
        db_subscription.expires_at = subscription.resolve_expires_at()
        db_subscription.active = subscription.is_active()
        return db_subscription

    async def create_or_update_user_info(
        self,
        subscription_user: ultima_scraper_api.user_types,
        db_user: "UserModel",
    ):
        from ultima_scraper_api.apis.onlyfans.classes.user_model import create_user

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
        if isinstance(subscription_user, create_user):
            user_info.stream_count = subscription_user.finished_streams_count
        user_info.archived_post_count = subscription_user.archived_posts_count
        if isinstance(subscription_user, create_user):
            user_info.private_archived_post_count = (
                subscription_user.private_archived_posts_count
            )
        user_info.favourited_count = subscription_user.favorited_count
        if isinstance(subscription_user, create_user):
            user_info.favourites_count = subscription_user.favorites_count
        user_info.subscribers_count = subscription_user.subscribers_count or 0
        user_info.location = subscription_user.location
        user_info.website = subscription_user.website
        from ultima_scraper_api.apis.onlyfans.classes.user_model import create_user

        user_info.promotion = (
            bool(await subscription_user.get_promotions())
            if isinstance(subscription_user, create_user)
            else False
        )
        user_info.location = subscription_user.location
        user_info.website = subscription_user.website
        return user_info

    async def create_or_update_content(
        self, db_performer: UserModel, content: ContentMetadata
    ):
        api_performer = content.__soft__.get_author()
        content_manager = db_performer.content_manager
        found_db_content = await content_manager.find_content(content.content_id)
        db_content = found_db_content or await content_manager.add_content(content)
        content.__db_content__ = db_content
        content.paid = False
        if isinstance(db_content, PostModel | MessageModel):
            if not db_content.paid:
                db_content.paid = True if content.paid else False
                if (
                    bool(db_content.paid) == False
                    and content.price
                    and content.price > 0
                ):
                    assert self.datascraper
                    fmu = self.datascraper.filesystem_manager.get_file_manager(
                        api_performer.id
                    )
                    valid_local_media_count = 0
                    for file in fmu.files:
                        remote_media_date = content.__soft__.created_at
                        remote_post_date = content.__soft__.created_at
                        stat = file.stat()
                        mt = stat.st_mtime
                        local_media_date = datetime.fromtimestamp(mt).replace(
                            microsecond=0
                        )
                        local_post_date = datetime.fromtimestamp(mt).replace(
                            microsecond=0
                        )
                        if (
                            local_media_date == remote_media_date
                            or local_post_date == remote_post_date
                        ):
                            valid_local_media_count += 1
                    if valid_local_media_count == content.__soft__.media_count:
                        db_content.paid = True
            db_content.price = content.price or 0
        if content.preview_media_ids:
            if "poll" not in content.preview_media_ids:
                pass
            pass
        if isinstance(db_content, MessageModel):
            db_content.verified = True
        db_content.created_at = content.created_at

    async def create_or_update_media(self, content: ContentMetadata):
        api_type = content.api_type
        db_content = content.__db_content__
        assert db_content
        db_content_paid = None
        try:
            media_manager = db_content.user.content_manager.media_manager
        except Exception as e:
            return
        if isinstance(db_content, PostModel | MessageModel):
            db_content_paid = db_content.paid
        for media in content.medias:
            assert media.id
            final_url = media.urls[0] if media.urls else None
            found_db_media = media_manager.find_media(media.id)
            if found_db_media:
                db_media = found_db_media
                if not final_url:
                    final_url = db_media.url
                    if final_url:
                        # UNFINISHED
                        # Need to find a way to rebuild media's directory, filename, etc (We can use the reformatter)
                        # content_metadata = ContentMetadata(db_content.id, api_type)
                        # extractor = DBContentExtractor(db_content)
                        # extractor.__api__ = api
                        # await content_metadata.resolve_extractor(extractor)
                        # Remove none when solution found
                        final_url = None
                        pass
            else:
                found_global_media = media_manager.find_media(media.id)
                if found_global_media:
                    db_media = found_global_media
                else:
                    db_media = MediaModel(
                        id=media.id,
                        user_id=content.user_id,
                        url=final_url,
                        size=0,
                        preview=media.preview,
                        created_at=media.created_at,
                    )
                    media_manager.add_media(db_media)
                try:
                    db_content.media.append(db_media)
                except Exception as _e:
                    breakpoint()
            if not final_url:
                pass
            if db_media.created_at and db_media.created_at.tzinfo is None:
                db_media.created_at = db_media.created_at
            try:
                filepath = db_media.find_filepath(db_content.id, api_type)
            except Exception as _e:
                breakpoint()
            if not filepath and final_url:
                assert media.directory
                assert media.filename
                filepath = FilePathModel(
                    # media_id=db_media.id,
                    filepath=media.directory.joinpath(media.filename).as_posix(),
                    preview=media.preview,
                )
                await filepath.set_content(db_content)
                try:
                    db_media.filepaths.append(filepath)
                except Exception as _e:
                    breakpoint()
            if final_url:
                if db_content_paid or (not content.paid and not content.price):
                    # No previews get through here
                    db_media.preview = False
                    db_media.url = final_url
                    assert filepath and media.directory and media.filename
                    filepath.media_id = db_media.id
                    filepath.filepath = media.directory.joinpath(
                        media.filename
                    ).as_posix()
                    filepath.preview = False
                else:
                    if isinstance(db_content, PostModel | MessageModel):
                        # Handle previews
                        if not db_content.paid and not db_media.url:
                            db_media.preview = True
                            db_media.url = final_url
                            assert filepath and media.directory and media.filename
                            filepath.media_id = db_media.id
                            filepath.filepath = media.directory.joinpath(
                                media.filename
                            ).as_posix()
                            filepath.preview = True
            elif db_content_paid:
                db_media.preview = False
                if filepath:
                    filepath.preview = False
            db_media.category = media.media_type
            if media.size >= int(db_media.size):
                db_media.size = media.size
            media_created_at = media.created_at
            if db_media.created_at is None or media_created_at < db_media.created_at:
                if db_media.created_at is None:
                    pass
                db_media.created_at = media_created_at

    async def create_or_update_comment(self, content: ContentMetadata):
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
        self, api_authed: AuthModel, db_user: UserModel, print_filter: list[str] = []
    ):
        print("Getting paid content")
        paid_contents = await api_authed.get_paid_content()
        for paid_content in paid_contents:
            if isinstance(paid_content, dict):
                continue
            if any(x in paid_content.text for x in print_filter):
                urls: list[str] = []
                for x in paid_content.media:
                    url: ParseResult | None = paid_content.url_picker(x)
                    if isinstance(url, ParseResult):
                        urls.append(url.geturl())
                if urls:
                    print(urls, f"{paid_content.id}\n")
            supplier = paid_content.get_author()
            local_user = await self.get_user(supplier.id, load_content=True)
            await self.create_or_update_user(supplier, local_user)
            found_bought_content = await db_user.find_bought_content(supplier.id)
            if not found_bought_content:
                bought_content = BoughtContentModel(supplier_id=supplier.id)
                db_user.bought_contents.append(bought_content)
