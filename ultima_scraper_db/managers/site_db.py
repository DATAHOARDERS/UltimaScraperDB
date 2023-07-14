from datetime import datetime
from typing import TYPE_CHECKING, Any, Type

import ultima_scraper_api
from sqlalchemy import ScalarResult, Select, func, or_, select
from sqlalchemy.ext.asyncio.session import async_object_session
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import and_, exists, select, union_all
from ultima_scraper_api.apis.onlyfans.classes.comment_model import (
    CommentModel as OFCommentModel,
)
from ultima_scraper_api.apis.onlyfans.classes.user_model import (
    create_user as OFUserModel,
)
from ultima_scraper_collection.helpers.main_helper import is_valuable
from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
    ContentMetadata,
)
from ultima_scraper_db.databases.ultima.schemas.templates.site import (
    BoughtContentModel,
    CommentModel,
    FilePathModel,
    MediaModel,
    MessageModel,
    NotificationModel,
    PostModel,
    StoryModel,
    SubscriptionModel,
    UserAuthModel,
    UserInfoModel,
    UserModel,
)
from ultima_scraper_db.managers.database_manager import Schema

content_model_types = StoryModel | PostModel | MessageModel
from ultima_scraper_api import post_types

if TYPE_CHECKING:
    from ultima_scraper_collection import datascraper_types


class ContentManager:
    def __init__(
        self,
        user: "UserModel",
    ):
        self.__user__ = user

    async def init(self):
        await self.__user__.awaitable_attrs._stories
        await self.__user__.awaitable_attrs._posts
        await self.__user__.awaitable_attrs._messages
        self.stories: list["StoryModel"] = self.__user__._stories  # type: ignore
        self.posts: list["PostModel"] = self.__user__._posts  # type: ignore
        self.messages: list["MessageModel"] = self.__user__._messages  # type: ignore
        return self

    async def get_contents(self, content_type: str | None = None):
        if content_type:
            # if not hasattr(self, content_type.lower()):
            #     empty_list: content_model_types = []
            #     return empty_list
            result: list[content_model_types] = getattr(self, content_type.lower())
            return result
        return self.stories + self.posts + self.messages

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
                session = async_object_session(self.__user__)
                assert session
                await session.flush()
                assert content.receiver_id
                content_model.receiver_id = content.receiver_id
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

        session = async_object_session(self.__user__)
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

    async def sum(self):
        session = async_object_session(self.__user__)
        assert session

        stmt1 = (
            select(func.sum(MediaModel.size))
            .join(StoryModel.media)
            .where(StoryModel.user_id == self.__user__.id)
        )
        stmt2 = (
            select(func.sum(MediaModel.size))
            .join(PostModel.media)
            .where(PostModel.user_id == self.__user__.id)
        )
        stmt3 = (
            select(func.sum(MediaModel.size))
            .join(MessageModel.media)
            .where(MessageModel.user_id == self.__user__.id)
        )
        result1 = await session.execute(stmt1)
        result2 = await session.execute(stmt2)
        result3 = await session.execute(stmt3)
        final_sum = sum(
            (result1.scalar() or 0, result2.scalar() or 0, result3.scalar() or 0)
        )
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

    def filter_by_user_identifiers(self, identifiers: list[int | str]):
        final_identifiers = [str(x) for x in identifiers]
        template_query = [
            UserModel.username.in_([x for x in final_identifiers if not x.isdigit()]),
            UserModel.id.in_([int(x) for x in final_identifiers if x.isdigit()]),
        ]
        if self.model == UserModel:
            self.statement = self.statement.filter(or_(*template_query))
        else:
            self.statement = self.statement.join(UserModel).filter(or_(*template_query))

        return self.statement

    def filter_by_description(self, description: str):
        description = description.lower()
        if self.model == UserModel:
            self.statement = self.statement.join(UserInfoModel).filter(
                UserInfoModel.description.ilike(f"%{description}%")
            )
        else:
            self.statement = self.statement.join(UserModel).filter(
                UserInfoModel.description.ilike(f"%{description}%")
            )
        return self.statement


class SiteDB:
    def __init__(
        self, schema: Schema, datascraper: "datascraper_types | None" = None
    ) -> None:
        self.database = schema.database
        self.schema = self.database.get_schema(schema.name)
        self.session = self.schema.session
        self.datascraper = datascraper

    async def get_users(self, identifiers: list[int | str]):
        stmt_builder = StatementBuilder(UserModel).filter_by_user_identifiers(
            identifiers
        )
        db_users: ScalarResult[UserModel] = await self.session.scalars(stmt_builder)
        return db_users.all()

    async def get_user(self, identifier: int | str):
        stmt_builder = StatementBuilder(UserModel).filter_by_user_identifiers(
            [identifier]
        )
        db_users: ScalarResult[UserModel] = await self.session.scalars(stmt_builder)
        return db_users.first()

    async def get_media(self, media_id: int):
        stmnt = select(MediaModel).where(MediaModel.id == media_id)
        found_media = await self.session.scalars(stmnt)
        return found_media.first()

    async def update_user(
        self, api_user: ultima_scraper_api.user_types, found_db_user: UserModel | None
    ):
        _db_user = await self.create_or_update_user(
            api_user, existing_user=found_db_user
        )
        await self.session.commit()

        current_job = api_user.get_current_job()
        if current_job:
            assert current_job
            current_job.done = True

    async def create_or_update_user(
        self, api_user: ultima_scraper_api.user_types, existing_user: UserModel | None
    ):
        session = self.schema.session
        db_user = existing_user or UserModel()
        if not existing_user:
            db_user.id = api_user.id
            session.add(db_user)
        found_user_info = await db_user.find_user_info(api_user.username)
        if api_user.username != db_user.username:
            checkpoint_ui = await db_user.find_user_info(db_user.username)
            if not found_user_info:
                user_info = await self.create_or_update_user_info(
                    api_user, checkpoint_ui=checkpoint_ui
                )
                await db_user.awaitable_attrs.user_infos
                db_user.user_infos.append(user_info)
                pass
            else:
                user_info = await self.create_or_update_user_info(
                    api_user, existing_user_info=found_user_info
                )
                pass
        else:
            user_info = await self.create_or_update_user_info(
                api_user, existing_user_info=found_user_info
            )
            pass
        if not existing_user:
            status = False
            if await is_valuable(api_user):
                if await api_user.subscription_price() == 0:
                    if (
                        isinstance(api_user, OFUserModel)
                        and await api_user.get_paid_contents()
                    ):
                        status = True
                else:
                    status = True
            if status:
                notification = NotificationModel(
                    user_id=api_user.id, category="new_performer"
                )
                session.add(notification)
            await session.flush()
        if existing_user:
            _alias = await db_user.add_alias(api_user.username)
        if api_user.is_authed_user():
            api_authed = api_user.get_authed()
            await self.create_or_update_auth_info(api_authed, db_user)
            if api_authed.is_authed():
                await db_user.activate()

            paid_contents = await api_authed.get_paid_content()
            for paid_content in paid_contents:
                if isinstance(paid_content, dict):
                    continue
                supplier = paid_content.get_author()
                temp_local_user = await session.scalars(
                    select(UserModel).where(UserModel.id == supplier.id)
                )
                local_user = temp_local_user.first()
                await self.create_or_update_user(supplier, local_user)
                found_bought_content = await db_user.find_bought_content(supplier.id)
                if not found_bought_content:
                    bought_content = BoughtContentModel(supplier_id=supplier.id)
                    db_user.bought_contents.append(bought_content)
                    pass
                else:
                    pass
            pass
            api_subscriptions = await api_authed.get_subscriptions()
            for api_subscription in api_subscriptions:
                await self.create_or_update_subscription(api_subscription, db_user)
                pass
            pass
        pass
        if isinstance(api_user, OFUserModel):
            socials = await api_user.get_socials()
            await db_user.add_socials(socials)

            spotify = await api_user.get_spotify()
            if spotify:
                spotify["socialMedia"] = "spotify"
                spotify["username"] = spotify["displayName"]
                await db_user.add_socials([spotify])
        db_user.username = api_user.username
        db_user.balance = api_user.creditBalance
        db_user.performer = api_user.isPerformer
        db_user.join_date = (
            datetime.fromisoformat(api_user.joinDate) if api_user.joinDate else None
        )
        _s = await db_user.awaitable_attrs._stories
        _p = await db_user.awaitable_attrs._posts
        _m = await db_user.awaitable_attrs._messages
        pass
        for _key, contents in api_user.content_manager.categorized.__dict__.items():
            for _, content in contents.items():
                await self.create_or_update_content(db_user, content)
        db_user.last_checked_at = datetime.now()
        user_info.size = await db_user.content_manager.sum()
        await self.session.flush()
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
    ):
        temp_db_sub_user = await self.session.scalars(
            select(UserModel).where(UserModel.id == subscription.user.id)
        )
        db_sub_user = temp_db_sub_user.first()
        if db_sub_user:
            pass
        db_sub_user = await self.create_or_update_user(subscription.user, db_sub_user)
        subscription_user = subscription.user
        db_subscription = await db_authed.find_subscription(subscription_user.id)
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
        pass
        expires_at = subscription.resolve_expires_at()
        db_subscription.expires_at = expires_at
        db_subscription.active = subscription.is_active()
        await self.session.flush()
        await db_sub_user.awaitable_attrs.subscribers
        return db_subscription

    async def create_or_update_user_info(
        self,
        subscription_user: ultima_scraper_api.user_types,
        existing_user_info: "UserInfoModel | None" = None,
        checkpoint_ui: "UserInfoModel | None" = None,
    ):
        user_info = existing_user_info or UserInfoModel()
        user_info.username = subscription_user.username
        user_info.name = subscription_user.name
        user_info.description = subscription_user.about
        user_info.post_count = subscription_user.postsCount
        user_info.image_count = subscription_user.photosCount
        user_info.video_count = subscription_user.videosCount
        user_info.audio_count = subscription_user.audiosCount
        user_info.favourited_count = subscription_user.favoritedCount
        pass
        user_info.price = await subscription_user.subscription_price() or 0
        from ultima_scraper_api.apis.onlyfans.classes.user_model import create_user

        user_info.promotion = (
            bool(await subscription_user.get_promotions())
            if isinstance(subscription_user, create_user)
            else False
        )
        user_info.location = subscription_user.location
        user_info.website = subscription_user.website
        if not existing_user_info and checkpoint_ui:
            user_info.sex = checkpoint_ui.sex
            user_info.size = checkpoint_ui.size
            user_info.downloaded_at = checkpoint_ui.downloaded_at
            user_info.uploaded_at = checkpoint_ui.uploaded_at
        await self.session.flush()
        return user_info

    async def create_or_update_content(
        self, db_performer: UserModel, content: ContentMetadata
    ):
        api_performer = content.__soft__.get_author()
        api_type = content.api_type
        content_manager = await db_performer.content_manager.init()
        found_db_content = await content_manager.find_content(content.content_id)
        if not found_db_content:
            pass
        if content.paid:
            pass
        db_content = found_db_content or await content_manager.add_content(content)
        db_content_paid = None
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
                    if valid_local_media_count == content.__soft__.mediaCount:
                        db_content.paid = True
            db_content_paid = db_content.paid
            db_content.price = content.price or 0
        if content.preview_media_ids:
            if "poll" not in content.preview_media_ids:
                pass
            pass
        if isinstance(db_content, MessageModel):
            db_content.verified = True
        db_content.created_at = content.created_at

        for media in content.medias:
            assert media.id
            final_url = media.urls[0] if media.urls else None
            found_db_media = await db_content.find_media(media.id)
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
                found_global_media = await self.get_media(media.id)
                if found_global_media:
                    db_media = found_global_media
                else:
                    db_media = MediaModel(
                        id=media.id,
                        url=final_url,
                        size=0,
                        preview=media.preview,
                        created_at=media.created_at,
                    )
                await db_content.awaitable_attrs.media
                db_content.media.append(db_media)
            if not final_url:
                pass
            if db_media.created_at and db_media.created_at.tzinfo is None:
                db_media.created_at = db_media.created_at
            filepath = await db_media.find_filepath(db_content.id, api_type)
            if not filepath and final_url:
                assert media.directory
                assert media.filename
                filepath = FilePathModel(
                    # media_id=db_media.id,
                    filepath=media.directory.joinpath(media.filename).as_posix(),
                    preview=media.preview,
                )
                await filepath.set_content(db_content)
                await db_media.awaitable_attrs.filepaths
                db_media.filepaths.append(filepath)
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
        await self.session.flush()
        if isinstance(content.__soft__, post_types):
            if len(content.__soft__.comments) > 1:
                pass
            for comment in content.__soft__.comments:
                found_db_comment = await self.session.scalar(
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
                    self.session.add(db_comment)
                else:
                    pass
        pass
