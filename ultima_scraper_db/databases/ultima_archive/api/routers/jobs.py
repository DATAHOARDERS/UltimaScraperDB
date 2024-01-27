from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import or_, orm, select, update
from sqlalchemy.orm import contains_eager, lazyload, sessionmaker
from ultima_scraper_db.databases.ultima_archive.api.client import UAClient
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    FilePathModel,
    JobModel,
)

restricted = (
    # lazyload(UserModel.user_auth_info),
    # orm.defer(UserModel.performer),
    # orm.defer(UserModel.favorite),
    # orm.defer(UserModel.balance),
    # orm.defer(UserModel.spend),
    # orm.defer(UserModel.updated_at),
    # orm.defer(UserModel.created_at),
)

router = APIRouter(
    prefix="/jobs",
    tags=["jobs"],
    responses={404: {"description": "Not found"}},
)


class JobData(BaseModel):
    server_id: int | None = None
    username: str | None = None
    category: str | None = None
    active: bool | None = None


class UpdateJob(BaseModel):
    id: int | None = None
    active: bool | None = None


@router.post("/")
async def get_jobs(
    job_type: JobData,
    site_name: str,
    page: int = 1,
    limit: int = 100,
):
    database_api = UAClient.database_api

    site_api = database_api.get_site_api(site_name)
    async with site_api as site_api:
        limit = 100 if limit > 100 else limit
        jobs = await site_api.get_jobs(
            category=job_type.category, page=page, limit=limit, active=job_type.active
        )
    return jobs


@router.post("/create")
async def create_job(
    job_type: JobData,
    site_name: str,
):
    database_api = UAClient.database_api

    site_api = database_api.get_site_api(site_name)
    async with site_api as site_api:
        user = await site_api.get_user(job_type.username)
        if user:
            assert job_type.category
            _job = await site_api.create_or_update_job(
                user, job_type.category, server_id=2
            )
            await user.awaitable_attrs.subscribers
            return user


@router.post("/update")
async def update_job(
    job_type: UpdateJob,
    site_name: str,
):
    database_api = UAClient.database_api

    site_api = database_api.get_site_api(site_name)
    async with site_api as site_api:
        stmt = (
            update(JobModel)
            .where(JobModel.id == job_type.id)
            .values(active=job_type.active)
        )
        await site_api.get_session().execute(stmt)
        return True


from fastapi import Query


class MediaDetectionFilter(BaseModel):
    label: str
    score: float


@router.post("/media")
async def get_detected_media(
    filters: list[MediaDetectionFilter],
    site_name: str,
    page: int = Query(1, alias="page", gt=0),
    limit: int = Query(10, alias="limit", gt=0),
    sex: int | None = None,
):
    from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
        MediaDetectionModel,
    )

    database_api = UAClient.database_api
    site_api = database_api.get_site_api(site_name)
    stmt = MediaDetectionModel().filter_stmt(filters, sex)
    offset = (page - 1) * limit
    temp = await site_api.get_session().scalars(stmt.offset(offset).limit(limit))
    media_detections = temp.all()
    # [await x.awaitable_attrs.media for x in media_detections]
    return [x.filepath for x in media_detections]


@router.post("/test")
async def test(site_name: str, filepath_str: str = Query(alias="filepath")):
    from pathlib import Path

    from ultima_scraper_collection.config import site_config_types
    from ultima_scraper_collection.managers.filesystem_manager import FilesystemManager

    database_api = UAClient.database_api
    site_db_api = database_api.get_site_api(site_name)
    config = UAClient.config
    site_config: site_config_types = config.get_site_config(site_name=site_name)
    fsm = FilesystemManager()
    fsm.activate_directory_manager(site_config)
    filepath = Path(filepath_str).as_posix()
    db_filepaths = await site_db_api.get_filepaths(Path(filepath).name)
    performer_identifiers: list[int | str] = []

    for db_filepath in db_filepaths:
        db_content = await db_filepath.get_content()
        await db_content.awaitable_attrs.user
        db_user = db_content.user
        performer_identifiers.extend([db_user.id, db_user.username])

    found_path = None

    for performer_identity in performer_identifiers:
        appearances = filepath.count(str(performer_identity))
        if appearances == 1:
            last_position = filepath.rfind(str(performer_identity))
            part1 = Path(filepath[:last_position])
            part2 = Path(filepath[last_position:])

            for directory in site_config.download_setup.directories:
                if directory.path:
                    temp_unique_path = Path(directory.path, part2)

                    if not temp_unique_path.exists():
                        reversed_parts = []
                        for part in reversed(part1.parts):
                            reversed_parts = [part] + reversed_parts
                            temp_unique_path = Path(
                                directory.path, Path(*reversed_parts), part2
                            )

                            if temp_unique_path.exists():
                                found_path = temp_unique_path
                                break

            if found_path:
                break
        else:
            if appearances > 1:
                breakpoint()

    if found_path:
        return found_path
