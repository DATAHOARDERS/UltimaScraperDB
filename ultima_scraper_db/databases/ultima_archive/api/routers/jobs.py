from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import or_, orm, select, update
from sqlalchemy.orm import contains_eager, lazyload, sessionmaker

from ultima_scraper_db.databases.ultima_archive.api.client import UAClient
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import JobModel

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


class Job(BaseModel):
    server_id: int | None = None
    category: str | None = None
    active: bool | None = None


class UpdateJob(BaseModel):
    id: int | None = None
    active: bool | None = None


@router.post("/")
async def get_jobs(
    job_type: Job,
    site_name: str,
    page: int = 1,
    limit: int = 100,
):
    database_api = UAClient.database_api

    site_api = database_api.site_apis[site_name]
    limit = 100 if limit > 100 else limit
    jobs = await site_api.get_jobs(category=job_type.category, page=page, limit=limit)
    return jobs


@router.post("/update")
async def update_job(
    job_type: UpdateJob,
    site_name: str,
):
    database_api = UAClient.database_api

    site_api = database_api.site_apis[site_name]
    stmt = (
        update(JobModel)
        .where(JobModel.id == job_type.id)
        .values(active=job_type.active)
    )
    async with site_api.schema.sessionmaker.begin() as session:
        await session.execute(stmt)
    return True
