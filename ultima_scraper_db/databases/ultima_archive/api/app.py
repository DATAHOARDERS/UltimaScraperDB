from fastapi import APIRouter

routers: list[APIRouter] = []
from ultima_scraper_db.databases.ultima_archive.api.routers.users import router

routers.append(router)
from ultima_scraper_db.databases.ultima_archive.api.routers.jobs import router

routers.append(router)
