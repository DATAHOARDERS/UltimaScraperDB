from fastapi import APIRouter

routers: list[APIRouter] = []
router_names = [
    "users",
    "jobs",
    "client",
    "remote_urls",
    "notifications",
    "filepaths",
    "media_detections",
    "sites",
    "hosts",
    "search",
]

for name in router_names:
    module_path = f"ultima_scraper_db.databases.ultima_archive.api.routers.{name}"
    router = getattr(__import__(module_path, fromlist=["router"]), "router")
    routers.append(router)
