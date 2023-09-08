from fastapi import APIRouter, FastAPI


class RestAPI(FastAPI):
    def include_routers(self, routers: list[APIRouter]):
        [self.include_router(x) for x in routers]
