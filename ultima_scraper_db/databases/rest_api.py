from typing import Any

import aiohttp
from fastapi import APIRouter, FastAPI
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address


class RestAPI(FastAPI):
    Session = aiohttp.ClientSession
    limiter = Limiter(key_func=get_remote_address)

    def __init__(self, **extra: Any):
        super().__init__(**extra)
        self.state.limiter: Limiter = self.limiter
        self.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        self.add_middleware(SlowAPIMiddleware)

    def include_routers(self, routers: list[APIRouter]):
        [self.include_router(x) for x in routers]
