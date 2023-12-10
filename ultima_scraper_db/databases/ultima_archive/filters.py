from datetime import datetime


class AuthedInfoFilter:
    def __init__(
        self,
        between_dates: tuple[datetime, datetime] | None = None,
        exclude_between_dates: tuple[datetime, datetime] | None = None,
        active: bool | None = None,
    ) -> None:
        self.between_dates = between_dates
        self.exclude_between_dates = exclude_between_dates
        self.active = active
