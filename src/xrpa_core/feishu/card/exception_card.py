from datetime import datetime

from .template import CardTemplate


class ExceptionCard(CardTemplate):
    def __init__(
        self,
        app_info: str,
        exc_msg: str,
        track_info: str,
        time: datetime | None = None,
    ):
        self.app_info = app_info
        self.exc_msg = exc_msg
        self.track_info = track_info
        self.time = time or datetime.now()
        super().__init__(
            template_id="AAqe3MQQoZD4c",
            template_version="1.0.3",
            variable={
                "raise_time": self.time.strftime("%Y-%m-%d %H:%M:%S"),
                "app_info": self.app_info,
                "exception": self.exc_msg,
                "track_info": self.track_info,
            },
        )
