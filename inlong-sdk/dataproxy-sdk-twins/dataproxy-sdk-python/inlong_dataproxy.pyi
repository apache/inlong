from collections.abc import Callable
from typing import Optional

UserCallBack = Callable[[str, str, str, int, int, str], int]
# e.g. def callback_func(inlong_group_id: str, inlong_stream_id: str, msg: str, msg_len: int, report_time: int, ip: str) -> int:

class InLongApi:
    def __init__(self) -> None: ...
    def init_api(self, config_path: str) -> int: ...
    def send(
        self,
        groupId: str,
        streamId: str,
        msg: str,
        msgLen: int,
        pyCallback: Optional[UserCallBack] = ...,  # None means no callback
    ) -> int: ...
    def close_api(self, timeout_ms: int) -> int: ...
