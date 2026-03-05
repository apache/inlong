#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

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
