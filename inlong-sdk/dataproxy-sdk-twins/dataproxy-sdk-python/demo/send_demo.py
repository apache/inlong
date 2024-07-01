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

import sys

import inlong_dataproxy

# user defined callback function
def callback_func(inlong_group_id, inlong_stream_id, msg, msg_len, report_time, ip):
    print("******this is call back, print info******")
    print("inlong_group_id:", inlong_group_id, ", inlong_stream_id:", inlong_stream_id)
    print("msg_len:", msg_len, ", msg content:", msg)
    print("report_time:", report_time, ", client ip:", ip)
    print("******call back end******")
    return 0

def main():
    if len(sys.argv) < 2:
        print("USAGE: python send_demo.py config_example.json [inlong_group_id] [inlong_stream_id]")
        return

    inlong_api = inlong_dataproxy.InLongApi()

    # step1. init api
    init_status = inlong_api.init_api(sys.argv[1])
    if init_status:
        print("init error, error code is: " + init_status)
        return

    print("---->start sdk successfully")

    count = 5
    inlong_group_id = "test_pulsar_group"
    inlong_stream_id = "test_pulsar_stream"

    if (len(sys.argv) == 4):
        inlong_group_id = sys.argv[2]
        inlong_stream_id = sys.argv[3]

    print("inlong_group_id:", inlong_group_id, ", inlong_stream_id:", inlong_stream_id)

    msg = "python sdk|1234"

    # step2. send message
    print("---->start tc_api_send")
    for i in range(count):
        send_status = inlong_api.send(inlong_group_id, inlong_stream_id, msg, len(msg), callback_func)
        if send_status:
            print("tc_api_send error, error code is: " + send_status)

    # step3. close api
    close_status = inlong_api.close_api(10000)
    if close_status:
        print("close sdk error, error code is: " + close_status)
    else:
        print("---->close sdk successfully")

if __name__ == "__main__":
    main()