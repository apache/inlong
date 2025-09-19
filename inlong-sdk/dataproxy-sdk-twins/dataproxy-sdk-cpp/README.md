<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# DataProxy-SDK-cpp

dataproxy-sdk cpp version, used for sending data to dataproxy

## Features

- **TCP Send**: Traditional TCP-based data transmission to DataProxy
- **HTTP Send**: Direct HTTP POST requests to DataProxy endpoints

## Prerequisites

* CMake 3.1+
* snappy
* curl
* rapidjson
* asio
* log4cplus
* zstd
* brotli

## Build

Go to the dataproxy-sdk-cpp root, and run

```
./build_third_party.sh

./build.sh
```

## Config Parameters

Refer to `release/conf/config_example.json`.

| name                     | default value                                                      | description                                                                                                |
|:-------------------------|:-------------------------------------------------------------------|:-----------------------------------------------------------------------------------------------------------|
| thread_num               | 10                                                                 | number of network sending threads                                                                          |
| inlong_group_ids         | ""                                                                 | the list of inlong_group_id, seperated by commas, such as "b_inlong_group_test_01, b_inlong_group_test_02" |
| enable_groupId_isolation | false                                                              | whether different groupid data using different buffer pools inside the sdk                                 |
| buffer_num_per_groupId   | 5                                                                  | number of buffer pools of each groupid                                                                     |
| enable_pack              | true                                                               | whether multiple messages are packed while sending to dataproxy                                            |
| pack_size                | 4096                                                               | byte, pack messages and send to dataproxy when the data in buffer pool exceeds this value                  |
| ext_pack_size            | 16384                                                              | byte, maximum length of a message                                                                          |
| enable_zip               | true                                                               | whether zip data while sending to dataproxy                                                                |
| min_ziplen               | 512                                                                | byte, minimum zip len                                                                                      |
| enable_retry             | true                                                               | whether do resend while failed to send data                                                                |
| retry_ms                 | 3000                                                               | millisecond, resend interval                                                                               |
| retry_num                | 3                                                                  | maximum resend times                                                                                       |
| max_active_proxy         | 3                                                                  | maximum number of established connections with dataproxy                                                   |
| max_buf_pool             | 50 `*`1024`*` 1024                                                 | byte, the size of buffer pool                                                                              |
| log_num                  | 10                                                                 | maximum number of log files                                                                                |
| log_size                 | 10                                                                 | MB, maximum size of one log file                                                                           |
| log_level                | 2                                                                  | log level: trace(4)>debug(3)>info(2)>warn(1)>error(0)                                                      |
| log_file_type            | 2                                                                  | type of log output: 2->file, 1->console                                                                    |
| log_path                 | ./logs/                                                            | log path                                                                                                   |
| proxy_update_interval    | 10                                                                 | interval of requesting and updating dataproxy lists from manager                                           |
| manager_url              | "http://127.0.0.1:8099/inlong/manager/openapi/dataproxy/getIpList" | the url of manager openapi                                                                                 |
| need_auth                | false                                                              | whether need authentication while interacting with manager                                                 |
| auth_id                  | ""                                                                 | authenticate id if need authentication                                                                     |
| auth_key                 | ""                                                                 | authenticate key if need authentication                                                                    |

## Usage

1. First, init dataproxy-sdk, there are two ways you can choose:

- A) `int32_t InitApi(const char* config_file)`. Here, `config_file` is the path of your config file, and absolute
  path is recommended. Note that only once called is needed in one process.

2. Then, send data. There are two ways to send data:

### TCP Send

   `int32_t Send(const char* inlong_group_id, const char* inlong_stream_id, const char* msg, int32_t msg_len, UserCallBack call_back = NULL)`.
   If you set `call_back`, it will be callbacked if your data failed to send. See the signature of `UserCallBack`
   in `src/core/sdk_msg.h`.

### HTTP Send

   `int32_t SendHttp(const char* url, const char* body, int timeout = 5)`.
   This method sends HTTP POST requests directly to the specified URL.

   Parameters:

- `url`: The target HTTP endpoint URL
- `body`: The HTTP request body (usually JSON format)
- `timeout`: Request timeout in seconds (default: 5)

   Returns:

- `0`: Success
- Non-zero: HTTP error code or network error

   Example:

```cpp
   std::string url = "http://127.0.0.1:8080/inlong/dataproxy/send";
   std::string body = R"({"groupId":"test_group","streamId":"test_stream","msg":"test message"})";
   int result = inlong_api.SendHttp(url.c_str(), body.c_str(), 5);
   if (result == 0) {
       std::cout << "HTTP send success" << std::endl;
   } else {
       std::cout << "HTTP send failed, code=" << result << std::endl;
   }
```

3. Finally, close sdk if no more data to be sent: `int32_t CloseApi(int32_t max_waitms)`. Here, `max_waitms` is the
   interval of waiting data in memory to be sent.
4. Note, the above functions return 0 if success, otherwise it means failure. As for other return results, please refer
   to `SDKInvalidResult` in `src/core/inlong_api.h`.

## Demo

1. Refer to `release/demo/send_demo.cc`.
2. Static lib is in `release/lib`. Header file is in `release/lib`.

### TCP Send Demo

The demo shows how to use TCP send functionality:

```cpp
InLongApi inlong_api;
// Initialize SDK
inlong_api.InitApi("config.json");

// Send data via TCP
std::string msg = "test message";
inlong_api.Send("test_group", "test_stream", msg.c_str(), msg.length());

// Close SDK
inlong_api.CloseApi(10000);
```

### HTTP Send Demo

The demo also includes HTTP send functionality:

```cpp
InLongApi inlong_api;
// Initialize SDK
inlong_api.InitApi("config.json");

// Send data via HTTP
std::string http_url = "http://127.0.0.1:8080/inlong/dataproxy/send";
std::string http_body = R"({"groupId":"test_cpp_sdk","streamId":"stream1","msg":"this is a http test"})";
int http_ret = inlong_api.SendHttp(http_url.c_str(), http_body.c_str(), 5);
if (http_ret == 0) {
    std::cout << "HTTP send success" << std::endl;
} else {
    std::cout << "HTTP send failed, code=" << http_ret << std::endl;
}

// Close SDK
inlong_api.CloseApi(10000);
```
