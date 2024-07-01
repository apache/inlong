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

# DataProxy-SDK-Python
Dataproxy-SDK Python version, used for sending data to InLong dataproxy.

InLong Dataproxy Python SDK is a wrapper over the existing [C++ SDK](https://github.com/apache/inlong/tree/master/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-cpp) and exposes all of the same features.

## Prerequisites
- CMake 3.5+
- Python 3.6+

## Build
Go to the dataproxy-sdk-python root directory, and run

```bash
chmod +x ./build.sh
./build.sh
```

After the build process finished, you can import the package (`import inlong_dataproxy`) in your python project to use InLong dataproxy.

> **Note**: When the C++ SDK or the version of Python you're using is updated, you'll need to rebuild it by re-executing the `build.sh` script

## Config Parameters

Refer to `demo/config_example.json`.

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
| manager_url              | "http://127.0.0.1:8083/inlong/manager/openapi/dataproxy/getIpList" | the url of manager openapi                                                                                 |
| need_auth                | false                                                              | whether need authentication while interacting with manager                                                 |
| auth_id                  | ""                                                                 | authenticate id if need authentication                                                                     |
| auth_key                 | ""                                                                 | authenticate key if need authentication                                                                    |

## Usage

Follow these steps to use the DataProxy Python SDK:

1. **Initialize the SDK**: Before using the SDK, you need to initialize it by calling `init_api(config_file)`. The `config_file` parameter is the path to your configuration file. It is recommended to use an absolute path. Please note that this function only needs to be called once per process.

   Example:
   ```python
   inlong_api = inlong_dataproxy.InLongApi()
   inlong_api.init_api("path/to/your/config_file.json")
   ```

2. **Send data**: To send data, use the `send(inlong_group_id, inlong_stream_id, msg, msg_len, call_back_func = null)` function. The parameters are as follows:
    - `inlong_group_id`: The group ID associated with the data.
    - `inlong_stream_id`: The stream ID associated with the data.
    - `msg`: The data message to be sent.
    - `msg_len`: The length of the data message.
    - `call_back_func` (optional): A callback function that will be called if your data fails to send.

   Example:
   ```python
   inlong_api.send("your_inlong_group_id", "your_inlong_stream_id", "your_message", len("your_message"), call_back_func = your_callback_function)
   ```

3. **Close the SDK**: Once you have no more data to send, close the SDK by calling `close_api(max_waitms)`. The `max_waitms` parameter is the maximum time interval (in milliseconds) to wait for data in memory to be sent.

   Example:
   ```python
   inlong_api.close_api(1000)
   ```

4. **Function return values**: The functions mentioned above return 0 if they are successful, and a non-zero value indicates failure. Make sure to check the return values to ensure proper execution.

## Demo

You can refer to the `/demo/send_demo.py` file. To run this demo, you first need to ensure that the SDK has been built and installed properly. Then, follow these steps:

1. Navigate to the `demo` directory in your terminal or command prompt.
2. Modify the configuration settings in `config_example.json` as needed to match your specific use case.
3. Execute the following command, replacing `[inlong_group_id]` and `[inlong_stream_id]` with the appropriate IDs:

   ```bash
   python send_demo.py config_example.json [inlong_group_id] [inlong_stream_id]
   ```