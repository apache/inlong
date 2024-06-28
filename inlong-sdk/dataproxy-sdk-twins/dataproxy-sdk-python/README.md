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
