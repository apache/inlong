#!/bin/bash
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

set -e

# Check if required files exist
if [ ! -f "build_third_party.sh" ]; then
    echo "Error: build_third_party.sh not found in current directory"
    echo "Please make sure you have mounted the source code directory correctly"
    echo "Expected mount: -v /path/to/dataproxy-sdk-cpp:/dataproxy-sdk-cpp"
    exit 1
fi

if [ ! -f "build.sh" ]; then
    echo "Error: build.sh not found in current directory"
    echo "Please make sure you have mounted the source code directory correctly"
    echo "Expected mount: -v /path/to/dataproxy-sdk-cpp:/dataproxy-sdk-cpp"
    exit 1
fi

echo "=== Building third party dependencies ==="
./build_third_party.sh

echo "=== Building dataproxy-sdk-cpp ==="
./build.sh

echo "=== Build completed successfully ==="