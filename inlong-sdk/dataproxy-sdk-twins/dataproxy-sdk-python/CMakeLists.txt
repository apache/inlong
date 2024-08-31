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

cmake_minimum_required(VERSION 3.5)
project(dataproxy-sdk-python)

set(CMAKE_CXX_STANDARD 11)

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/dataproxy-sdk-cpp/src/core")

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/dataproxy-sdk-cpp/third_party/lib")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/dataproxy-sdk-cpp/third_party/lib64")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/dataproxy-sdk-cpp/release/lib")

add_subdirectory(pybind11)

link_directories("${CMAKE_CURRENT_SOURCE_DIR}/dataproxy-sdk-cpp/third_party/lib")
link_directories("${CMAKE_CURRENT_SOURCE_DIR}/dataproxy-sdk-cpp/third_party/lib64")
link_directories("${CMAKE_CURRENT_SOURCE_DIR}/dataproxy-sdk-cpp/release/lib")

pybind11_add_module(inlong_dataproxy inlong_dataproxy.cpp)

target_link_libraries(inlong_dataproxy PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/dataproxy-sdk-cpp/release/lib/dataproxy_sdk.a" liblog4cplusS.a libsnappy.a libcurl.a libssl.a libcrypto.a)
