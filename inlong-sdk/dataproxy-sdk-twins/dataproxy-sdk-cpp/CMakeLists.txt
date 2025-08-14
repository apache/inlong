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

cmake_minimum_required(VERSION 3.1)

project(DataProxySDK)

# compile level -O2
set(CMAKE_CXX_FLAGS "-std=c++11 -O2 -g -Wall  -Wsign-compare -Wfloat-equal -fno-strict-aliasing -fPIC -DASIO_STANDALONE")

include_directories(${PROJECT_SOURCE_DIR}/third_party/include)
include_directories(${PROJECT_SOURCE_DIR}/third_party/rapidjson/src/rapidjson/include)
include_directories(release/inc)
include_directories(src/utils)
include_directories(src/core)
include_directories(src/manager)
include_directories(src/group)
include_directories(src/protocol)
include_directories(src/client)
include_directories(src/metric)

link_directories(${PROJECT_SOURCE_DIR}/third_party/lib)
link_directories(${PROJECT_SOURCE_DIR}/third_party/lib64)

add_subdirectory(src/utils)
add_subdirectory(src/config)
add_subdirectory(src/core)
add_subdirectory(src/manager)
add_subdirectory(src/group)
add_subdirectory(src/protocol)
add_subdirectory(src/client)
add_subdirectory(release)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/release/lib)

aux_source_directory(src/utils UTILS)
aux_source_directory(src/config CONFIGS)
aux_source_directory(src/core CORE)
aux_source_directory(src/manager MANAGER)
aux_source_directory(src/group GROUP)
aux_source_directory(src/protocol PROTOCOL)
aux_source_directory(src/client CLIENT)

# static library
add_library(dataproxy_sdk STATIC ${UTILS} ${CONFIGS} ${CORE} ${MANAGER} ${GROUP} ${PROTOCOL} ${CLIENT} ${METRIC})

set_target_properties(dataproxy_sdk PROPERTIES OUTPUT_NAME "dataproxy_sdk" PREFIX "")

target_link_libraries(dataproxy_sdk liblog4cplusS.a libsnappy.a libcurl.a libssl.a libcrypto.a libzstd.a -lbrotlidec -lbrotlicommon)

