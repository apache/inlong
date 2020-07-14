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


cmake_minimum_required (VERSION 3.1)

project (TubeMQ)

set (CXX_FLAGS
    -g
    -fPIC
    -Wall
    -D__STDC_FORMAT_MACROS
    -Wno-unused-parameter
    -Wno-unused-function
    -Wunused-variable
    -Wunused-value
    -Wshadow
    -Wcast-qual
    -Wcast-align
    -Wwrite-strings
    -Wsign-compare
    -Winvalid-pch
    -fms-extensions
    -Wfloat-equal
    -Wextra
    -std=c++11
    )

INCLUDE_DIRECTORIES(include)

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/rapidjson/include)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/third_party/log4cplus/include)
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/third_party/log4cplus/include)
LINK_DIRECTORIES(${CMAKE_BINARY_DIR}/third_party/log4cplus/lib)

ADD_SUBDIRECTORY(src)
ADD_SUBDIRECTORY(third_party)
ADD_SUBDIRECTORY(example)


