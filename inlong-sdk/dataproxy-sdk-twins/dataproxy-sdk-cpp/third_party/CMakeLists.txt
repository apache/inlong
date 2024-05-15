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

project(dataproxysdk_third_party)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error")

include(ExternalProject)

ExternalProject_Add(
        log4cplus
        GIT_REPOSITORY https://github.com/log4cplus/log4cplus.git
        GIT_TAG REL_2_0_8
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${PROJECT_SOURCE_DIR} -DBUILD_SHARED_LIBS=OFF
)

ExternalProject_Add(
        snappy_proj
        URL https://github.com/google/snappy/archive/1.1.8.tar.gz
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${PROJECT_SOURCE_DIR}
        -DSNAPPY_BUILD_TESTS=OFF
        -DHAVE_LIBLZO2=OFF
        -DCMAKE_CXX_FLAGS=-fPIC
        TEST_BEFORE_INSTALL 0
        BUILD_IN_SOURCE 1
)

ExternalProject_Add(
        curl_proj
        URL https://github.com/curl/curl/releases/download/curl-7_78_0/curl-7.78.0.tar.gz
        CONFIGURE_COMMAND ./configure --prefix=${PROJECT_SOURCE_DIR} --without-zlib --without-ssl --disable-shared --disable-ldap && make install
        TEST_BEFORE_INSTALL 0
        BUILD_IN_SOURCE 1
)

ExternalProject_Add(
        rapidjson
        PREFIX "rapidjson"
        URL "https://github.com/Tencent/rapidjson/archive/v1.1.0.tar.gz"
        INSTALL_DIR ${PROJECT_SOURCE_DIR}
        CMAKE_ARGS
        -DRAPIDJSON_BUILD_TESTS=OFF
        -DRAPIDJSON_BUILD_DOC=OFF
        -DRAPIDJSON_BUILD_EXAMPLES=OFF
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        UPDATE_COMMAND ""
)

ExternalProject_Add(
        asio
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/asio
        BINARY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/asio/asio/
        URL https://github.com/chriskohlhoff/asio/archive/asio-1-28-0.tar.gz
        CONFIGURE_COMMAND ./autogen.sh
        COMMAND ./configure --prefix=${PROJECT_SOURCE_DIR}/ CFLAGS=-std=c++11 CPPFLAGS=-std=c++11 CXXFLAGS=-std=c++11 --with-openssl=${PROJECT_SOURCE_DIR}
)

ExternalProject_Add(
        openssl
        URL https://www.openssl.org/source/openssl-1.1.1q.tar.gz
        CONFIGURE_COMMAND ./config --prefix=${PROJECT_SOURCE_DIR}
        BUILD_COMMAND make
        INSTALL_COMMAND make install
        BUILD_IN_SOURCE 1
)