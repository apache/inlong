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

# Use CentOS 7 as base image
FROM centos:7

# Set working directory
WORKDIR /dataproxy-sdk-cpp/

## Switch to Tencent mirror to fix CentOS 7 yum source issues
RUN sed -e 's|^mirrorlist=|#mirrorlist=|g' \
       -e 's|^#baseurl=http://mirror.centos.org|baseurl=https://mirrors.tencent.com|g' \
       -i.bak /etc/yum.repos.d/CentOS-*.repo && \
   yum clean all && \
   yum makecache

# Install necessary dependency packages
RUN yum update -y && \
    yum install -y \
        gcc \
        gcc-c++ \
        make \
        autoconf \
        automake \
        libtool \
        pkgconfig \
        wget \
        openssl-devel \
        zlib-devel \
    && yum clean all && rm -rf /var/cache/yum

# Build and install SSL-enabled curl (used by Git and CMake)
ARG CURL_VERSION=7.78.0
RUN cd /tmp && \
    wget https://curl.se/download/curl-${CURL_VERSION}.tar.gz && \
    tar -xzf curl-${CURL_VERSION}.tar.gz && \
    cd curl-${CURL_VERSION} && \
    ./configure --prefix=/usr/local --with-ssl --with-zlib && \
    make -j"$(nproc)" && \
    make install && \
    ln -sf /usr/local/bin/curl /usr/bin/curl && \
    echo "/usr/local/lib" > /etc/ld.so.conf.d/usr-local.conf && ldconfig && \
    cd / && rm -rf /tmp/curl-*

# Verify curl SSL support
RUN curl --version

# Build and install Git (compile from source with CURL support)
ARG GIT_VERSION=2.34.1
RUN cd /tmp && \
    wget https://github.com/git/git/archive/v${GIT_VERSION}.tar.gz && \
    tar -xzf v${GIT_VERSION}.tar.gz && \
    cd git-${GIT_VERSION} && \
    make configure && \
    ./configure --prefix=/usr/local --with-curl=/usr/local && \
    make -j"$(nproc)" \
        NO_GETTEXT=YesPlease \
        NO_EXPAT=YesPlease \
        NO_PERL=YesPlease \
        NO_TCLTK=YesPlease && \
    make install \
        NO_GETTEXT=YesPlease \
        NO_EXPAT=YesPlease \
        NO_PERL=YesPlease \
        NO_TCLTK=YesPlease && \
    ln -sf /usr/local/bin/git /usr/bin/git && \
    cd / && rm -rf /tmp/git-* /tmp/v${GIT_VERSION}.tar.gz

# Verify gcc and git versions
RUN gcc --version && git --version

# Build and install CMake using system curl
ARG CMAKE_VERSION=3.12.4
RUN cd /tmp && \
    wget https://cmake.org/files/v3.12/cmake-${CMAKE_VERSION}.tar.gz && \
    tar -xzf cmake-${CMAKE_VERSION}.tar.gz && \
    cd cmake-${CMAKE_VERSION} && \
    ./configure --prefix=/usr/local --system-curl && \
    make -j"$(nproc)" && \
    make install && \
    ln -sf /usr/local/bin/cmake /usr/bin/cmake && \
    ln -sf /usr/local/bin/ctest /usr/bin/ctest && \
    ln -sf /usr/local/bin/cpack /usr/bin/cpack && \
    cd / && rm -rf /tmp/cmake-*

# Verify CMake version
RUN cmake --version

# Copy and setup build script from docker directory
COPY build_docker.sh /usr/local/bin/build_docker.sh
RUN chmod +x /usr/local/bin/build_docker.sh

# Set entrypoint to build script
ENTRYPOINT ["/usr/local/bin/build_docker.sh"]