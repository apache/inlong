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
WORKDIR /dataproxy-sdk-twins/

## Switch to Tencent mirror to fix CentOS 7 yum source issues
RUN sed -e 's|^mirrorlist=|#mirrorlist=|g' \
       -e 's|^#baseurl=http://mirror.centos.org|baseurl=https://mirrors.tencent.com|g' \
       -i.bak /etc/yum.repos.d/CentOS-*.repo && \
   yum clean all && \
   yum makecache

# Install necessary dependency packages (excluding python3 to avoid conflicts)
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
        cpp \
        glibc-devel \
        glibc-headers \
        kernel-headers \
        libffi-devel \
    && ln -sf /usr/bin/cpp /lib/cpp \
    && yum clean all && rm -rf /var/cache/yum

# Remove system Python 3.6 to avoid conflicts (if it exists)
RUN yum remove -y python3 python3-libs python3-pip python3-setuptools python3-devel || true && \
    rm -rf /usr/bin/python3* /usr/bin/pip3* /usr/lib/python3* /usr/lib64/python3* || true && \
    yum clean all

# Build and install OpenSSL 1.1.1w (LTS version)
ARG OPENSSL_VERSION=1.1.1w
RUN cd /tmp && \
    wget https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz && \
    tar -xzf openssl-${OPENSSL_VERSION}.tar.gz && \
    cd openssl-${OPENSSL_VERSION} && \
    ./config \
        --prefix=/usr/local/openssl \
        --openssldir=/usr/local/openssl \
        shared \
        zlib \
        enable-ec_nistp_64_gcc_128 \
        -Wl,-rpath=/usr/local/openssl/lib && \
    make -j$(nproc) && \
    make install && \
    echo "/usr/local/openssl/lib" > /etc/ld.so.conf.d/openssl-1.1.1.conf && \
    ldconfig && \
    cd / && rm -rf /tmp/openssl-*

ENV LDFLAGS="-L/usr/local/openssl/lib -Wl,-rpath,/usr/local/openssl/lib"
ENV CPPFLAGS="-I/usr/local/openssl/include"
ENV PKG_CONFIG_PATH="/usr/local/openssl/lib/pkgconfig:${PKG_CONFIG_PATH}"

# Build and install SSL-enabled curl (used by Git and CMake)
ARG CURL_VERSION=7.78.0
RUN cd /tmp && \
    wget https://curl.se/download/curl-${CURL_VERSION}.tar.gz && \
    tar -xzf curl-${CURL_VERSION}.tar.gz && \
    cd curl-${CURL_VERSION} && \
    ./configure --prefix=/usr/local --with-ssl=/usr/local/openssl --with-zlib && \
    make -j$(nproc) && \
    make install && \
    ln -sf /usr/local/bin/curl /usr/bin/curl && \
    cd / && rm -rf /tmp/curl-*

# Install updated version of Git (compile from source)
ARG GIT_VERSION=2.34.1
RUN cd /tmp && \
    wget https://github.com/git/git/archive/v${GIT_VERSION}.tar.gz && \
    tar -xzf v${GIT_VERSION}.tar.gz && \
    cd git-${GIT_VERSION} && \
    make configure && \
    ./configure --prefix=/usr/local --with-openssl=/usr/local/openssl && \
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

# Build and install CMake using system curl
ARG CMAKE_VERSION=3.12.4
RUN cd /tmp && \
    wget https://cmake.org/files/v3.12/cmake-${CMAKE_VERSION}.tar.gz && \
    tar -xzf cmake-${CMAKE_VERSION}.tar.gz && \
    cd cmake-${CMAKE_VERSION} && \
    ./configure --prefix=/usr/local --system-curl && \
    make -j$(nproc) && \
    make install && \
    ln -sf /usr/local/bin/cmake /usr/bin/cmake && \
    ln -sf /usr/local/bin/ctest /usr/bin/ctest && \
    ln -sf /usr/local/bin/cpack /usr/bin/cpack && \
    cd / && rm -rf /tmp/cmake-*

# Build and install Python from source (configurable version)
ARG PYTHON_VERSION=3.8.0
RUN cd /tmp && \
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xzf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --prefix=/usr/local \
        --enable-shared \
        --with-openssl=/usr/local/openssl \
        --with-openssl-rpath=auto \
        --with-ssl-default-suites=openssl \
        --with-system-ffi \
        --with-computed-gotos \
        --enable-loadable-sqlite-extensions && \
    make -j"$(nproc)" && \
    make altinstall && \
    cd / && rm -rf /tmp/Python-*

# Create symlinks for python and pip based on version
RUN PYTHON_MAJOR_MINOR=$(echo ${PYTHON_VERSION} | cut -d. -f1,2) && \
    ln -sf /usr/local/bin/python${PYTHON_MAJOR_MINOR} /usr/local/bin/python && \
    ln -sf /usr/local/bin/python${PYTHON_MAJOR_MINOR} /usr/local/bin/python3 && \
    ln -sf /usr/local/bin/pip${PYTHON_MAJOR_MINOR} /usr/local/bin/pip && \
    ln -sf /usr/local/bin/pip${PYTHON_MAJOR_MINOR} /usr/local/bin/pip3 && \
    echo "/usr/local/lib" >> /etc/ld.so.conf.d/usr-local.conf && ldconfig

# Update PATH to prioritize /usr/local/bin
ENV PATH="/usr/local/bin:${PATH}"

# Verify installations
RUN gcc --version && \
    cmake --version && \
    git --version && \
    python --version && \
    pip --version

# Copy and setup build scripts
COPY build_docker.sh /usr/local/bin/build_docker.sh
RUN chmod +x /usr/local/bin/build_docker.sh

# Set entrypoint to build script
ENTRYPOINT ["/usr/local/bin/build_docker.sh"]