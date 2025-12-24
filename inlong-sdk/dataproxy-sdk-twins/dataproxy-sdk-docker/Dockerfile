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

FROM centos:7

ARG PYTHON_VERSION=3.8.0
ARG CMAKE_VERSION=3.12.4
ARG CURL_VERSION=7.78.0
ARG OPENSSL_VERSION=1.1.1w
ARG GIT_VERSION=2.34.1

WORKDIR /workspace

RUN sed -e 's|^mirrorlist=|#mirrorlist=|g' \
        -e 's|^#baseurl=http://mirror.centos.org|baseurl=https://mirrors.tencent.com|g' \
        -i.bak /etc/yum.repos.d/CentOS-*.repo && \
    yum clean all && \
    yum makecache && \
    yum update -y && \
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
        glibc-devel \
        glibc-headers \
        kernel-headers \
        libffi-devel \
        curl-devel \
        expat-devel \
        gettext-devel \
        perl-CPAN \
        perl-devel \
        && \
    yum clean all && \
    rm -rf /var/cache/yum

# Build and install Git, Curl and CMake
RUN cd /tmp && \
    # Build Git \
    wget https://mirrors.edge.kernel.org/pub/software/scm/git/git-${GIT_VERSION}.tar.gz && \
    tar -xzf git-${GIT_VERSION}.tar.gz && \
    cd git-${GIT_VERSION} && \
    make prefix=/usr/local all -j"$(nproc)" && \
    make prefix=/usr/local install && \
    cd /tmp && \
    rm -rf git-* && \
    # Build Curl \
    wget https://curl.se/download/curl-${CURL_VERSION}.tar.gz && \
    tar -xzf curl-${CURL_VERSION}.tar.gz && \
    cd curl-${CURL_VERSION} && \
    ./configure \
        --prefix=/usr/local \
        --with-ssl \
        --with-zlib \
        && \
    make -j"$(nproc)" && \
    make install && \
    ln -sf /usr/local/bin/curl /usr/bin/curl && \
    echo "/usr/local/lib" > /etc/ld.so.conf.d/usr-local.conf && \
    ldconfig && \
    cd /tmp && \
    rm -rf curl-* && \
    # Install CMake \
    wget https://cmake.org/files/v3.12/cmake-${CMAKE_VERSION}-Linux-x86_64.tar.gz && \
    tar -xzf cmake-${CMAKE_VERSION}-Linux-x86_64.tar.gz -C /usr/local --strip-components=1 && \
    cd / && \
    rm -rf /tmp/*

# Build and install OpenSSL
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
        -Wl,-rpath=/usr/local/openssl/lib \
        && \
    make -j$(nproc) && \
    make install && \
    echo "/usr/local/openssl/lib" > /etc/ld.so.conf.d/openssl-1.1.1.conf && \
    ldconfig && \
    cd / && \
    rm -rf /tmp/*

# Set OpenSSL environment variables
ENV LDFLAGS="-L/usr/local/openssl/lib -Wl,-rpath,/usr/local/openssl/lib" \
    CPPFLAGS="-I/usr/local/openssl/include" \
    PKG_CONFIG_PATH="/usr/local/openssl/lib/pkgconfig:${PKG_CONFIG_PATH}" \
    PATH="/usr/local/bin:${PATH}"

# Build and install Python
RUN cd /tmp && \
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xzf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure \
        --prefix=/usr/local \
        --enable-shared \
        --with-openssl=/usr/local/openssl \
        --with-openssl-rpath=auto \
        --with-ssl-default-suites=openssl \
        --with-system-ffi \
        --with-computed-gotos \
        --enable-loadable-sqlite-extensions \
        && \
    make -j"$(nproc)" && \
    make altinstall && \
    PYTHON_MAJOR_MINOR=$(echo ${PYTHON_VERSION} | cut -d. -f1,2) && \
    ln -sf /usr/local/bin/python${PYTHON_MAJOR_MINOR} /usr/local/bin/python && \
    ln -sf /usr/local/bin/python${PYTHON_MAJOR_MINOR} /usr/local/bin/python3 && \
    ln -sf /usr/local/bin/pip${PYTHON_MAJOR_MINOR} /usr/local/bin/pip && \
    ln -sf /usr/local/bin/pip${PYTHON_MAJOR_MINOR} /usr/local/bin/pip3 && \
    echo "/usr/local/lib" >> /etc/ld.so.conf.d/usr-local.conf && \
    ldconfig && \
    cd / && \
    rm -rf /tmp/*

# Verify installations and create environment info
RUN gcc --version && \
    cmake --version && \
    git --version && \
    python --version && \
    pip --version

CMD ["/bin/bash"]