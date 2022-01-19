#!/usr/bin/env bash
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

if [ -z "$DOCKER_USER" ]; then
    echo "Docker user in variable \$DOCKER_USER was not set. Skipping image publishing"
    exit 1
fi

if [ -z "$DOCKER_PASSWORD" ]; then
    echo "Docker password in variable \$DOCKER_PASSWORD was not set. Skipping image publishing"
    exit 1
fi

DOCKER_ORG="${DOCKER_ORG:-inlong}"

docker login ${DOCKER_REGISTRY} -u="$DOCKER_USER" -p="$DOCKER_PASSWORD"
if [ $? -ne 0 ]; then
    echo "Failed to loging to Docker Hub"
    exit 1
fi

MVN_VERSION=`python ./get-project-version.py`
echo "InLong version: ${MVN_VERSION}"

if [[ -z ${DOCKER_REGISTRY} ]]; then
    docker_registry_org=${DOCKER_ORG}
else
    docker_registry_org=${DOCKER_REGISTRY}/${DOCKER_ORG}
    echo "Starting to push images to ${docker_registry_org}..."
fi

set -x

# Fail if any of the subsequent commands fail
set -e

# tag all images
docker tag inlong/manager-web:latest     ${docker_registry_org}/manager-web:latest
docker tag inlong/agent:latest           ${docker_registry_org}/agent:latest
docker tag inlong/dataproxy:latest       ${docker_registry_org}/dataproxy:latest
docker tag inlong/tubemq-manager:latest  ${docker_registry_org}/tubemq-manager:latest
docker tag inlong/tubemq-all:latest      ${docker_registry_org}/tubemq-all:latest
docker tag inlong/tubemq-build:latest    ${docker_registry_org}/tubemq-build:latest
docker tag inlong/dashboard:latest         ${docker_registry_org}/dashboard:latest
docker tag inlong/tubemq-cpp:latest      ${docker_registry_org}/tubemq-cpp:latest

docker tag inlong/manager-web:$MVN_VERSION     ${docker_registry_org}/manager-web:$MVN_VERSION
docker tag inlong/agent:$MVN_VERSION           ${docker_registry_org}/agent:$MVN_VERSION
docker tag inlong/dataproxy:$MVN_VERSION       ${docker_registry_org}/dataproxy:$MVN_VERSION
docker tag inlong/tubemq-manager:$MVN_VERSION  ${docker_registry_org}/tubemq-manager:$MVN_VERSION
docker tag inlong/tubemq-all:$MVN_VERSION      ${docker_registry_org}/tubemq-all:$MVN_VERSION
docker tag inlong/tubemq-build:$MVN_VERSION    ${docker_registry_org}/tubemq-build:$MVN_VERSION
docker tag inlong/dashboard:$MVN_VERSION         ${docker_registry_org}/dashboard:$MVN_VERSION
docker tag inlong/tubemq-cpp:$MVN_VERSION      ${docker_registry_org}/tubemq-cpp:$MVN_VERSION

# Push all images and tags
docker push inlong/manager-web:latest
docker push inlong/agent:latest
docker push inlong/dataproxy:latest
docker push inlong/tubemq-manager:latest
docker push inlong/tubemq-all:latest
docker push inlong/tubemq-build:latest
docker push inlong/dashboard:latest
docker push inlong/tubemq-cpp:latest

docker push inlong/manager-web:$MVN_VERSION
docker push inlong/agent:$MVN_VERSION
docker push inlong/dataproxy:$MVN_VERSION
docker push inlong/tubemq-manager:$MVN_VERSION
docker push inlong/tubemq-all:$MVN_VERSION
docker push inlong/tubemq-build:$MVN_VERSION
docker push inlong/dashboard:$MVN_VERSION
docker push inlong/tubemq-cpp:$MVN_VERSION

echo "Finished pushing images to ${docker_registry_org}"
