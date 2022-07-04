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

ARCH_X86="x86_64"
ARCH_AARCH64="aarch64"
ARCH=$(uname -m)
NEED_BUILD=false
NEED_TAG=false
NEED_PUBLISH=false
NEED_MANIFEST=false

SRC_POSTFIX=""
DES_POSTFIX="-x86"

SHELL_FOLDER=$(cd "$(dirname "$0")";pwd)

MVN_VERSION=$(python ./get-project-version.py)

buildImage() {
  echo "Start building images"
  if [ $ARCH = "$ARCH_AARCH64" ]; then
    sh build-arm-docker-images.sh
  else
    cd "${SHELL_FOLDER}"
    cd ..
    mvn clean install -DskipTests -Pdocker
  fi
  echo "End building images"
}

tagImage() {
  echo "Start tagging images"
  if [ $ARCH = "$ARCH_AARCH64" ]; then
    SRC_POSTFIX="-aarch64"
    DES_POSTFIX="-aarch64"
  fi

  docker tag inlong/manager:latest${SRC_POSTFIX}      ${docker_registry_org}/manager:latest${DES_POSTFIX}
  docker tag inlong/agent:latest${SRC_POSTFIX}           ${docker_registry_org}/agent:latest${DES_POSTFIX}
  docker tag inlong/dataproxy:latest${SRC_POSTFIX}       ${docker_registry_org}/dataproxy:latest${DES_POSTFIX}
  docker tag inlong/tubemq-manager:latest${SRC_POSTFIX}  ${docker_registry_org}/tubemq-manager:latest${DES_POSTFIX}
  docker tag inlong/tubemq-all:latest${SRC_POSTFIX}      ${docker_registry_org}/tubemq-all:latest${DES_POSTFIX}
  docker tag inlong/tubemq-build:latest${SRC_POSTFIX}    ${docker_registry_org}/tubemq-build:latest${DES_POSTFIX}
  docker tag inlong/dashboard:latest${SRC_POSTFIX}         ${docker_registry_org}/dashboard:latest${DES_POSTFIX}
  docker tag inlong/tubemq-cpp:latest${SRC_POSTFIX}      ${docker_registry_org}/tubemq-cpp:latest${DES_POSTFIX}
  docker tag inlong/audit:latest${SRC_POSTFIX}      ${docker_registry_org}/audit:latest${DES_POSTFIX}

  docker tag inlong/manager:${MVN_VERSION}${SRC_POSTFIX} \
    ${docker_registry_org}/manager:${MVN_VERSION}${DES_POSTFIX}
  docker tag inlong/agent:${MVN_VERSION}${SRC_POSTFIX} \
    ${docker_registry_org}/agent:${MVN_VERSION}${DES_POSTFIX}
  docker tag inlong/dataproxy:${MVN_VERSION}${SRC_POSTFIX} \
    ${docker_registry_org}/dataproxy:${MVN_VERSION}${DES_POSTFIX}
  docker tag inlong/tubemq-manager:${MVN_VERSION}${SRC_POSTFIX} \
    ${docker_registry_org}/tubemq-manager:${MVN_VERSION}${DES_POSTFIX}
  docker tag inlong/tubemq-all:${MVN_VERSION}${SRC_POSTFIX}  \
  ${docker_registry_org}/tubemq-all:${MVN_VERSION}${DES_POSTFIX}
  docker tag inlong/tubemq-build:${MVN_VERSION}${SRC_POSTFIX} \
    ${docker_registry_org}/tubemq-build:${MVN_VERSION}${DES_POSTFIX}
  docker tag inlong/dashboard:${MVN_VERSION}${SRC_POSTFIX} \
    ${docker_registry_org}/dashboard:${MVN_VERSION}${DES_POSTFIX}
  docker tag inlong/tubemq-cpp:${MVN_VERSION}${SRC_POSTFIX} \
    ${docker_registry_org}/tubemq-cpp:${MVN_VERSION}${DES_POSTFIX}
  docker tag inlong/audit:${MVN_VERSION}${SRC_POSTFIX}  \
    ${docker_registry_org}/audit:${MVN_VERSION}${DES_POSTFIX}
  echo "End tagging images"
}

publishImages() {
  if [ -z "$DOCKER_USER" ]; then
      echo "Docker user in variable \$DOCKER_USER was not set. Skipping image publishing"
      exit 1
  fi

  if [ -z "$DOCKER_PASSWORD" ]; then
      echo "Docker password in variable \$DOCKER_PASSWORD was not set. Skipping image publishing"
      exit 1
  fi

  DOCKER_ORG="${DOCKER_ORG:-inlong}"

  echo $DOCKER_PASSWORD | docker login ${DOCKER_REGISTRY} -u="$DOCKER_USER" --password-stdin
  if [ $? -ne 0 ]; then
      echo "Failed to login to ${DOCKER_REGISTRY}"
      exit 1
  fi

  if [[ -z ${DOCKER_REGISTRY} ]]; then
      docker_registry_org=${DOCKER_ORG}
  else
      docker_registry_org=${DOCKER_REGISTRY}/${DOCKER_ORG}
      echo "Starting to push images to ${docker_registry_org}..."
  fi

  set -x

  set -e

  pushImage ${docker_registry_org}
}

pushImage() {
  echo "Start pushing images"
  docker_registry_org=$1

  if [ "$ARCH" = "$ARCH_AARCH64" ]; then
    SRC_POSTFIX="-aarch64"
    DES_POSTFIX="-arm64"
  elif [ "$NEED_TAG" = true ]; then
    SRC_POSTFIX="-x86"
  fi

  docker push inlong/manager:latest${SRC_POSTFIX}
  docker push inlong/agent:latest${SRC_POSTFIX}
  docker push inlong/dataproxy:latest${SRC_POSTFIX}
  docker push inlong/tubemq-manager:latest${SRC_POSTFIX}
  docker push inlong/tubemq-all:latest${SRC_POSTFIX}
  docker push inlong/tubemq-build:latest${SRC_POSTFIX}
  docker push inlong/dashboard:latest${SRC_POSTFIX}
  docker push inlong/tubemq-cpp:latest${SRC_POSTFIX}
  docker push inlong/audit:latest${SRC_POSTFIX}

  docker push inlong/manager:${MVN_VERSION}${SRC_POSTFIX}
  docker push inlong/agent:${MVN_VERSION}${SRC_POSTFIX}
  docker push inlong/dataproxy:${MVN_VERSION}${SRC_POSTFIX}
  docker push inlong/tubemq-manager:${MVN_VERSION}${SRC_POSTFIX}
  docker push inlong/tubemq-all:${MVN_VERSION}${SRC_POSTFIX}
  docker push inlong/tubemq-build:${MVN_VERSION}${SRC_POSTFIX}
  docker push inlong/dashboard:${MVN_VERSION}${SRC_POSTFIX}
  docker push inlong/tubemq-cpp:${MVN_VERSION}${SRC_POSTFIX}
  docker push inlong/audit:${MVN_VERSION}${SRC_POSTFIX}

  echo "Finished pushing images to ${docker_registry_org}"
}

pushManifest() {
  echo "Start pushing manifest ..."
  docker manifest create --insecure --amend inlong/manager:latest \
    inlong/manager:latest-arm64 inlong/manager:latest-x86
  docker manifest create --insecure --amend inlong/agent:latest \
    inlong/agent:latest-arm64 inlong/agent:latest-x86
  docker manifest create --insecure --amend inlong/dataproxy:latest \
    inlong/dataproxy:latest-arm64 inlong/dataproxy:latest-x86
  docker manifest create --insecure --amend inlong/tubemq-manager:latest \
    inlong/tubemq-manager:latest-arm64 inlong/tubemq-manager:latest-x86
  docker manifest create --insecure --amend inlong/tubemq-all:latest \
    inlong/tubemq-all:latest-arm64 inlong/tubemq-all:latest-x86
  docker manifest create --insecure --amend inlong/tubemq-build:latest \
    inlong/tubemq-build:latest-arm64 inlong/tubemq-build:latest-x86
  docker manifest create --insecure --amend inlong/dashboard:latest \
    inlong/dashboard:latest-arm64 inlong/dashboard:latest-x86
  docker manifest create --insecure --amend inlong/tubemq-cpp:latest \
    inlong/tubemq-cpp:latest-arm64 inlong/tubemq-cpp:latest-x86
  docker manifest create --insecure --amend inlong/audit:latest \
    inlong/audit:latest-arm64 inlong/audit:latest-x86

  docker manifest create --insecure --amend inlong/manager:${MVN_VERSION} \
    inlong/manager:${MVN_VERSION}-arm64 inlong/manager:${MVN_VERSION}-x86
  docker manifest create --insecure --amend inlong/agent:${MVN_VERSION} \
    inlong/agent:${MVN_VERSION}-arm64 inlong/agent:${MVN_VERSION}-x86
  docker manifest create --insecure --amend inlong/dataproxy:${MVN_VERSION} \
    inlong/dataproxy:${MVN_VERSION}-arm64 inlong/dataproxy:${MVN_VERSION}-x86
  docker manifest create --insecure --amend inlong/tubemq-manager:${MVN_VERSION} \
    inlong/tubemq-manager:${MVN_VERSION}-arm64 inlong/tubemq-manager:${MVN_VERSION}-x86
  docker manifest create --insecure --amend inlong/tubemq-all:${MVN_VERSION} \
    inlong/tubemq-all:${MVN_VERSION}-arm64 inlong/tubemq-all:${MVN_VERSION}-x86
  docker manifest create --insecure --amend inlong/tubemq-build:${MVN_VERSION} \
    inlong/tubemq-build:${MVN_VERSION}-arm64 inlong/tubemq-build:${MVN_VERSION}-x86
  docker manifest create --insecure --amend inlong/dashboard:${MVN_VERSION} \
    inlong/dashboard:${MVN_VERSION}-arm64 inlong/dashboard:${MVN_VERSION}-x86
  docker manifest create --insecure --amend inlong/tubemq-cpp:${MVN_VERSION} \
    inlong/tubemq-cpp:${MVN_VERSION}-arm64 inlong/tubemq-cpp:${MVN_VERSION}-x86
  docker manifest create --insecure --amend inlong/audit:${MVN_VERSION} \
    inlong/audit:${MVN_VERSION}-arm64 inlong/audit:${MVN_VERSION}-x86
  echo "End pushing manifest"
}

help() {
  cat <<EOF
Usage: ./publish-by-arch.sh [option]
Options:
  -b/--build        Add build operation before publish. Build docker images by arch.
  -t/--tag          Add tag operation before publish. Add arch after version.
  -p/--publish      Publish images according to docker registry information.
  -m/--manifest     Push manifest. This option doesn't need arch.
  -h/--help         Show help information.
Example:
  Use "./publish-by-arch.sh -b" to publish arm images after build operation.
  Use "./publish-by-arch.sh -t" to publish amd images after tag already x86 images as x86.
EOF
}

for arg in "$@"
do
  if [ "$arg" = "-b" ] || [ "$arg" = "--build" ]; then
    NEED_BUILD=true
  elif [ "$arg" = "-t" ] || [ "$arg" = "--tag" ]; then
    NEED_TAG=true
  elif [ "$arg" = "-m" ] || [ "$arg" = "--manifest" ]; then
    NEED_MANIFEST=true
  elif [ "$arg" = "-p" ] || [ "$arg" = "--publish" ]; then
    NEED_PUBLISH=true
  elif [ "$arg" = "-h" ] || [ "$arg" = "--help" ]; then
    help
    exit 0
  else
    echo "Wrong param: $arg. Please check help information."
    exit 1
  fi
done

if [ "$NEED_BUILD" = true ]; then
  buildImage
fi

if [ "$NEED_TAG" = true ]; then
  tagImage
fi

if [ "$NEED_PUBLISH" = true ]; then
  publishImages
fi

if [ "$NEED_MANIFEST" = true ]; then
  pushManifest
fi
