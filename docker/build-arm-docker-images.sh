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

cd ..
mvn clean install -DskipTests
cd -

tag="1.2.0-incubating"
manager_tarball="../inlong-manager/manager-web/target/*manager-web*bin.tar.gz"
agent_tarball="../inlong-agent/agent-release/target/*agent*bin.tar.gz"
audit_tarball="../inlong-audit/audit-release/target/*audit*bin.tar.gz"
dataproxy_tarball="../inlong-dataproxy/dataproxy-dist/target/*dataproxy*bin.tar.gz"
tubemq_manager_tarball="../inlong-tubemq/tubemq-manager/target/*tubemq-manager*bin.tar.gz"
tubemq_all_tarball="../inlong-tubemq/tubemq-server/target/*tubemq-server*bin.tar.gz"

manager_dockerfile_path="../inlong-manager/manager-docker/"
agent_dockerfile_path="../inlong-agent/agent-docker/"
audit_dockerfile_path="../inlong-audit/audit-docker/"
dataproxy_dockerfile_path="../inlong-dataproxy/dataproxy-docker/"
tubemq_manager_dockerfile_path="../inlong-tubemq/tubemq-docker/tubemq-manager/"
tubemq_all_dockerfile_path="../inlong-tubemq/tubemq-docker/tubemq-all/"

manager_tarball_name=$(basename $manager_tarball)
agent_tarball_name=$(basename $agent_tarball)
audit_tarball_name=$(basename $audit_tarball)
dataproxy_tarball_name=$(basename $dataproxy_tarball)
dashboard_file_name="build"
tubemq_manager_tarball_name=$(basename $tubemq_manager_tarball)
tubemq_all_tarball_name=$(basename $tubemq_all_tarball)

MANAGER_TARBALL="target/${manager_tarball_name}"
DATAPROXY_TARBALL="target/${dataproxy_tarball_name}"
AUDIT_TARBALL="target/${audit_tarball_name}"
TUBEMQ_MANAGER_TARBALL="target/${tubemq_manager_tarball_name}"
TUBEMQ_TARBALL="target/${tubemq_all_tarball_name}"
DASHBOARD_FILE="${dashboard_file_name}"
AGENT_TARBALL="target/${agent_tarball_name}"

cp ${manager_tarball} ${manager_dockerfile_path}/target/${manager_tarball_name}
cp ${agent_tarball} ${agent_dockerfile_path}/target/${agent_tarball_name}
cp ${audit_tarball} ${audit_dockerfile_path}/target/${audit_tarball_name}
cp ${dataproxy_tarball} ${dataproxy_dockerfile_path}/target/${dataproxy_tarball_name}
cp ${tubemq_manager_tarball} ${tubemq_manager_dockerfile_path}/target/${tubemq_manager_tarball_name}
cp ${tubemq_all_tarball} ${tubemq_all_dockerfile_path}/target/${tubemq_all_tarball_name}

docker build -t inlong/manager:${tag} ../inlong-manager/manager-docker/ --build-arg MANAGER_TARBALL=${MANAGER_TARBALL}
docker build -t inlong/dataproxy:${tag} ../inlong-dataproxy/dataproxy-docker/ --build-arg DATAPROXY_TARBALL=${DATAPROXY_TARBALL}
docker build -t inlong/audit:${tag} ../inlong-audit/audit-docker/ --build-arg AUDIT_TARBALL=${AUDIT_TARBALL}
docker build -t inlong/tubemq-manager:${tag} ../inlong-tubemq/tubemq-docker/tubemq-manager/ --build-arg TUBEMQ_MANAGER_TARBALL=${TUBEMQ_MANAGER_TARBALL}
docker build -f ../inlong-tubemq/tubemq-docker/tubemq-all/ArmDockerfile -t inlong/tubemq-all:${tag} --build-arg TUBEMQ_TARBALL=${TUBEMQ_TARBALL} ../inlong-tubemq/tubemq-docker/tubemq-all/
docker build -t inlong/dashboard:${tag} ../inlong-dashboard/ --build-arg DASHBOARD_FILE=${DASHBOARD_FILE}
docker build -t inlong/agent:${tag} ../inlong-agent/agent-docker/ --build-arg AGENT_TARBALL=${AGENT_TARBALL}
docker build -f ../inlong-tubemq/tubemq-docker/tubemq-cpp/ArmDockerfile -t inlong/tubemq-cpp:${tag} ../inlong-tubemq/tubemq-docker/tubemq-cpp/
docker build -f ../inlong-tubemq/tubemq-docker/tubemq-build/ArmDockerfile -t inlong/tubemq-build:${tag} ../inlong-tubemq/tubemq-docker/tubemq-build/

docker tag inlong/manager:${tag} inlong/manager:latest
docker tag inlong/dataproxy:${tag} inlong/dataproxy:latest
docker tag inlong/audit:${tag} inlong/audit:latest
docker tag inlong/tubemq-manager:${tag} inlong/tubemq-manager:latest
docker tag inlong/tubemq-all:${tag} inlong/tubemq-all:latest
docker tag inlong/dashboard:${tag} inlong/dashboard:latest
docker tag inlong/agent:${tag} inlong/agent:latest
docker tag inlong/tubemq-cpp:${tag} inlong/tubemq-cpp:latest
docker tag inlong/tubemq-build:${tag} inlong/tubemq-build:latest
