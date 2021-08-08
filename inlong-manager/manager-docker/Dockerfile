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
FROM openjdk:8-jdk-alpine
EXPOSE 8083

# profile and env virables
ENV ACTIVE_PROFILE=dev
# Note: '/' and '&' symbles need to be escaped with the '\'
ENV JDBC_URL='jdbc:mysql:\/\/127.0.0.1:3306\/apache_inlong_manager?useSSL=false\&allowPublicKeyRetrieval=true\&characterEncoding=UTF-8\&nullCatalogMeansCurrent=true\&serverTimezone=GMT%2b8'
ENV USERNAME=root
ENV PASSWORD=123456
ENV TUBE_MANAGER=http://127.0.0.1:8081
ENV TUBE_MASTER=127.0.0.1:8000,127.0.0.1:8010
ENV TUBE_CLUSTER_ID=1
ENV ZK_URL=127.0.0.1:2181
ENV ZK_ROOT=inlong_hive
ENV SORT_APP_NAME=inlong_hive

WORKDIR /opt/inlong-manager

# add tarball from manager output
ARG MANAGER_TARBALL
ADD ${MANAGER_TARBALL} /opt/inlong-manager
ADD manager-docker.sh bin/
RUN chmod +x bin/manager-docker.sh
CMD ["bin/manager-docker.sh"]
