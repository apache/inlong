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
FROM openjdk:8-jdk
EXPOSE 8083

# profile and env virables
ENV ACTIVE_PROFILE=prod
ENV JDBC_URL=127.0.0.1:3306
ENV USERNAME=root
ENV PASSWORD=inlong
ENV TUBE_MANAGER=http://127.0.0.1:8089
ENV TUBE_MASTER=master1:8715,master2:8715
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
