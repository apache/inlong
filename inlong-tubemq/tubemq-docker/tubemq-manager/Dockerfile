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
# add tarball from target output
ARG TUBEMQ_MANAGER_TARBALL
ADD ${TUBEMQ_MANAGER_TARBALL} /opt/tubemq-manager
EXPOSE 8089
ENV MYSQL_HOST=127.0.0.1
ENV MYSQL_PORT=3306
ENV MYSQL_USER=root
ENV MYSQL_PASSWD=inlong
WORKDIR /opt/tubemq-manager
ADD manager-docker.sh bin/
RUN chmod +x bin/manager-docker.sh
CMD ["bin/manager-docker.sh"]
