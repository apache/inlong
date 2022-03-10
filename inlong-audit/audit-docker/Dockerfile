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
# add tarball from target output
ARG AUDIT_TARBALL
ADD ${AUDIT_TARBALL} /opt/inlong-audit
ENV JDBC_URL='jdbc:mysql:\/\/127.0.0.1:3306\/apache_inlong_audit?useSSL=false\&allowPublicKeyRetrieval=true\&characterEncoding=UTF-8\&nullCatalogMeansCurrent=true\&serverTimezone=GMT%2b8'
ENV USERNAME=root
ENV PASSWORD=inlong
WORKDIR /opt/inlong-audit
ADD audit-docker.sh bin/
RUN chmod +x bin/audit-docker.sh
CMD ["bin/audit-docker.sh"]
