#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

file_path=$(
  cd "$(dirname "$0")"/../
  pwd
)

if [ -f "${ACTIVE_PROFILE}" ]; then
  "${ACTIVE_PROFILE}" = dev
fi

conf_file="${file_path}"/conf/application-"${ACTIVE_PROFILE}".properties

# replace the configuration
sed -i "s/spring.profiles.active=.*$/spring.profiles.active=${ACTIVE_PROFILE}/g" "${file_path}"/conf/application.properties
sed -i "s/127.0.0.1:3306/${JDBC_URL}/g" "${conf_file}"
sed -i "s/datasource.druid.username=.*$/datasource.druid.username=${USERNAME}/g" "${conf_file}"
sed -i "s/datasource.druid.password=.*$/datasource.druid.password=${PASSWORD}/g" "${conf_file}"

sed -i "s/cluster.zk.url=.*$/cluster.zk.url=${ZK_URL}/g" "${conf_file}"

# startup the application
JAVA_OPTS="-Dspring.profiles.active=${ACTIVE_PROFILE}"

# get plugins from remote address.
if [[ "${PLUGINS_URL}" =~ ^http* ]]; then
    # remove the default plugins
    rm -rf plugins
    # get the third party plugins
    wget ${PLUGINS_URL} -O plugins.tar.gz
    tar -zxvf plugins.tar.gz -C "${file_path}"/
    rm plugins.tar.gz
fi

sh "${file_path}"/bin/startup.sh "${JAVA_OPTS}"
sleep 3
# keep alive
tail -F "${file_path}"/log/manager-web.log
