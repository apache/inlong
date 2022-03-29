#!/bin/sh
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

file_path=$(cd "$(dirname "$0")"/../;pwd)
# store config
store_conf_file=${file_path}/conf/application.properties
# proxy config
proxy_conf_file=${file_path}/conf/audit-proxy-${MQ_TYPE}.conf

# replace the configuration for audit store
sed -i "s/spring.datasource.druid.url=.*$/spring.datasource.druid.url=${JDBC_URL}/g" "${store_conf_file}"
sed -i "s/spring.datasource.druid.username=.*$/spring.datasource.druid.username=${USERNAME}/g" "${store_conf_file}"
sed -i "s/spring.datasource.druid.password=.*$/spring.datasource.druid.password=${PASSWORD}/g" "${store_conf_file}"

# replace the configuration for audit proxy
if [ "${MQ_TYPE}" == "pulsar" ]; then
  sed -i "s/agent1.sinks.pulsar-sink-msg1.pulsar_server_url = .*$/agent1.sinks.pulsar-sink-msg1.pulsar_server_url = pulsar:\/\/${PULSAR_BROKER_LIST}/g" "${proxy_conf_file}"
  sed -i "s/agent1.sinks.pulsar-sink-msg2.pulsar_server_url = .*$/agent1.sinks.pulsar-sink-msg2.pulsar_server_url = pulsar:\/\/${PULSAR_BROKER_LIST}/g" "${proxy_conf_file}"
fi
if [ "${MQ_TYPE}" == "tube" ]; then
  sed -i "s/agent1.sinks.tube-sink-msg1.master-host-port-list = .*$/agent1.sinks.tube-sink-msg1.master-host-port-list = ${TUBE_MASTER_LIST}/g" "${proxy_conf_file}"
  sed -i "s/agent1.sinks.tube-sink-msg2.master-host-port-list = .*$/agent1.sinks.tube-sink-msg2.master-host-port-list = ${TUBE_MASTER_LIST}/g" "${proxy_conf_file}"
fi

# start
bash +x ${file_path}/bin/proxy-start.sh
bash +x ${file_path}/bin/store-start.sh
sleep 3
# keep alive
tail -F ${file_path}/logs/audit-*
