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

file_path=$(cd "$(dirname "$0")"/../;pwd)

#SQL file
sql_mysql_file="${file_path}"/sql/apache_inlong_audit_mysql.sql

# proxy config
proxy_conf_file=${file_path}/conf/audit-proxy-${MQ_TYPE}.conf

# store config
store_conf_file=${file_path}/conf/application.properties

# audit-service config
service_conf_file=${file_path}/conf/audit-service.properties

# replace the configuration for audit-proxy
sed -i "s/manager.hosts=.*$/manager.hosts=${MANAGER_OPENAPI_IP}:${MANAGER_OPENAPI_PORT}/g" "${store_conf_file}"
sed -i "s/default.mq.cluster.tag=.*$/default.mq.cluster.tag=${CLUSTER_TAG}/g" "${store_conf_file}"
if [ "${MQ_TYPE}" = "pulsar" ]; then
  sed -i "s/audit.config.proxy.type=.*$/audit.config.proxy.type=pulsar"/g "${store_conf_file}"
  sed -i "s/audit.pulsar.topic = .*$/audit.pulsar.topic = ${PULSAR_AUDIT_TOPIC}/g" "${store_conf_file}"
  sed -i "s/agent1.sinks.pulsar-sink-msg1.topic = .*$/agent1.sinks.pulsar-sink-msg1.topic = ${PULSAR_AUDIT_TOPIC}/g" "${proxy_conf_file}"
  sed -i "s/agent1.sinks.pulsar-sink-msg2.topic = .*$/agent1.sinks.pulsar-sink-msg2.topic = ${PULSAR_AUDIT_TOPIC}/g" "${proxy_conf_file}"
fi
if [ "${MQ_TYPE}" = "kafka" ]; then
  sed -i "s/audit.config.proxy.type=.*$/audit.config.proxy.type=kafka"/g "${store_conf_file}"
fi
if [ "${MQ_TYPE}" = "tubemq" ]; then
  sed -i "s/audit.config.proxy.type=.*$/audit.config.proxy.type=tube"/g "${store_conf_file}"
  sed -i "s/audit.tube.topic = .*$/audit.tube.topic = ${TUBE_AUDIT_TOPIC}/g" "${store_conf_file}"
  sed -i "s/agent1.sinks.tube-sink-msg1.topic = .*$/agent1.sinks.tube-sink-msg1.topic = ${TUBE_AUDIT_TOPIC}/g" "${proxy_conf_file}"
  sed -i "s/agent1.sinks.tube-sink-msg2.topic = .*$/agent1.sinks.tube-sink-msg2.topic = ${TUBE_AUDIT_TOPIC}/g" "${proxy_conf_file}"
fi

# replace the audit db name for audit sql file
sed -i "s/apache_inlong_audit/${AUDIT_DBNAME}/g" "${sql_mysql_file}"

# replace the configuration for audit-store
sed -i "s/127.0.0.1:3306\/apache_inlong_audit/${AUDIT_JDBC_URL}\/${AUDIT_DBNAME}/g" "${store_conf_file}"
sed -i "s/audit.store.jdbc.username=.*$/audit.store.jdbc.username=${AUDIT_JDBC_USERNAME}/g" "${store_conf_file}"
sed -i "s/audit.store.jdbc.password=.*$/audit.store.jdbc.password=${AUDIT_JDBC_PASSWORD}/g" "${store_conf_file}"

# replace the configuration for audit-service
sed -i "s/mysql.jdbc.url=.*$/mysql.jdbc.url=jdbc:mysql:\/\/${AUDIT_JDBC_URL}\/${AUDIT_DBNAME}/g" "${service_conf_file}"
sed -i "s/mysql.jdbc.username=.*$/mysql.jdbc.username=${AUDIT_JDBC_USERNAME}/g" "${service_conf_file}"
sed -i "s/mysql.jdbc.password=.*$/mysql.jdbc.password=${AUDIT_JDBC_PASSWORD}/g" "${service_conf_file}"
sed -i "s/audit.proxy.address.agent=.*$/audit.proxy.address.agent = ${AUDIT_PROXY_ADDRESS}/g" "${service_conf_file}"
sed -i "s/audit.proxy.address.dataproxy=.*$/audit.proxy.address.dataproxy = ${AUDIT_PROXY_ADDRESS}/g" "${service_conf_file}"
sed -i "s/audit.proxy.address.sort=.*$/audit.proxy.address.sort = ${AUDIT_PROXY_ADDRESS}/g" "${service_conf_file}"

# Whether the database table exists. If it does not exist, initialize the database and skip if it exists.
if [[ "${AUDIT_JDBC_URL}" =~ (.+):([0-9]+) ]]; then
  datasource_hostname=${BASH_REMATCH[1]}
  datasource_port=${BASH_REMATCH[2]}

  select_db_sql="SELECT COUNT(*) FROM information_schema.TABLES WHERE table_schema = 'apache_inlong_audit'"
  inlong_audit_count=$(mysql -h${datasource_hostname} -P${datasource_port} -u${AUDIT_JDBC_USERNAME} -p${AUDIT_JDBC_PASSWORD} -e "${select_db_sql}")
  inlong_num=$(echo "$inlong_audit_count" | tr -cd "[0-9]")
  if [ "${inlong_num}" = 0 ]; then
    mysql -h${datasource_hostname} -P${datasource_port} -u${AUDIT_JDBC_USERNAME} -p${AUDIT_JDBC_PASSWORD} < sql/apache_inlong_audit_mysql.sql
  fi
fi

# start proxy
cd "${file_path}/"
if [ "${START_MODE}" = "all" ] || [ "${START_MODE}" = "proxy" ]; then
  if [ "${MQ_TYPE}" = "pulsar" ]; then
    bash +x ./bin/proxy-start.sh pulsar
  fi
  if [ "${MQ_TYPE}" = "kafka" ]; then
    bash +x ./bin/proxy-start.sh kafka
  fi
  if [ "${MQ_TYPE}" = "tubemq" ]; then
    bash +x ./bin/proxy-start.sh tubemq
  fi
fi

# start store
if [ "${START_MODE}" = "all" ] || [ "${START_MODE}" = "store" ]; then
  bash +x ./bin/store-start.sh
fi

# start service
if [ "${START_MODE}" = "all" ] || [ "${START_MODE}" = "service" ]; then
  bash +x ./bin/service-start.sh
fi

sleep 3
# keep alive
tail -F ./logs/info.log
