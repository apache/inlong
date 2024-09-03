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
# Initialize the configuration files of inlong components

INLONG_HOME=$(
  cd $(dirname $0)
  cd ..
  pwd
)
source $INLONG_HOME/conf/inlong.conf

SED_COMMAND=${SED_COMMAND:-"sed -i"}

function detect_sed_command() {
  case "$(uname -s)" in
        Linux)
          os_name=linux
          ;;
        Darwin)
          os_name=darwin
          ;;
        *)
          echo "Unsupported OS, must be Linux or Mac OS X." >&2
          exit 1
          ;;
      esac
  
  if [ "${os_name}" == "darwin" ]; then
      SED_COMMAND="sed -i '' "
  else
      SED_COMMAND="sed -i "
  fi
}

detect_sed_command

init_inlong_agent() {
  echo "Init agent configuration parameters"
  cd $INLONG_HOME/inlong-agent/conf
  $SED_COMMAND "s|agent.local.ip=.*|agent.local.ip=${local_ip}|g" agent.properties
  $SED_COMMAND "s|agent.manager.addr=.*|agent.manager.addr=http://${manager_server_hostname}:${manager_server_port}|g" agent.properties
}

init_inlong_audit() {
  echo "Init audit configuration parameters"
  cd $INLONG_HOME/inlong-audit/conf
  # configure Audit Store
  $SED_COMMAND 's#jdbc:mysql://.*apache_inlong_audit#'''jdbc:mysql://${spring_datasource_hostname}:${spring_datasource_port}/apache_inlong_audit'''#g' application.properties
  $SED_COMMAND 's/jdbc.username=.*/'''jdbc.username=${spring_datasource_username}'''/g' application.properties
  $SED_COMMAND 's/jdbc.password=.*/'''jdbc.password=${spring_datasource_password}'''/g' application.properties
  if [ $mq_type == "pulsar" ]; then
    $SED_COMMAND 's/audit.config.proxy.type=.*/'''audit.config.proxy.type=pulsar'''/g' application.properties
  fi
  if [ $mq_type == "kafka" ]; then
    $SED_COMMAND 's/audit.config.proxy.type=.*/'''audit.config.proxy.type=kafka'''/g' application.properties
  fi
  if [ $mq_type == "tubemq" ]; then
    $SED_COMMAND 's/audit.config.proxy.type=.*/'''audit.config.proxy.type=tube'''/g' application.properties
  fi
  # configure Audit Service
  $SED_COMMAND 's#jdbc:mysql://.*apache_inlong_audit#'''jdbc:mysql://${spring_datasource_hostname}:${spring_datasource_port}/apache_inlong_audit'''#g' audit-service.properties
  $SED_COMMAND 's/mysql.username=.*/'''mysql.username=${spring_datasource_username}'''/g' audit-service.properties
  $SED_COMMAND 's/mysql.password=.*/'''mysql.password=${spring_datasource_password}'''/g' audit-service.properties
  $SED_COMMAND 's/audit.proxy.address.agent=.*/'''audit.proxy.address.agent=${audit_service_ip}:${audit_proxy_port}'''/g' audit-service.properties
  $SED_COMMAND 's/audit.proxy.address.dataproxy=.*/'''audit.proxy.address.dataproxy=${audit_service_ip}:${audit_proxy_port}'''/g' audit-service.properties
  $SED_COMMAND 's/audit.proxy.address.sort=.*/'''audit.proxy.address.sort=${audit_service_ip}:${audit_proxy_port}'''/g' audit-service.properties
}

init_inlong_dataproxy() {
  echo "Init dataproxy configuration parameters"
  cd $INLONG_HOME/inlong-dataproxy/conf
  $SED_COMMAND 's/manager.hosts=.*/'''manager.hosts=${manager_server_hostname}:${manager_server_port}'''/g' common.properties
  $SED_COMMAND "s/audit.proxys.discovery.manager.enable=.*$/audit.proxys.discovery.manager.enable=true/g" common.properties
}

init_inlong_manager() {
  echo "Init inlong manager configuration"
  cd $INLONG_HOME/inlong-manager/conf
  $SED_COMMAND 's/spring.profiles.active=.*/'''spring.profiles.active=${spring_profiles_active}'''/g' application.properties
  $SED_COMMAND 's/server.port=.*/'''server.port=${manager_server_port}'''/g' application.properties
  if [ $spring_profiles_active == "dev" ]; then
    $SED_COMMAND 's#jdbc:mysql://.*apache_inlong_manager#'''jdbc:mysql://${spring_datasource_hostname}:${spring_datasource_port}/apache_inlong_manager'''#g' application-dev.properties
    $SED_COMMAND 's/spring.datasource.druid.username=.*/'''spring.datasource.druid.username=${spring_datasource_username}'''/g' application-dev.properties
    $SED_COMMAND 's/spring.datasource.druid.password=.*/'''spring.datasource.druid.password=${spring_datasource_password}'''/g' application-dev.properties
    $SED_COMMAND 's/audit.query.url=.*/'''audit.query.url=${audit_service_ip}:${audit_service_port}'''/g' application-dev.properties
  fi
  if [ $spring_profiles_active == "prod" ]; then
    $SED_COMMAND 's#jdbc:mysql://.*apache_inlong_manager#'''jdbc:mysql://${spring_datasource_hostname}:${spring_datasource_port}/apache_inlong_manager'''#g' application-prod.properties
    $SED_COMMAND 's/spring.datasource.druid.username=.*/'''spring.datasource.druid.username=${spring_datasource_username}'''/g' application-prod.properties
    $SED_COMMAND 's/spring.datasource.druid.password=.*/'''spring.datasource.druid.password=${spring_datasource_password}'''/g' application-prod.properties
    $SED_COMMAND 's/audit.query.url=.*/'''audit.query.url=${audit_service_ip}:${audit_service_port}'''/g' application-prod.properties
  fi
  echo "Init inlong manager flink plugin configuration"
  cd $INLONG_HOME/inlong-manager/plugins
  $SED_COMMAND 's/flink.rest.address=.*/'''flink.rest.address=${flink_rest_address}'''/g' flink-sort-plugin.properties
  $SED_COMMAND 's/flink.rest.port=.*/'''flink.rest.port=${flink_rest_port}'''/g' flink-sort-plugin.properties
}

if [ $# -eq 0 ]; then
  init_inlong_agent
  init_inlong_audit
  init_inlong_dataproxy
  init_inlong_manager
else
  init_inlong_$1
fi
