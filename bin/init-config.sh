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
echo $INLONG_HOME
source $INLONG_HOME/conf/inlong.conf

init_inlong_agent() {
  echo "Init agent configuration parameters"
  cd $INLONG_HOME/inlong-agent/conf
  sed -i 's/agent.local.ip=.*/'''agent.local.ip=${agent_local_ip}'''/g' agent.properties
  sed -i 's/agent.manager.vip.http.host=.*/'''agent.manager.vip.http.host=${manager_server_hostname}'''/g' agent.properties
  sed -i 's/agent.manager.vip.http.port=.*/'''agent.manager.vip.http.port=${manager_server_port}'''/g' agent.properties
  sed -i 's/audit.proxys=.*/'''audit.proxys=${audit_proxys_ip}:10081'''/g' agent.properties
  sed -i 's/agent.prometheus.enable=.*/'''agent.prometheus.enable=${agent_prometheus_enable}'''/g' agent.properties
  sed -i 's/agent.audit.enable=.*/'''agent.audit.enable=${agent_audit_enable}'''/g' agent.properties
}

init_inlong_audit() {
  if [ $source_type == "pulsar" ]; then
    echo "Init audit configuration parameters"
    cd $INLONG_HOME/inlong-audit/conf
    sed -i 's#pulsar://.*#'''${pulsar_service_url}'''#g' audit-proxy.conf
    sed -i 's#pulsar://.*#'''${pulsar_service_url}'''#g' application.properties
    sed -i 's#jdbc:mysql://.*apache_inlong_audit#'''jdbc:mysql://${spring_datasource_hostname}:${spring_datasource_port}/apache_inlong_audit'''#g' application.properties
    sed -i 's/spring.datasource.druid.username=.*/'''spring.datasource.druid.username=${spring_datasource_username}'''/g' application.properties
    sed -i 's/spring.datasource.druid.password=.*/'''spring.datasource.druid.password=${spring_datasource_password}'''/g' application.properties
  fi
}

init_inlong_dataproxy() {
  echo "Init dataproxy configuration parameters"
  cd $INLONG_HOME/inlong-dataproxy/conf
  if [ $source_type == "pulsar" ]; then
    if [ -f "flume-mulit-pulsar-tcp-example.conf" ]; then
      mv flume.conf flume-tubemq.conf
      mv flume-mulit-pulsar-tcp-example.conf flume.conf
    fi
    sed -i 's#pulsar://.*#'''${pulsar_service_url}'''#g' flume.conf
  else
    if [ -f "flume-tubemq.conf" ]; then
      mv flume.conf flume-mulit-pulsar-tcp-example.conf
      mv flume-tubemq.conf flume.conf
    fi
    sed -i 's/master-host-port-list.*/'''master-host-port-list=${tubemaster_hostname}:${tubemaster_port}'''/g' flume.conf
  fi
  sed -i 's/manager_hosts=.*/'''manager_hosts=${manager_server_hostname}:${manager_server_port}'''/g' common.properties
  sed -i 's/audit.proxys=.*/'''audit.proxys=${audit_proxys_ip}:10081'''/g' common.properties
}

init_inlong_tubemaster() {
  echo "Init tubemq server configuration parameters"
  cd $INLONG_HOME/inlong-tubemq-server/conf
  echo "Init tubemq master configuration"
  sed -i 's/hostName=.*/'''hostName=${tubemaster_hostname}'''/g' master.ini
  sed -i 's/port=.*/'''port=${tubemaster_port}'''/g' master.ini
  sed -i 's/webPort=.*/'''webPort=${tubemaster_webport}'''/g' master.ini
  sed -i 's#metaDataPath=.*#'''metaDataPath=${metadata_path}'''#g' master.ini
  sed -i 's/;metaDataPath/metaDataPath/g' master.ini
  sed -i 's/confModAuthToken=.*/'''confModAuthToken=${confmod_authtoken}'''/g' master.ini
  sed -i 's/zkServerAddr=.*/'''zkServerAddr=${zkserver_addr}'''/g' master.ini
  sed -i 's/repHelperHost=.*/'''repHelperHost=${tubemaster_hostname}:9001'''/g' master.ini
  sed -i 's/;repHelperHost/'''repHelperHost'''/g' master.ini
}

init_inlong_tubebroker() {
  echo "Init tubemq broker configuration"
  cd $INLONG_HOME/inlong-tubemq-server/conf
  sed -i 's/brokerId=.*/'''brokerId=${broker_id}'''/g' broker.ini
  sed -i 's/hostName=.*/'''hostName=${tubebroker_hostname}'''/g' broker.ini
  sed -i 's/port=.*/'''port=${tubebroker_port}'''/g' broker.ini
  sed -i 's/webPort=.*/'''webPort=${tubebroker_webport}'''/g' broker.ini
  sed -i 's/masterAddressList=.*/'''masterAddressList=${tubemaster_hostname}:${tubemaster_port}'''/g' broker.ini
  sed -i 's#primaryPath=.*#'''primaryPath=${primary_path}'''#g' broker.ini
  sed -i 's/zkServerAddr=.*/'''zkServerAddr=${zkserver_addr}'''/g' broker.ini
}

init_inlong_tubemanager() {
  echo "Init tubemq manager configuration"
  cd $INLONG_HOME/inlong-tubemq-manager/conf
  sed -i 's#jdbc:mysql://.*tubemanager#'''jdbc:mysql://${spring_datasource_hostname}:${spring_datasource_port}/tubemanager'''#g' application.properties
  sed -i 's/spring.datasource.username=.*/'''spring.datasource.username=${spring_datasource_username}'''/g' application.properties
  sed -i 's/spring.datasource.password=.*/'''spring.datasource.password=${spring_datasource_password}'''/g' application.properties
  sed -i 's/#.*spring.datasource/spring.datasource/g' application.properties
  sed -i 's/server.port=.*/'''server.port=${tube_manager_port}'''/g' application.properties
}

init_inlong_manager() {
  echo "Init inlong manager configuration"
  cd $INLONG_HOME/inlong-manager/conf
  sed -i 's/spring.profiles.active=.*/'''spring.profiles.active=${spring_profiles_active}'''/g' application.properties
  sed -i 's/server.port=.*/'''server.port=${manager_server_port}'''/g' application.properties
  sed -i 's#jdbc:mysql://.*apache_inlong_manager#'''jdbc:mysql://${spring_datasource_hostname}:${spring_datasource_port}/apache_inlong_manager'''#g' application-${spring_profiles_active}.properties
  sed -i 's/spring.datasource.druid.username=.*/'''spring.datasource.druid.username=${spring_datasource_username}'''/g' application-${spring_profiles_active}.properties
  sed -i 's/spring.datasource.druid.password=.*/'''spring.datasource.druid.password=${spring_datasource_password}'''/g' application-${spring_profiles_active}.properties
  if [ $source_type == "tubemq" ]; then
    sed -i 's#cluster.tube.manager=.*#'''cluster.tube.manager=http://${tube_manager_ip}:${tube_manager_port}'''#g' application-${spring_profiles_active}.properties
    sed -i 's#cluster.tube.master=.*#'''cluster.tube.master=${tubemaster_hostname}:${tubemaster_port}'''#g' application-${spring_profiles_active}.properties
  else
    sed -i 's#pulsar.adminUrl=.*#'''pulsar.adminUrl=${pulsar_admin_url}'''#g' application-${spring_profiles_active}.properties
    sed -i 's#pulsar.serviceUrl=.*#'''pulsar.serviceUrl=${pulsar_service_url}'''#g' application-${spring_profiles_active}.properties
    sed -i 's/pulsar.defaultTenant=.*/'''pulsar.defaultTenant=${pulsar_default_tenant}'''/g' application-${spring_profiles_active}.properties
  fi
  sed -i 's/cluster.zk.url=.*/'''cluster.zk.url=${zkserver_addr}'''/g' application-${spring_profiles_active}.properties
  sed -i 's/cluster.zk.root=.*/'''cluster.zk.root=${cluster_zk_root}'''/g' application-${spring_profiles_active}.properties
  sed -i 's/sort.appName=.*/'''sort.appName=${sort_app_name}'''/g' application-${spring_profiles_active}.properties
}

if [ $# -eq 0 ]; then
  init_inlong_agent
  init_inlong_audit
  init_inlong_dataproxy
  init_inlong_tubemaster
  init_inlong_tubebroker
  init_inlong_tubemanager
  init_inlong_manager
else
  init_inlong_$1
fi
