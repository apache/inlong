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

basePath=$(
  cd $(dirname $0)
  cd ..
  pwd
)
echo $basePath
source $basePath/conf/inlong.conf

inlongAgent() {
  echo "Replace agent configuration parameters"
  cd $basePath/inlong-agent/conf
  sed -i 's/agent.local.ip=.*/'''agent.local.ip=${agent_local_ip}'''/g' agent.properties
  sed -i 's/agent.manager.vip.http.host=.*/'''agent.manager.vip.http.host=${manager_server_hostname}'''/g' agent.properties
  sed -i 's/agent.manager.vip.http.port=.*/'''agent.manager.vip.http.port=${manager_server_port}'''/g' agent.properties
  sed -i 's/audit.proxys=.*/'''audit.proxys=${audit_proxys_ip}:10081'''/g' agent.properties
  sed -i 's/agent.prometheus.enable=.*/'''agent.prometheus.enable=${agent_prometheus_enable}'''/g' agent.properties
  sed -i 's/agent.audit.enable=.*/'''agent.audit.enable=${agent_audit_enable}'''/g' agent.properties
}

inlongAudit() {
  if [ $source_type == "pulsar" ]; then
    echo "Replace audit configuration parameters"
    cd $basePath/inlong-audit/conf
    sed -i 's#pulsar://.*#'''${pulsar_serviceUrl}'''#g' audit-proxy.conf
    sed -i 's#pulsar://.*#'''${pulsar_serviceUrl}'''#g' application.properties
    sed -i 's#jdbc:mysql://.*apache_inlong_audit#'''jdbc:mysql://${spring_datasource_hostname}:${spring_datasource_port}/apache_inlong_audit'''#g' application.properties
    sed -i 's/spring.datasource.druid.username=.*/'''spring.datasource.druid.username=${spring_datasource_username}'''/g' application.properties
    sed -i 's/spring.datasource.druid.password=.*/'''spring.datasource.druid.password=${spring_datasource_password}'''/g' application.properties
  fi
}

inlongDataProxy() {
  echo "Replace dataproxy configuration parameters"
  cd $basePath/inlong-dataproxy/conf
  if [ $source_type == "pulsar" ]; then
    if [ -f "flume-mulit-pulsar-tcp-example.conf" ]; then
      mv flume.conf flume-tubemq.conf
      mv flume-mulit-pulsar-tcp-example.conf flume.conf
    fi
    sed -i 's#pulsar://.*#'''${pulsar_serviceUrl}'''#g' flume.conf
  else
    if [ -f "flume-tubemq.conf" ]; then
      mv flume.conf flume-mulit-pulsar-tcp-example.conf
      mv flume-tubemq.conf flume.conf
    fi
    sed -i 's/master-host-port-list.*/'''master-host-port-list=${tubemqMaster_hostName}:${tubemqMaster_port}'''/g' flume.conf
  fi
  sed -i 's/manager_hosts=.*/'''manager_hosts=${manager_server_hostname}:${manager_server_port}'''/g' common.properties
  sed -i 's/audit.proxys=.*/'''audit.proxys=${audit_proxys_ip}:10081'''/g' common.properties
}

tubeMaster() {
  echo "Replace tubemq server configuration parameters"
  cd $basePath/inlong-tubemq-server/conf
  echo "Replace master.ini configuration"
  sed -i 's/hostName=.*/'''hostName=${tubemqMaster_hostName}'''/g' master.ini
  sed -i 's/port=.*/'''port=${tubemqMaster_port}'''/g' master.ini
  sed -i 's/webPort=.*/'''webPort=${tubemqMaster_webPort}'''/g' master.ini
  sed -i 's#metaDataPath=.*#'''metaDataPath=${metaDataPath}'''#g' master.ini
  sed -i 's/;metaDataPath/metaDataPath/g' master.ini
  sed -i 's/confModAuthToken=.*/'''confModAuthToken=${confModAuthToken}'''/g' master.ini
  sed -i 's/zkServerAddr=.*/'''zkServerAddr=${zkServerAddr}'''/g' master.ini
  sed -i 's/repHelperHost=.*/'''repHelperHost=${tubemqMaster_hostName}:9001'''/g' master.ini
  sed -i 's/;repHelperHost/'''repHelperHost'''/g' master.ini
}

tubeBroker() {
  echo "Replace broker.ini configuration"
  cd $basePath/inlong-tubemq-server/conf
  sed -i 's/brokerId=.*/'''brokerId=${brokerId}'''/g' broker.ini
  sed -i 's/hostName=.*/'''hostName=${tubemqBroker_hostName}'''/g' broker.ini
  sed -i 's/port=.*/'''port=${tubemqBroker_port}'''/g' broker.ini
  sed -i 's/webPort=.*/'''webPort=${tubemqBroker_webPort}'''/g' broker.ini
  sed -i 's/masterAddressList=.*/'''masterAddressList=${tubemqMaster_hostName}:${tubemqMaster_port}'''/g' broker.ini
  sed -i 's#primaryPath=.*#'''primaryPath=${primaryPath}'''#g' broker.ini
  sed -i 's/zkServerAddr=.*/'''zkServerAddr=${zkServerAddr}'''/g' broker.ini
}

tubeManager() {
  echo "Replace tubemq manager configuration"
  cd $basePath/inlong-tubemq-manager/conf
  sed -i 's#jdbc:mysql://.*tubemanager#'''jdbc:mysql://${spring_datasource_hostname}:${spring_datasource_port}/tubemanager'''#g' application.properties
  sed -i 's/spring.datasource.username=.*/'''spring.datasource.username=${spring_datasource_username}'''/g' application.properties
  sed -i 's/spring.datasource.password=.*/'''spring.datasource.password=${spring_datasource_password}'''/g' application.properties
  sed -i 's/#.*spring.datasource/spring.datasource/g' application.properties
  sed -i 's/server.port=.*/'''server.port=${TUBE_MANAGER_PORT}'''/g' application.properties
}

inlongManager() {
  echo "Replace inlong manager configuration"
  cd $basePath/inlong-manager/conf
  sed -i 's/spring.profiles.active=.*/'''spring.profiles.active=${spring_profiles_active}'''/g' application.properties
  sed -i 's/server.port=.*/'''server.port=${manager_server_port}'''/g' application.properties
  sed -i 's#jdbc:mysql://.*apache_inlong_manager#'''jdbc:mysql://${spring_datasource_hostname}:${spring_datasource_port}/apache_inlong_manager'''#g' application-${spring_profiles_active}.properties
  sed -i 's/spring.datasource.druid.username=.*/'''spring.datasource.druid.username=${spring_datasource_username}'''/g' application-${spring_profiles_active}.properties
  sed -i 's/spring.datasource.druid.password=.*/'''spring.datasource.druid.password=${spring_datasource_password}'''/g' application-${spring_profiles_active}.properties
  if [ $source_type == "tubemq" ]; then
    sed -i 's#cluster.tube.manager=.*#'''cluster.tube.manager=http://${TUBE_MANAGER_IP}:${TUBE_MANAGER_PORT}'''#g' application-${spring_profiles_active}.properties
    sed -i 's#cluster.tube.master=.*#'''cluster.tube.master=${tubemqMaster_hostName}:${tubemqMaster_port}'''#g' application-${spring_profiles_active}.properties
  else
    sed -i 's#pulsar.adminUrl=.*#'''pulsar.adminUrl=${pulsar_adminUrl}'''#g' application-${spring_profiles_active}.properties
    sed -i 's#pulsar.serviceUrl=.*#'''pulsar.serviceUrl=${pulsar_serviceUrl}'''#g' application-${spring_profiles_active}.properties
    sed -i 's/pulsar.defaultTenant=.*/'''pulsar.defaultTenant=${pulsar_defaultTenant}'''/g' application-${spring_profiles_active}.properties
  fi
  sed -i 's/cluster.zk.url=.*/'''cluster.zk.url=${zkServerAddr}'''/g' application-${spring_profiles_active}.properties
  sed -i 's/cluster.zk.root=.*/'''cluster.zk.root=${cluster_zk_root}'''/g' application-${spring_profiles_active}.properties
  sed -i 's/sort.appName=.*/'''sort.appName=${sort_appName}'''/g' application-${spring_profiles_active}.properties
}

if [ $# -eq 0 ]; then
  inlongAgent
  inlongAudit
  inlongDataProxy
  tubeMaster
  tubeBroker
  tubeManager
  inlongManager
else
  $1
fi
