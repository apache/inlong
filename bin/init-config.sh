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

# tubemqMaster Default port
tubemqMaster_port=8715
# inlong  manager Default env
spring_profiles_active=dev

basePath=$(cd `dirname $0`;cd ..; pwd)
echo $basePath
source $basePath/conf/standalone.conf

echo  "Replace agent configuration parameters"
cd $basePath/inlong-agent/conf
sed -i 's/agent.http.enable=.*/'''agent.http.enable=${agent_http_enable}'''/g' agent.properties
sed -i 's/agent.local.ip=.*/'''agent.local.ip=${agent_local_ip}'''/g' agent.properties  
sed -i 's/agent.manager.vip.http.host=.*/'''agent.manager.vip.http.host=${manager_server_hostname}'''/g' agent.properties 
sed -i 's/agent.manager.vip.http.port=.*/'''agent.manager.vip.http.port=${manager_server_port}'''/g' agent.properties 

echo "Replace dataproxy configuration parameters"
cd $basePath/inlong-dataproxy/conf
if [ $source_type == "pulsar" ];then
  if [ -f "flume-mulit-pulsar-demo.conf" ];then
     mv flume.conf flume-tubemq.conf
     mv flume-mulit-pulsar-demo.conf flume.conf
  fi
  sed -i 's#pulsar://.*#'''${pulsar_serviceUrl}'''#g' flume.conf 
  sed -i 's/org.apache.inlong.dataproxy.PulsarSink/org.apache.inlong.dataproxy.sink.PulsarSink/g' flume.conf
else
  if [ -f "flume-tubemq.conf" ];then
	 mv flume.conf flume-mulit-pulsar-demo.conf
	 mv flume-tubemq.conf flume.conf  
  fi
  sed -i 's/master-host-port-list.*/'''master-host-port-list=${tubemqMaster_hostName}:${tubemqMaster_port}'''/g' flume.conf
fi
sed -i 's/manager_hosts=.*/'''manager_hosts=${manager_server_hostname}:${manager_server_port}'''/g' common.properties


cd $basePath/inlong-tubemq-server/conf
if [ $source_type == "tubemq" ];then
  echo "Replace tubemq server configuration parameters(Optional)"
  echo "Replace master.ini configuration"
  
  sed -i 's/hostName=.*/'''hostName=${tubemqMaster_hostName}'''/g' master.ini 
  
  sed -i 's/port=.*/'''port=${tubemqMaster_port}'''/g' master.ini
  if [ -n "$tubemqMaster_webPort" ];then 
     sed -i 's/webPort=.*/'''webPort=${tubemqMaster_webPort}'''/g' master.ini 
  fi
  if [ -n "$confModAuthToken" ];then
     sed -i 's/confModAuthToken=.*/'''confModAuthToken=${confModAuthToken}'''/g' master.ini
  fi
  if [ -n "$metaDataPath" ];then
     sed -i 's#metaDataPath=.*#'''metaDataPath=${metaDataPath}'''#g' master.ini
	 sed -i 's/;metaDataPath/metaDataPath/g' master.ini
  fi
  if [ -n "$zkServerAddr" ];then
     sed -i 's/zkServerAddr=.*/'''zkServerAddr=${zkServerAddr}'''/g' master.ini
  fi
  sed -i 's/repHelperHost=.*/'''repHelperHost=${tubemqMaster_hostName}:9001'''/g' master.ini
  sed -i 's/;repHelperHost/'''repHelperHost'''/g' master.ini
  echo "Replace broker.ini configuration"
  
  if [ -n "$brokerId" ];then
     sed -i 's/brokerId=.*/'''brokerId=${brokerId}'''/g' broker.ini 
  fi
     sed -i 's/hostName=.*/'''hostName=${tubemqBroker_hostName}'''/g' broker.ini 
  if [ -n "$tubemqBroker_port" ];then
     sed -i 's/port=.*/'''port=${tubemqBroker_port}'''/g' broker.ini
  fi
  if [ -n "$tubemqBroker_webPort" ];then
     sed -i 's/webPort=.*/'''webPort=${tubemqBroker_webPort}'''/g' broker.ini
  fi
  
  sed -i 's/masterAddressList=.*/'''masterAddressList=${tubemqMaster_hostName}:${tubemqMaster_port}'''/g' broker.ini
  
  if [ -n "$primaryPath" ];then
     sed -i 's#primaryPath=.*#'''primaryPath=${primaryPath}'''#g' broker.ini
  fi
  if [ -n "$zkServerAddr" ];then
   sed -i 's/zkServerAddr=.*/'''zkServerAddr=${zkServerAddr}'''/g' broker.ini
  fi
fi
 
cd $basePath/inlong-tubemq-manager/conf
if [ $source_type == "tubemq" ];then
  echo "Replace tubemq manager configuration(Optional)"
  sed -i 's#jdbc:mysql://.*tubemanager#'''jdbc:mysql://${spring_datasource_hostname}:${spring_datasource_port}/tubemanager'''#g'  application.properties
  sed -i 's/spring.datasource.username=.*/'''spring.datasource.username=${spring_datasource_username}'''/g' application.properties
  sed -i 's/spring.datasource.password=.*/'''spring.datasource.password=${spring_datasource_password}'''/g' application.properties
  sed -i 's/#.*spring.datasource/spring.datasource/g' application.properties
  sed -i 's/server.port=.*/'''server.port=${TUBE_MANAGER_PORT}'''/g' application.properties
fi


echo "Replace inlong manager configuration"
cd  $basePath/inlong-manager-web/conf

sed -i 's/spring.profiles.active=.*/'''spring.profiles.active=${spring_profiles_active}'''/g' application.properties
sed -i 's/server.port=.*/'''server.port=${manager_server_port}'''/g' application.properties

sed -i 's#jdbc:mysql://.*apache_inlong_manager#'''jdbc:mysql://${spring_datasource_hostname}:${spring_datasource_port}/apache_inlong_manager'''#g'  application-${spring_profiles_active}.properties

sed -i 's/spring.datasource.druid.username=.*/'''spring.datasource.druid.username=${spring_datasource_username}'''/g' application-${spring_profiles_active}.properties
sed -i 's/spring.datasource.druid.password=.*/'''spring.datasource.druid.password=${spring_datasource_password}'''/g' application-${spring_profiles_active}.properties

if [ $source_type == "tubemq" ];then
  sed -i 's#cluster.tube.manager=.*#'''cluster.tube.manager=http://${TUBE_MANAGER_IP}:${TUBE_MANAGER_PORT}'''#g'  application-${spring_profiles_active}.properties
  sed -i 's#cluster.tube.master=.*#'''cluster.tube.master=${tubemqMaster_hostName}:${tubemqMaster_port}'''#g' application-${spring_profiles_active}.properties
else 
  sed -i 's#pulsar.adminUrl=.*#'''pulsar.adminUrl=${pulsar_adminUrl}'''#g'  application-${spring_profiles_active}.properties
  sed -i 's#pulsar.serviceUrl=.*#'''pulsar.serviceUrl=${pulsar_serviceUrl}'''#g' application-${spring_profiles_active}.properties
  if [ -n "$pulsar_defaultTenant" ];then
     sed -i 's/pulsar.defaultTenant=.*/'''pulsar.defaultTenant=${pulsar_defaultTenant}'''/g' application-${spring_profiles_active}.properties
  fi
fi
if [ -n "$zkServerAddr" ];then
   sed -i 's/cluster.zk.url=.*/'''cluster.zk.url=${zkServerAddr}'''/g' application-${spring_profiles_active}.properties
fi
if [ -n "$cluster_zk_root" ];then
   sed -i 's/cluster.zk.root=.*/'''cluster.zk.root=${cluster_zk_root}'''/g' application-${spring_profiles_active}.properties
fi
if [ -n "$sort_appName" ];then
   sed -i 's/sort.appName=.*/'''sort.appName=${sort_appName}'''/g' application-${spring_profiles_active}.properties
fi