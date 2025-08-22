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
BASE_DIR=$(cd "$(dirname "$0")"/../../;pwd)
installerConfigFile=$BASE_DIR/agent-installer/conf/installer.properties
agentConfigFile=$BASE_DIR/inlong-agent/conf/agent.properties

managerAddr=$(cat $installerConfigFile|grep -i 'agent.manager.addr'|awk -F = '{print $2}')
localIp=$(cat $installerConfigFile|grep -i 'agent.local.ip'|awk -F = '{print $2}')
clusterTag=$(cat $installerConfigFile|grep -i 'agent.cluster.tag'|awk -F = '{print $2}')
clusterName=$(cat $installerConfigFile|grep -i 'agent.cluster.name'|awk -F = '{print $2}')
tdwSecurityUrl=$(cat $installerConfigFile|grep -i 'tdw.security.url'|awk -F = '{print $2}')
auditFlag=$(cat $installerConfigFile|grep -i 'audit.enable'|awk -F = '{print $2}')

if [ ${#managerAddr} -gt 0 ]; then
  sed -i "/agent.manager.addr=*/c\agent.manager.addr=$managerAddr" $agentConfigFile
else
  echo "manager addr empty"
fi

if [ ${#localIp} -gt 0 ]; then
  sed -i "/agent.local.ip=*/c\agent.local.ip=$localIp" $agentConfigFile
else
  echo "agent local ip empty"
fi

if [ ${#clusterTag} -gt 0 ]; then
   sed -i "/agent.cluster.tag=*/c\agent.cluster.tag=$clusterTag" $agentConfigFile
else
  echo "cluster tag empty"
fi

if [ ${#clusterName} -gt 0 ]; then
   sed -i "/agent.cluster.name=*/c\agent.cluster.name=$clusterName" $agentConfigFile
else
   echo "cluster name empty"
fi

if [ ${#tdwSecurityUrl} -gt 0 ]; then
  sed -i "/tdw.security.url=*/c\tdw.security.url=$tdwSecurityUrl" $BASE_DIR/inlong-agent/bin/agent-env.sh
else
  echo "tdw security url empty"
fi

if [ ${#auditFlag} -gt 0 ]; then
  sed -i "/audit.enable=*/c\audit.enable=$auditFlag" $agentConfigFile
else
  echo "audit flag empty"
fi
