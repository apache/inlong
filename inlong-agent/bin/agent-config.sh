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
installerEnv=$BASE_DIR/agent-installer/bin/installer-env.sh
agentEnv=$BASE_DIR/inlong-agent/bin/agent-env.sh

managerAddr=$(cat $installerConfigFile|grep -i 'agent.manager.addr'|awk -F = '{print $2}')
localIp=$(cat $installerConfigFile|grep -i 'agent.local.ip'|awk -F = '{print $2}')
clusterTag=$(cat $installerConfigFile|grep -i 'agent.cluster.tag'|awk -F = '{print $2}')
clusterName=$(cat $installerConfigFile|grep -i 'agent.cluster.name'|awk -F = '{print $2}')
secretId=$(cat $installerConfigFile|grep -i 'agent.manager.auth.secretId'|awk -F = '{print $2}')
secretKey=$(cat $installerConfigFile|grep -i 'agent.manager.auth.secretKey'|awk -F = '{print $2}')
auditFlag=$(cat $installerConfigFile|grep -i 'audit.enable'|awk -F = '{print $2}')
auditProxy=$(cat $installerConfigFile|grep -i 'audit.proxys'|awk -F = '{print $2}')

tdwSecurityUrl=$(cat $installerEnv|grep -i 'TDW_SECURITY_URL'|awk -F = '{print $2}')

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

if [ ${#secretId} -gt 0 ]; then
  sed -i "/agent.manager.auth.secretId=*/c\agent.manager.auth.secretId=$secretId" $agentConfigFile
else
  echo "secretId empty"
fi

if [ ${#secretKey} -gt 0 ]; then
  sed -i "/agent.manager.auth.secretKey=*/c\agent.manager.auth.secretKey=$secretKey" $agentConfigFile
else
  echo "secretKey empty"
fi

if [ ${#auditFlag} -gt 0 ]; then
  sed -i "/audit.enable=*/c\audit.enable=$auditFlag" $agentConfigFile
else
  echo "audit flag empty"
fi

if [ ${#auditProxy} -gt 0 ]; then
  sed -i "/audit.proxys.null=*/c\audit.proxys=$auditProxy" $agentConfigFile
else
  echo "audit proxy empty"
fi

if [ ${#tdwSecurityUrl} -gt 0 ]; then
  sed -i "/export TDW_SECURITY_URL_NULL/c\export TDW_SECURITY_URL=$tdwSecurityUrl" $agentEnv
else
  sed -i "/export TDW_SECURITY_URL_NULL/c\export TDW_SECURITY_URL_NULL" $agentEnv
  echo "tdw security url empty"
fi
