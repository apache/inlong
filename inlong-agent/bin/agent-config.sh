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

managerAddr=$(cat ~/inlong/agent-installer/conf/installer.properties|grep -i 'manager.addr'|awk -F = '{print $2}')
localIp=$(cat ~/inlong/agent-installer/conf/installer.properties|grep -i 'local.ip'|awk -F = '{print $2}')
clusterTag=$(cat ~/inlong/agent-installer/conf/installer.properties|grep -i 'agent.cluster.tag'|awk -F = '{print $2}')
clusterName=$(cat ~/inlong/agent-installer/conf/installer.properties|grep -i 'agent.cluster.name'|awk -F = '{print $2}')

if [ ${#managerAddr} -gt 0 ]; then
  sed -i "/manager.addr/s#default#$managerAddr#g" ~/inlong/inlong-agent/conf/agent.properties
else
  echo "manager addr empty"
fi

if [ ${#localIp} -gt 0 ]; then
  sed -i "/local.ip/s#default#$localIp#g" ~/inlong/inlong-agent/conf/agent.properties
else
  echo "local ip empty"
fi

if [ ${#clusterTag} -gt 0 ]; then
   sed -i "/agent.cluster.tag/s#default#$clusterTag#g" ~/inlong/inlong-agent/conf/agent.properties
else
  echo "cluster tag empty"
fi

if [ ${#clusterName} -gt 0 ]; then
   sed -i "/agent.cluster.name/s#default#$clusterName#g" ~/inlong/inlong-agent/conf/agent.properties
else
   echo "cluster name empty"
fi
