#!/bin/bash
#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

cd "$(dirname "$0")"/../
export HOST_IP=`more conf/common.properties |grep "adminTask.host"|awk -F"=" '{print $2}'`
export ADMIN_PORT=`more conf/common.properties |grep "adminTask.port"|awk -F"=" '{print $2}'`
if [ ${HOST_IP} ] && [ ${ADMIN_PORT} ]; then
    curl -X POST -d'[{"headers":{"cmd":"stopService"},"body":"body"}]' "http://${HOST_IP}:${ADMIN_PORT}"
    echo "stop server and sleep."
    sleep 61s
fi

#this program kills the sort
pidInfo=$(ps -ef | grep java |grep org.apache.inlong.sort.standalone.SortStandaloneApplication| grep -v grep | awk '{print $2}')
echo "`date` the pid info is $pidInfo">>$logFile

for pid in $pidInfo;do
	kill $pid
done

sleep 5s

#force kill
pidInfo=$(ps -ef | grep java |grep org.apache.inlong.sort.standalone.SortStandaloneApplication| grep -v grep | awk '{print $2}')
for pid in $pidInfo;do
	kill -9 $pid
done
