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
# config
sed -i "s/TUBE_LIST/$TUBMQ_MASTER_LIST/g" ${file_path}/conf/flume.conf
cat <<EOF > ${file_path}/conf/common.properties
manager_hosts=$MANAGER_OPENAPI_IP:$MANAGER_OPENAPI_PORT
EOF
# start
sh ${file_path}/bin/dataproxy-start.sh
sleep 3
# keep alive
tail -F ${file_path}/logs/flume.log
