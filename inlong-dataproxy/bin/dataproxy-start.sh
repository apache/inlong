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
cd "$(dirname "$0")"/../conf || exit

for i in {mx.properties,transfer.properties,weight.properties,common.properties,blacklist.properties,groupid_mapping.properties,dc_mapping.properties,topics.properties,tube_switch.properties,thrid_party_cluster.properties}
do
  if [ ! -f "$i" ]; then
    touch "$i"
  fi
done

cd .. || exit
nohup bin/flume-ng agent --conf conf/ -f conf/flume.conf -n agent1 --no-reload-conf  > /dev/null 2>&1 &