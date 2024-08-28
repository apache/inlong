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

BASE_DIR=$(dirname $0)/..

AGENT_CONF="${BASE_DIR}"/conf/agent.properties
source "${BASE_DIR}"/bin/agent-env.sh
CONSOLE_OUTPUT_FILE="${LOG_DIR}/oom.log"

function running() {
  agent_uniq=`cat ${AGENT_CONF}|grep -Ev '^[[:space:]].*|^#' |grep -E 'agent.uniq.id'`
  check_agent_uniq="${agent_uniq:-"agent.uniq.id=1"}"
  arg_uniq="-D${check_agent_uniq}"
  process=$(ps -aux | grep 'java' | grep 'inlong-agent' | grep "$check_agent_uniq" | awk '{print $2}')
  if [ "${process}" = "" ]; then
    return 1;
  else
    return 0;
  fi
}

function stop_agent() {
  time=$(date "+%Y-%m-%d %H:%M:%S")
  if ! running; then
    echo "$time oom agent is not running." >> $CONSOLE_OUTPUT_FILE
    exit 1
  fi
  count=0
  while running;
  do
    (( count++ ))
    time=$(date "+%Y-%m-%d %H:%M:%S")
    pid=$(ps -aux | grep 'java' | grep 'inlong-agent' | grep "$check_agent_uniq" | awk '{print $2}')
    echo "$time oom stopping agent($pid) $count times" >> $CONSOLE_OUTPUT_FILE
    if [ "${count}" -gt 10 ]; then
        echo "$time oom kill -9 $pid" >> $CONSOLE_OUTPUT_FILE
        kill -9 "${pid}"
    else
        echo "$time oom kill $pid" >> $CONSOLE_OUTPUT_FILE
        kill "${pid}"
    fi
    sleep 6;
  done
  time=$(date "+%Y-%m-%d %H:%M:%S")
  echo "$time oom stop agent($pid) successfully." >> $CONSOLE_OUTPUT_FILE
}

stop_agent;