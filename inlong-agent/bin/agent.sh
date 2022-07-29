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

source "${BASE_DIR}"/bin/agent-env.sh
CONSOLE_OUTPUT_FILE="${LOG_DIR}/agent-out.log"
CONFIG_FILE="${BASE_DIR}/conf/agent.properties"

function help() {
    echo "Usage: agent.sh {status|start|stop|restart}" >&2
    echo "       status:     the status of inlong agent"
    echo "       start:      start the inlong agent"
    echo "       stop:       stop the inlong agent"
    echo "       restart:    restart the inlong agent"
    echo "       help:       get help from inlong agent"
}

function running() {
	process=$("$JPS" | grep "AgentMain" | grep -v grep)
	if [ "${process}" = "" ]; then
	  	return 1;
	else
		return 0;
	fi
}

# update agent.local.ip in agent.properties
function update_local_ip() {
  local_ip=$(netstat -ntu | grep -v "127.0.0.1" | awk '{print $4}' | cut -d: -f1 | awk '/^[0-9]/ {print $0}' | sort | uniq -c | awk '{print $2}')
  if [ "${local_ip}" = "" ]; then
    local_ip="127.0.0.1"
  fi
  sed -i "s/agent.local.ip=127.0.0.1/agent.local.ip=${local_ip}/g" "${CONFIG_FILE}"
}

# start agent
function start_agent() {
  if running; then
		echo "agent is running."
		exit 1
	fi
	update_local_ip
	nohup ${JAVA} ${AGENT_ARGS} org.apache.inlong.agent.core.AgentMain > /dev/null 2>&1 &
}

# stop agent
function stop_agent() {
  if ! running; then
		echo "agent is not running."
		exit 1
	fi
	count=0
	pid=$("$JPS" | grep "AgentMain" | grep -v grep | awk '{print $1}')
	while running;
	do
	  (( count++ ))
	  echo "Stopping agent $count times"
	  if [ "${count}" -gt 10 ]; then
	  	  echo "kill -9 $pid"
	      kill -9 "${pid}"
	  else
	      kill "${pid}"
	  fi
	  sleep 6;
	done
	echo "Stop agent successfully."
}

# get status of agent
function status_agent() {
  if running; then
		echo "agent is running."
		exit 0
	else
	  echo "agent is not running."
	  exit 1
	fi
}

function help_agent() {
    "${JAVA}" "${AGENT_ARGS}"  org.apache.inlong.agent.core.AgentMain -h
}

command=$1
shift 1
case $command in
    status)
        status_agent $@;
        ;;
    start)
        start_agent $@;
        ;;
    stop)
        stop_agent $@;
        ;;
    restart)
        $0 stop $@
        $0 start $@
        ;;
    help)
        help_agent;
        ;;
    *)
        help;
        exit 1;
        ;;
esac