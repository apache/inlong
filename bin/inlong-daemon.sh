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

# globle values
basePath=$(
  cd $(dirname $0)
  cd ..
  pwd
)
tubemqDBNAME="tubemanager"           # tubemq manager database
inlongDBNAME="apache_inlong_manager" # inlong manager Metabase name
source $basePath/conf/inlong.conf
# if less than two arguments supplied
if [ $# -lt 2 ]; then
  commandHelp
  exit 1
fi

COMMAND=$1
SERVICE=$2
initCompo() {
  if [[ "$SERVICE" != standalone ]]; then
    cd $basePath/bin
    echo "config $SERVICE"
    ./init-config.sh $SERVICE
  fi
}

startTubeMqMaster() {
  initCompo
  echo "start-up tubemq master ..."
  cd $basePath/inlong-tubemq-server/bin
  chmod 755 tubemq.sh
  ./tubemq.sh master start
}

startTubeMqBroker() {
  initCompo
  # Judge whether the TubeMqMaster is started in the current system
  tubemq_master_thread=$($JAVA_HOME/bin/jps | grep MasterStartup)
  if [[ ! -n "$tubemq_master_thread" ]]; then
    echo "The system does not start tubemqMaster or starts abnormally. Please start tubemqMaster normally first"
    exit 1
  fi
  # add broker
  curl -d 'type=op_modify&method=admin_add_broker_configure&brokerId='"$brokerId"'&brokerIp='"$tubemqBroker_hostName"'&createUser=docker&brokerPort='"$tubemqBroker_port"'&deletePolicy=delete,168h&numPartitions=3&unflushThreshold=1000&acceptPublish=true&acceptSubscribe=true&unflushInterval=10000&confModAuthToken='"$confModAuthToken"'' http://$tubemqMaster_hostName:$tubemqMaster_webPort/webapi.htm
  # online
  curl -d 'type=op_modify&method=admin_online_broker_configure&brokerId='"$brokerId"'&modifyUser=docker&confModAuthToken='"$confModAuthToken"'' http://$tubemqMaster_hostName:$tubemqMaster_webPort/webapi.htm
  # broker start
  echo "start-up tubemq broker ..."
  cd $basePath/inlong-tubemq-server/bin
  chmod 755 tubemq.sh
  ./tubemq.sh broker start
}

startTubeMqManager() {
  initCompo
  TUBE_MASTER_IP=$tubemqMaster_hostName
  # Judge whether the  TubeMqServer is started in the current system
  tubemq_broker_thread=$($JAVA_HOME/bin/jps | grep BrokerStartup)
  tubemq_master_thread=$($JAVA_HOME/bin/jps | grep MasterStartup)
  if [[ ! -n "$tubemq_broker_thread" && ! -n "$tubemq_master_thread" ]]; then
    echo "The system does not start tubemqServer or starts abnormally. Please start tubemqServer(master and broker) normally first"
    exit 1
  fi
  echo "start-up tubemq manager ..."
  cd $basePath/inlong-tubemq-manager/bin
  # create tubemanager database
  create_db_sql="create database IF NOT EXISTS ${tubemqDBNAME}"
  mysql -h${spring_datasource_hostname} -P${spring_datasource_port} -u${spring_datasource_username} -p${spring_datasource_password} -e "${create_db_sql}"
  ./start-manager.sh
  sleep 10
  # first init
  flag=$(cat init-tube-cluster.sh | grep "flag=1")
  if [ ! -n "$flag" ]; then
    echo "init shell config"
    sed -i 's/TUBE_MANAGER_IP=.*/'''TUBE_MANAGER_IP=${TUBE_MANAGER_IP}'''/g' init-tube-cluster.sh
    sed -i 's/TUBE_MANAGER_PORT=.*/'''TUBE_MANAGER_PORT=${TUBE_MANAGER_PORT}'''/g' init-tube-cluster.sh
    sed -i 's/TUBE_MASTER_IP=.*/'''TUBE_MASTER_IP=${tubemqMaster_hostName}'''/g' init-tube-cluster.sh
    sed -i 's/TUBE_MASTER_PORT=.*/'''TUBE_MASTER_PORT=${tubemqMaster_port}'''/g' init-tube-cluster.sh
    sed -i 's/TUBE_MASTER_WEB_PORT=.*/'''TUBE_MASTER_WEB_PORT=${tubemqMaster_webPort}'''/g' init-tube-cluster.sh
    sed -i 's/TUBE_MASTER_TOKEN=.*/'''TUBE_MASTER_TOKEN=${confModAuthToken}'''/g' init-tube-cluster.sh
    echo "init tubemq cluster"
    ./init-tube-cluster.sh
    echo -e "\nflag=1" >>init-tube-cluster.sh
  else
    echo "tubemq cluster initialized,skip"
  fi
}

startInlongAudit() {
  initCompo
  echo "init apache_inlong_audit"
  cd $basePath/inlong-audit
  select_db_sql="SELECT COUNT(*) FROM information_schema.TABLES WHERE table_schema = 'apache_inlong_audit'"
  inlong_audit_count=$(mysql -h${spring_datasource_hostname} -P${spring_datasource_port} -u${spring_datasource_username} -p${spring_datasource_password} -e "${select_db_sql}")
  inlong_num=$(echo $inlong_audit_count | tr -cd "[0-9]")
  if [ $inlong_num -eq 0 ]; then
    mysql -h${spring_datasource_hostname} -P${spring_datasource_port} -u${spring_datasource_username} -p${spring_datasource_password} <sql/apache_inlong_audit.sql
  else
    echo "apache_inlong_audit database initialized,skip"
  fi
  if [ $source_type == "pulsar" ]; then
    cd $basePath/inlong-audit/bin
    echo "start-up audit"
    ./proxy-start.sh
    echo "start audit_store"
    ./store-start.sh
  fi
}

startInlongManager() {
  initCompo
  echo "start-up inlong manager ..."
  cd $basePath/inlong-manager
  # Whether the database table exists. If it does not exist, initialize the database and skip if it exists.
  select_db_sql="SELECT COUNT(*) FROM information_schema.TABLES WHERE table_schema = '${inlongDBNAME}'"
  inlong_manager_count=$(mysql -h${spring_datasource_hostname} -P${spring_datasource_port} -u${spring_datasource_username} -p${spring_datasource_password} -e "${select_db_sql}")
  inlong_num=$(echo $inlong_manager_count | tr -cd "[0-9]")
  if [ $inlong_num -eq 0 ]; then
    echo "init apache_inlong_manager database"
    mysql -h${spring_datasource_hostname} -P${spring_datasource_port} -u${spring_datasource_username} -p${spring_datasource_password} <sql/apache_inlong_manager.sql
  else
    echo "apache_inlong_manager database initialized,skip"
  fi
  cd $basePath/inlong-manager/bin
  ./startup.sh
}

startInlongDashboard() {
  echo "start-up inlong dashboard ..."
  if [[ "$manager_server_hostname" == "localhost" || "$manager_server_hostname" == "127.0.0.1" ]]; then
    manager_server_hostname=$local_ip
  fi
  docker pull inlong/dashboard:latest
  docker run -d --name dashboard -e MANAGER_API_ADDRESS=$manager_server_hostname:$manager_server_port -p $inlong_web_port:$docker_inlong_web_port inlong/dashboard
}

startInlongDataProxy() {
  initCompo
  echo "start-up inlong dataproxy ..."
  cd $basePath/inlong-dataproxy/bin
  chmod 755 *.sh
  ./dataproxy-start.sh
  echo "update cluster information into data_proxy_cluster table"
  update_db_sql="UPDATE apache_inlong_manager.data_proxy_cluster SET address='"$dataproxy_ip"' WHERE name='default_dataproxy'"
  mysql -h${spring_datasource_hostname} -P${spring_datasource_port} -u${spring_datasource_username} -p${spring_datasource_password} -e "${update_db_sql}"
  echo "cluster information updated"
}

startInlongAgent() {
  initCompo
  echo "start-up inlong agent ..."
  cd $basePath/inlong-agent/bin
  ./agent.sh start
}

startInlongSort() {
  echo "start-up inlong sort ..."
  # Judge whether the system has started Fink cluster
  flink_thread=$($JAVA_HOME/bin/jps | grep TaskManagerRunner)
  if [ ! -n "$flink_thread" ]; then
    echo "The system does not start Flink. Please start Flink manually first"
    exit 1
  else
    echo "Currently, the system starts Flink, which is used to process sort tasks"
  fi
  # This file is mainly used to find the Flink bin directory
  flagfile=$(find / -name find-flink-home.sh)
  flink_bin=$(dirname $flagfile)
  cd $basePath
  $flink_bin/flink run -c org.apache.inlong.sort.flink.Entrance inlong-sort/sort-core*.jar \
  --cluster-id $sort_appName --zookeeper.quorum $zkServerAddr --zookeeper.path.root $cluster_zk_root \
  --source.type $source_type --sink.type $sink_type &
}

# start inlong
startInlongAll() {
  # start-up message middleware
  echo "Judge the choice of message middleware tubemq or pulsar"
  if [ $source_type == "pulsar" ]; then
    # Judge whether the pulsar cluster is started in the current system
    pulsar_thread=$($JAVA_HOME/bin/jps | grep PulsarBrokerStarter)
    if [ ! -n "$pulsar_thread" ]; then
      echo "The system does not start the pulsar. Please start the pulsar manually first"
      exit 1
    else
      echo "The current system starts the pulsar, which is used to complete message delivery and storage"
    fi
  else
    startTubeMqMaster
    sleep 15
    startTubeMqBroker
    startTubeMqManager
  fi
  #start-up inlong audit
  startInlongAudit
  # start-up inlong manager
  startInlongManager
  # start-up inlong dashboard
  startInlongDashboard
  # start-up inlong dataproxy
  startInlongDataProxy
  # start-up inlong agent
  startInlongAgent
  # start-up inlong sort
  startInlongSort
}

stopTubeMqMaster() {
  echo "stop tubemq_master... "
  cd $basePath/inlong-tubemq-server/bin
  chmod 755 tubemq.sh
  ./tubemq.sh master stop
}

stopTubeMqBroker() {
  echo "stop tubemq_broker ... "
  cd $basePath/inlong-tubemq-server/bin
  chmod 755 tubemq.sh
  ./tubemq.sh broker stop
}

stopTubeMqManager() {
  echo "stop tubemq_manager ... "
  cd $basePath/inlong-tubemq-manager/bin
  ./stop-manager.sh
}

stopInlongManager() {
  echo "stop inlong_manager ... "
  cd $basePath/inlong-manager/bin
  ./shutdown.sh
}

stopInlongDashboard() {
  docker stop dashboard
  docker rm dashboard
}

stopInlongDataProxy() {
  echo "stop dataproxy ... "
  cd $basePath/inlong-dataproxy/bin
  chmod 755 *.sh
  ./dataproxy-stop.sh
}

stopInlongAudit() {
  echo "stop audit ... "
  cd $basePath/inlong-audit/bin
  ./proxy-stop.sh
  ./store-stop.sh
}

stopInlongAgent() {
  echo "stop agent ... "
  cd $basePath/inlong-agent/bin
  ./agent.sh stop
}

stopInlongSort() {
  echo "stop CliFrontend... "
  cli_frontend_thread=$($JAVA_HOME/bin/jps | grep CliFrontend)
  if [ ! -n "$cli_frontend_thread" ]; then
    echo "The system did not running cli_frontend"
  else
    cli_frontend_pid=$(echo $cli_frontend_thread | tr -cd "[0-9]")
    kill -9 $cli_frontend_pid
    echo "cli_frontend stopped "
  fi
  echo "stop sort flink job"
  #This file is mainly used to find the Flink bin directory
  flagfile=$(find / -name find-flink-home.sh)
  flink_bin=$(dirname $flagfile)
  runjob=$($flink_bin/flink list -r | grep "$sort_appName")
  OLD_IFS="$IFS"
  IFS=":"
  array=($runjob)
  IFS="$OLD_IFS"
  jobId=$(echo "${array[3]}")
  echo $jobId
  $flink_bin/flink cancel $jobId
}

# stop inlong
stopInlongAll() {
  #stop inlong tubemq
  if [ $source_type == "tubemq" ]; then
    stopTubeMqMaster
    stopTubeMqBroker
    stopTubeMqManager
  fi
  # stop inlong manager
  stopInlongManager
  # stop inlong website
  stopInlongDashboard
  if [ $source_type == "pulsar" ]; then
    # stop inlong audit
    stopInlongAudit
  fi
  # stop inlong dataproxy
  stopInlongDataProxy
  # stop inlong agent
  stopInlongAgent
  # stop inlong sort
  stopInlongSort
}

commandHelp() {
  echo "Usage: ./inlong-daemon.sh (start|stop) <command>
    where command is one of:
    inlongAudit         Run a inlongAudit server
    tubeMaster          Run a tubeMaster server
    tubeBroker          Run a tubeBroker server
    tubeManager         Run a tubeManager server
    inlongManager       Run a inlongManager server
    inlongDashboard     Run a inlongDashboard server
    inlongDataProxy     Run a inlongDataProxy server
    inlongAgent         Run a inlongAgent server
    inlongSort          Run a inlongSort server
    standalone          Run a standalone server(all servers)"
}

if [[ "$COMMAND" == start || "$COMMAND" == stop ]]; then
  case $SERVICE in
  tubeMaster)
    ${COMMAND}TubeMqMaster
    ;;
  tubeBroker)
    ${COMMAND}TubeMqBroker
    ;;
  tubeManager)
    ${COMMAND}TubeMqManager
    ;;
  inlongManager)
    ${COMMAND}InlongManager
    ;;
  inlongDashboard)
    ${COMMAND}InlongDashboard
    ;;
  inlongDataProxy)
    ${COMMAND}InlongDataProxy
    ;;
  inlongAgent)
    ${COMMAND}InlongAgent
    ;;
  inlongSort)
    ${COMMAND}InlongSort
    ;;
  inlongAudit)
    ${COMMAND}InlongAudit
    ;;
  standalone)
    ${COMMAND}InlongAll
    ;;
  *)
    commandHelp
    exit 1
    ;;
  esac
else
  commandHelp
  exit 1
fi
