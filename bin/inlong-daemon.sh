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

#start-up Tubemq
startTubeMqFun(){
    tubemqMaster_webPort=8080
    TUBE_MASTER_PORT=8715
    TUBE_MASTER_TOKEN=abc
    tubemqDBNAME="tubemanager"      #tubemq manager database
    basePath=$(cd `dirname $0`; cd ..; pwd)
    source $basePath/conf/standalone.conf
    TUBE_MASTER_IP=$tubemqMaster_hostName
    cd $basePath/inlong-tubemq-server/bin
    chmod 755 *.sh
    echo "start-up tubemq master ..."
    ./tubemq.sh master start 
    sleep 10
    # add broker
    curl -d 'type=op_modify&method=admin_add_broker_configure&brokerId=1&brokerIp='"$tubemqBroker_hostName"'&createUser=docker&brokerPort=8123&deletePolicy=delete,168h&numPartitions=3&unflushThreshold=1000&acceptPublish=true&acceptSubscribe=true&unflushInterval=10000&confModAuthToken=abc' http://$tubemqMaster_hostName:$tubemqMaster_webPort/webapi.htm
    # online
    curl -d 'type=op_modify&method=admin_online_broker_configure&brokerId=1&modifyUser=docker&confModAuthToken=abc' http://$tubemqMaster_hostName:$tubemqMaster_webPort/webapi.htm
    # broker start
    echo "start-up tubemq broker ..."
    ./tubemq.sh broker start
    #manager start
    echo "start-up tubemq manager ..."
    cd $basePath/inlong-tubemq-manager/bin                          
    #create tubemanager database
    create_db_sql="create database IF NOT EXISTS ${tubemqDBNAME}"
    mysql -h${spring_datasource_hostname}  -P${spring_datasource_port}  -u${spring_datasource_username} -p${spring_datasource_password} -e "${create_db_sql}"
    ./start-manager.sh
    sleep 10
    #first init
    flag=$(cat init-tube-cluster.sh | grep "flag=1") 
    if [ ! -n "$flag"  ] ;then
        echo  "init shell config"  
        sed -i 's/TUBE_MANAGER_IP=.*/'''TUBE_MANAGER_IP=${TUBE_MANAGER_IP}'''/g' init-tube-cluster.sh 
    	  sed -i 's/TUBE_MANAGER_PORT=.*/'''TUBE_MANAGER_PORT=${TUBE_MANAGER_PORT}'''/g' init-tube-cluster.sh 
    	  sed -i 's/TUBE_MASTER_IP=.*/'''TUBE_MASTER_IP=${TUBE_MASTER_IP}'''/g' init-tube-cluster.sh 
    	  sed -i 's/TUBE_MASTER_PORT=.*/'''TUBE_MASTER_PORT=${TUBE_MASTER_PORT}'''/g' init-tube-cluster.sh 
    	  sed -i 's/TUBE_MASTER_WEB_PORT=.*/'''TUBE_MASTER_WEB_PORT=${tubemqMaster_webPort}'''/g' init-tube-cluster.sh 
    	  sed -i 's/TUBE_MASTER_TOKEN=.*/'''TUBE_MASTER_TOKEN=${TUBE_MASTER_TOKEN}'''/g' init-tube-cluster.sh 
          echo  "init tubemq cluster"
    	  ./init-tube-cluster.sh 
    	  echo -e "\nflag=1" >> init-tube-cluster.sh
      else
    	  echo "tubemq cluster initialized,skip"
    fi   
}  

#start inlong
startInlong(){
    #default zk root path
    cluster_zk_root=inlong_hive
    #default sort_appName
    sort_appName=inlong_app
    
    basePath=$(cd `dirname $0`; cd ..; pwd)
    source $basePath/conf/standalone.conf
    
    inlongDBNAME="apache_inlong_manager"  # inlong manager Metabase name  
    
    echo "Judge the choice of message middleware tubemq or pulsar"
    if [ $source_type == "pulsar" ];then
    #Judge whether the pulsar cluster is started in the current system
       pulsar_thread=$($JAVA_HOME/bin/jps |grep PulsarBrokerStarter)
       if [ ! -n "$pulsar_thread" ]; then 
          echo "The system does not start the pulsar. Please start the pulsar manually first" 
    	  exit 1
       else
    	  echo "The current system starts the pulsar, which is used to complete message delivery and storage"
       fi
    else
       startTubeMqFun
	   echo  "Judge whether the  TubeMQ cluster is started in the current system"
       tubemq_manager_thread=$($JAVA_HOME/bin/jps |grep TubeMQManager)
       tubemq_broker_thread=$($JAVA_HOME/bin/jps |grep BrokerStartup)
       tubemq_master_thread=$($JAVA_HOME/bin/jps |grep MasterStartup)
       if [[ ! -n "$tubemq_manager_thread" && ! -n "$tubemq_broker_thread" && ! -n "$tubemq_master_thread" ]]; then 
          echo "The system does not start tubemq or starts abnormally. Please start tubemq normally first" 
    	  exit 1
       else
    	  echo "The current system starts tubemq, which is used to complete message delivery and storage"
       fi
    fi   
    
    echo "2.start-up inlong manager"	
    cd $basePath/inlong-manager-web
    #Whether the database table exists. If it does not exist, initialize the database and skip if it exists.
    select_db_sql="SELECT COUNT(*) FROM information_schema.TABLES WHERE table_schema = '${inlongDBNAME}'"
    inlong_manager_count=$(mysql -h${spring_datasource_hostname}  -P${spring_datasource_port}  -u${spring_datasource_username} -p${spring_datasource_password} -e "${select_db_sql}")
    inlong_num=$(echo $inlong_manager_count | tr -cd "[0-9]")
    if [ $inlong_num -eq 0 ] ;then
       echo  "init apache_inlong_manager database"  
       mysql -h${spring_datasource_hostname}  -P${spring_datasource_port}  -u${spring_datasource_username} -p${spring_datasource_password} < sql/apache_inlong_manager.sql
     else
       echo "apache_inlong_manager database initialized,skip"
    fi   
    cd $basePath/inlong-manager-web/bin
    ./startup.sh
    
    echo "3.start-up inlong web"
    if [[ "$manager_server_hostname" = "localhost" || "$manager_server_hostname" = "127.0.0.1" ]];then
          manager_server_hostname=$local_ip
    fi
    echo $manager_server_hostname
    docker stop website
    docker rm website
    docker pull inlong/website:latest
    docker run -d --name website -e MANAGER_API_ADDRESS=$manager_server_hostname:$manager_server_port -p $inlong_web_port:$docker_inlong_web_port inlong/website
    
    
    echo "4.start-up inlong dataproxy"
    cd $basePath/inlong-dataproxy/bin
    chmod 755 *.sh
    ./prepare_env.sh
    ./dataproxy-start.sh
    select_db_sql="SELECT COUNT(*) FROM  ${inlongDBNAME}.data_proxy_cluster where address='localhost'"
    cluster_count=$(mysql -h${spring_datasource_hostname}  -P${spring_datasource_port}  -u${spring_datasource_username} -p${spring_datasource_password} -e "${select_db_sql}")
    cluster_num=$(echo $cluster_count | tr -cd "[0-9]")
    #Insert the cluster information if it does not exist, and skip if it exists
    if [ $cluster_num -eq 0 ] ;then
       echo  "Insert cluster information into data_ proxy_ cluster table"  
       insert_db_sql="insert into ${inlongDBNAME}.data_proxy_cluster (name, address, port, status, is_deleted, creator, create_time, modify_time)
       values ('dataproxy', 'localhost',46801, 0, 0, 'admin', now(), now());"
       mysql -h${spring_datasource_hostname}  -P${spring_datasource_port}  -u${spring_datasource_username} -p${spring_datasource_password} -e "${insert_db_sql}"
    else
       echo "cluster information already exists, skip"
    fi   
    
    echo "5.start-up inlong agent"
    cd $basePath/inlong-agent/bin
    ./agent.sh start
    
    
    echo "6.start-up inlong sort"
    #Judge whether the system has started Fink cluster
       flink_thread=$($JAVA_HOME/bin/jps |grep TaskManagerRunner)
       if [ ! -n "$flink_thread" ]; then 
          echo "The system does not start Flink. Please start Flink manually first" 
    	  exit 1
       else
    	  echo "Currently, the system starts Flink, which is used to process sort tasks"
       fi
    #This file is mainly used to find the Flink bin directory
    flagfile=`find / -name find-flink-home.sh`
    flink_bin=$(dirname $flagfile)
    cd $basePath
    $flink_bin/flink run -c org.apache.inlong.sort.flink.Entrance inlong-sort/sort-core*.jar \
    --cluster-id $sort_appName --zookeeper.quorum $zkServerAddr --zookeeper.path.root $cluster_zk_root \
    --source.type $source_type --sink.type $sink_type &
}

#stop inlong
stopInlong(){
    echo "1.stop agent ... "
    agent_thread=$($JAVA_HOME/bin/jps |grep AgentMain)
    if [ ! -n "$agent_thread" ]; then 
       echo "The system did not running agent" 
    else
       agent_pid=$(echo $agent_thread | tr -cd "[0-9]")
       kill -9 $agent_pid
     echo "agent stopped "
    fi
    
    echo "2.stop inlong_manager ... "
    inlong_manager_thread=$($JAVA_HOME/bin/jps |grep InLongWebApplication)
    if [ ! -n "$inlong_manager_thread" ]; then 
       echo "The system did not running inlong manager" 
    else
       inlong_manager_pid=$(echo $inlong_manager_thread | tr -cd "[0-9]")
       kill -9 $inlong_manager_pid
       echo "inlong_manager stopped "
    fi
    
    echo "3.stop dataproxy ... "
    dataproxy_thread=$($JAVA_HOME/bin/jps |grep Application)
    if [ ! -n "$dataproxy_thread" ]; then 
       echo "The system did not running  dataproxy" 
    else
       dataproxy_pid=$(echo $dataproxy_thread | tr -cd "[0-9]")
       kill -9 $dataproxy_pid
       echo "dataproxy stopped "
    fi
    
    echo "4.stop tubemq_manager ... "
    tubemq_manager_thread=$($JAVA_HOME/bin/jps |grep TubeMQManager)
    if [ ! -n "$tubemq_manager_thread" ]; then 
       echo "The system did not running tubemq_manager" 
    else
       tubemq_manager_pid=$(echo $tubemq_manager_thread | tr -cd "[0-9]")
       kill -9 $tubemq_manager_pid
       echo "tubemq_manager stopped "
    fi
    
    echo "5.stop tubemq_broker ... "
    tubemq_broker_thread=$($JAVA_HOME/bin/jps |grep BrokerStartup)
    if [ ! -n "$tubemq_broker_thread" ]; then 
       echo "The system did not running  tubemq_broker" 
    else
       tubemq_broker_pid=$(echo $tubemq_broker_thread | tr -cd "[0-9]")
       kill -9 $tubemq_broker_pid
       echo "tubemq_broker stopped "
    fi
    
    echo "6.stop tubemq_master... "
    tubemq_master_thread=$($JAVA_HOME/bin/jps |grep MasterStartup)
    if [ ! -n "$tubemq_master_thread" ]; then 
       echo "The system did not running tubemq_master" 
    else
       tubemq_master_pid=$(echo $tubemq_master_thread | tr -cd "[0-9]")
       kill -9 $tubemq_master_pid
       echo "tubemq_master stopped "
    fi
    
    echo "7.stop CliFrontend... "
    cli_frontend_thread=$($JAVA_HOME/bin/jps |grep CliFrontend)
    if [ ! -n "$cli_frontend_thread" ]; then 
       echo "The system did not running cli_frontend" 
    else
       cli_frontend_pid=$(echo $cli_frontend_thread | tr -cd "[0-9]")
       kill -9 $cli_frontend_pid
       echo "cli_frontend stopped "
    fi
	
	echo "8.stop sort flink job"
	 stopFlinkJob
}



stopFlinkJob(){
    #This file is mainly used to find the Flink bin directory
    flagfile=`find / -name find-flink-home.sh`
    flink_bin=$(dirname $flagfile)
    runjob=$($flink_bin/flink list -r|grep "$sort_appName")
    echo $runjob
    OLD_IFS="$IFS"
    IFS=":"
    array=($runjob)
    IFS="$OLD_IFS"
    jobId=$(echo "${array[3]}")
    echo $jobId
	$flink_bin/flink cancel $jobId
}


# if less than two arguments supplied
if [ $# -lt 2 ]; then
  help;
  exit 1;
fi

COMMAND=$1
SERVICE=$2

if [ "$SERVICE" == standalone ];then 
case $COMMAND in
  start)
    startInlong;
    ;;
  stop)
    stopInlong;
    ;;
  *)
    help;
    exit 1;
    ;;
esac
else 
 echo "configs wrong such as ./inlong-daemon.sh start standalone "
fi
