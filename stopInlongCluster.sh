#!/bin/bash
##一键stop启动脚本
echo "1.stop agent ... "
agent_thread=$($JAVA_HOME/bin/jps |grep AgentMain)
if [ ! -n "$agent_thread" ]; then 
   echo "系统没启动agent" 
else
   agent_pid=$(echo $agent_thread | tr -cd "[0-9]")
   kill -9 $agent_pid
 echo "agent stopped "
fi

echo "2.stop inlong_manager ... "
inlong_manager_thread=$($JAVA_HOME/bin/jps |grep InLongWebApplication)
if [ ! -n "$inlong_manager_thread" ]; then 
   echo "系统没启动inlong manager" 
else
   inlong_manager_pid=$(echo $inlong_manager_thread | tr -cd "[0-9]")
   kill -9 $inlong_manager_pid
   echo "inlong_manager stopped "
fi

echo "3.stop dataproxy ... "
dataproxy_thread=$($JAVA_HOME/bin/jps |grep Application)
if [ ! -n "$dataproxy_thread" ]; then 
   echo "系统没启动dataproxy" 
else
   dataproxy_pid=$(echo $dataproxy_thread | tr -cd "[0-9]")
   kill -9 $dataproxy_pid
 echo "dataproxy stopped "
fi

echo "4.stop tubemq_manager ... "
tubemq_manager_thread=$($JAVA_HOME/bin/jps |grep TubeMQManager)
if [ ! -n "$tubemq_manager_thread" ]; then 
   echo "系统没启动tubemq_manager" 
else
   tubemq_manager_pid=$(echo $tubemq_manager_thread | tr -cd "[0-9]")
   kill -9 $tubemq_manager_pid
 echo "tubemq_manager stopped "
fi

echo "5.stop tubemq_broker ... "
tubemq_broker_thread=$($JAVA_HOME/bin/jps |grep BrokerStartup)
if [ ! -n "$tubemq_broker_thread" ]; then 
   echo "系统没启动tubemq_broker" 
else
   tubemq_broker_pid=$(echo $tubemq_broker_thread | tr -cd "[0-9]")
   kill -9 $tubemq_broker_pid
 echo "tubemq_broker stopped "
fi

echo "6.stop tubemq_master... "
tubemq_master_thread=$($JAVA_HOME/bin/jps |grep MasterStartup)
if [ ! -n "$tubemq_master_thread" ]; then 
   echo "系统没启动tubemq_master" 
else
   tubemq_master_pid=$(echo $tubemq_master_thread | tr -cd "[0-9]")
   kill -9 $tubemq_master_pid
 echo "tubemq_master stopped "
fi

echo "7.stop CliFrontend... "
cli_frontend_thread=$($JAVA_HOME/bin/jps |grep CliFrontend)
if [ ! -n "$cli_frontend_thread" ]; then 
   echo "系统没启动cli_frontend" 
else
   cli_frontend_pid=$(echo $cli_frontend_thread | tr -cd "[0-9]")
   kill -9 $cli_frontend_pid
 echo "cli_frontend stopped "
fi
