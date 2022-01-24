#!/bin/bash
##启动tubemq 的broker和manager
#当前项目的根路径
TUBE_MASTER_PORT=8715
TUBE_MASTER_WEB_PORT=8080
TUBE_MASTER_TOKEN=abc

basePath=$(cd `dirname $0`; pwd)
source $basePath/inlongConfig.properties
tubemqDBNAME="tubemanager"      #tubemq manager 元数据库名称 
 
TUBE_MASTER_IP=$tubemqMaster_hostName
                                 


if [ $source_type == "tubemq" ];then
   cd $basePath/inlong-tubemq-server/bin
   echo  "赋执行权限"
   chmod 755 *.sh
   echo "启动tubemq broker ..."
   ./tubemq.sh broker start
   echo "启动tubemq manager..."
   cd $basePath/inlong-tubemq-manager/bin
   #创建tubemanager数据库
   create_db_sql="create database IF NOT EXISTS ${tubemqDBNAME}"
   mysql -h${spring_datasource_hostname}  -P${spring_datasource_port}  -u${spring_datasource_username} -p${spring_datasource_password} -e "${create_db_sql}"
   ./start-manager.sh
   sleep 10
   #第一次初始化。
   flag=$(cat init-tube-cluster.sh | grep "flag=1") 
   if [ ! -n "$flag"  ] ;then
	  echo  "初始化init脚本参数"  
      sed -i 's/TUBE_MANAGER_IP=.*/'''TUBE_MANAGER_IP=${TUBE_MANAGER_IP}'''/g' init-tube-cluster.sh 
	  sed -i 's/TUBE_MANAGER_PORT=.*/'''TUBE_MANAGER_PORT=${TUBE_MANAGER_PORT}'''/g' init-tube-cluster.sh 
	  sed -i 's/TUBE_MASTER_IP=.*/'''TUBE_MASTER_IP=${TUBE_MASTER_IP}'''/g' init-tube-cluster.sh 
	  sed -i 's/TUBE_MASTER_PORT=.*/'''TUBE_MASTER_PORT=${TUBE_MASTER_PORT}'''/g' init-tube-cluster.sh 
	  sed -i 's/TUBE_MASTER_WEB_PORT=.*/'''TUBE_MASTER_WEB_PORT=${TUBE_MASTER_WEB_PORT}'''/g' init-tube-cluster.sh 
	  sed -i 's/TUBE_MASTER_TOKEN=.*/'''TUBE_MASTER_TOKEN=${TUBE_MASTER_TOKEN}'''/g' init-tube-cluster.sh 
      echo  "初始化tubemq集群"
	  ./init-tube-cluster.sh 
	  echo -e "\nflag=1" >> init-tube-cluster.sh
   else
	  echo "tubemq集群已初始化,跳过"
   fi  
fi   