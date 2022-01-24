#!/bin/bash
##一键启动脚本,运行该脚本之前,先运行initInlongConfig.sh,初始化inlong各组件的配置文件
#当前项目的根路径

#默认zk
zkServerAddr=localhost:2181
#默认zk root path
cluster_zk_root=inlong_hive
#默认sort_appName
sort_appName=inlong_app

basePath=$(cd `dirname $0`; pwd)
source $basePath/inlongConfig.properties

inlongDBNAME="apache_inlong_manager"  # inlong manager 元数据库名称  


echo "1.启动消息中间件(pulsar or tubemq)和对应组件的manager,其中pulsar由用户自启动,tubemq使用inlong自带的"
#判断消息中间件的选择 tubemq or pulsar
if [ $source_type == "pulsar" ];then
#判断当前系统是否启动了pulsar集群
   pulsar_thread=$($JAVA_HOME/bin/jps |grep PulsarBrokerStarter)
   if [ ! -n "$pulsar_thread" ]; then 
      echo "系统没启动pulsar,请先手动启动pulsar" 
	  exit 1
   else
	  echo "当前系统启动pulsar,使用该pulsar来完成消息传递和存储"
   fi
else
   tubemq_manager_thread=$($JAVA_HOME/bin/jps |grep TubeMQManager)
   tubemq_broker_thread=$($JAVA_HOME/bin/jps |grep BrokerStartup)
   tubemq_master_thread=$($JAVA_HOME/bin/jps |grep MasterStartup)
   if [[ ! -n "$tubemq_manager_thread" && ! -n "$tubemq_broker_thread" && ! -n "$tubemq_master_thread" ]]; then 
      echo "系统没启动tubemq或启动异常,请先正常启动tubemq" 
	  exit 1
   else
	  echo "当前系统启动tubemq,使用该tubemq来完成消息传递和存储"
   fi
fi   

echo "2.启动 inlong manager"	
cd $basePath/inlong-manager-web
#是否存在库表,不存在就初始化数据库,存在就跳过。
select_db_sql="SELECT COUNT(*) FROM information_schema.TABLES WHERE table_schema = '${inlongDBNAME}'"
inlong_manager_count=$(mysql -h${spring_datasource_hostname}  -P${spring_datasource_port}  -u${spring_datasource_username} -p${spring_datasource_password} -e "${select_db_sql}")
inlong_num=$(echo $inlong_manager_count | tr -cd "[0-9]")
if [ $inlong_num -eq 0 ] ;then
   echo  "初始化apache_inlong_manager数据库"  
   mysql -h${spring_datasource_hostname}  -P${spring_datasource_port}  -u${spring_datasource_username} -p${spring_datasource_password} < sql/apache_inlong_manager.sql
 else
   echo "apache_inlong_manager数据库已初始化"
fi   
cd $basePath/inlong-manager-web/bin
./startup.sh

echo "3.启动inlong web"
if [[ "$manager_server_hostname" = "localhost" || "$manager_server_hostname" = "127.0.0.1" ]];then
      manager_server_hostname=$local_ip
fi
echo $manager_server_hostname
docker stop website
docker rm website
docker pull inlong/website:latest
docker run -d --name website -e MANAGER_API_ADDRESS=$manager_server_hostname:$manager_server_port -p $inlong_web_port:$docker_inlong_web_port inlong/website


echo "4.启动inlong dataproxy"
cd $basePath/inlong-dataproxy/bin
echo  "赋执行权限"
chmod 755 *.sh
./prepare_env.sh
./dataproxy-start.sh
# name 为 DataProxy 的名称，可自定义
# address 为 DataProxy 服务所在主机的 IP
# port 为 DataProxy 服务所在的端口号，默认是 46801
select_db_sql="SELECT COUNT(*) FROM  ${inlongDBNAME}.data_proxy_cluster where address='localhost'"
cluster_count=$(mysql -h${spring_datasource_hostname}  -P${spring_datasource_port}  -u${spring_datasource_username} -p${spring_datasource_password} -e "${select_db_sql}")
cluster_num=$(echo $cluster_count | tr -cd "[0-9]")
#cluster 信息不存在就插入,存在就跳过
if [ $cluster_num -eq 0 ] ;then
   echo  "插入cluster 信息到data_proxy_cluster表"  
   insert_db_sql="insert into ${inlongDBNAME}.data_proxy_cluster (name, address, port, status, is_deleted, creator, create_time, modify_time)
   values ('dataproxy', 'localhost',46801, 0, 0, 'admin', now(), now());"
   mysql -h${spring_datasource_hostname}  -P${spring_datasource_port}  -u${spring_datasource_username} -p${spring_datasource_password} -e "${insert_db_sql}"
else
   echo "cluster 信息已存在 ,跳过"
fi   

echo "5.启动inlong agent"
cd $basePath/inlong-agent/bin
./agent.sh start


echo "6.启动inlong sort"
#判断系统是否启动了fink集群
   flink_thread=$($JAVA_HOME/bin/jps |grep TaskManagerRunner)
   if [ ! -n "$flink_thread" ]; then 
      echo "系统没启动flink,请先手动启动flink" 
	  exit 1
   else
	  echo "当前系统启动flink,使用该flink来处理sort任务"
   fi
#用该文件主要是为寻找flink bin目录
flagfile=`find / -name find-flink-home.sh`
flink_bin=$(dirname $flagfile)
cd $basePath
$flink_bin/flink run -c org.apache.inlong.sort.flink.Entrance inlong-sort/sort-core*.jar \
--cluster-id $sort_appName --zookeeper.quorum $zkServerAddr --zookeeper.path.root $cluster_zk_root \
--source.type $source_type --sink.type $sink_type &
