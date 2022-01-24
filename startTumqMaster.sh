#!/bin/bash
##启动tubemq master
#当前项目的根路径
basePath=$(cd `dirname $0`; pwd)
source $basePath/inlongConfig.properties
if [ $source_type == "tubemq" ];then
   cd $basePath/inlong-tubemq-server/bin
   echo  "赋执行权限"
   chmod 755 *.sh
   echo "启动tubemq master ..."
   ./tubemq.sh master start 
fi











