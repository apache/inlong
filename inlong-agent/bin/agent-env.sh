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



#project directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do
  TARGET="$(readlink "$SOURCE")"
  if [[ $TARGET == /* ]]; then
    SOURCE="$TARGET"
  else
    DIR="$( dirname "$SOURCE" )"
    SOURCE="$DIR/$TARGET"
  fi
done
CURRENT_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
BASE_DIR="$(dirname "$CURRENT_DIR")"

AS_USER=`whoami`
export LOG_DIR="$BASE_DIR/logs"

mkdir -p $LOG_DIR
chown -R $AS_USER $LOG_DIR

# find java home
if [ -z "$JAVA_HOME" ]; then
  export JAVA=`which java`
else
  export JAVA="$JAVA_HOME/bin/java"
fi

HEAP_OPTS="-Xmx1024m -Xms512m"
GC_OPTS="-XX:SurvivorRatio=6 -XX:+UseMembar -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark -XX:ParallelCMSThreads=3 -XX:+TieredCompilation -XX:+UseCMSCompactAtFullCollection -XX:+UnlockCommercialFeatures -XX:+UnlockCommercialFeatures -verbose:gc -Xloggc:$BASE_DIR/logs/gc.log.`date +%Y-%m-%d-%H-%M-%S` -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$BASE_DIR/logs/ -XX:+CMSClassUnloadingEnabled -XX:CMSInitiatingOccupancyFraction=60 -XX:CMSFullGCsBeforeCompaction=1 -Dsun.net.inetaddr.ttl=3 -Dsun.net.inetaddr.negative.ttl=1 -Djava.net.preferIPv4Stack=true"
AGENT_JVM_ARGS="$HEAP_OPTS $GC_OPTS"
export CLASSPATH=$CLASSPATH:$BASE_DIR/conf:$(ls $BASE_DIR/lib/*.jar | tr '\n' :)
export AGENT_ARGS="$AGENT_JVM_ARGS -cp $CLASSPATH -Dagent.home=$BASE_DIR -Dlog4j.configuration=file:$BASE_DIR/conf/log4j.properties"
