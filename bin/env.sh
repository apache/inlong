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

#Config your java home
#JAVA_HOME=/opt/jdk/

if [ -z "$JAVA_HOME" ]; then
  export JAVA=`which java`
else
  export JAVA="$JAVA_HOME/bin/java"
fi

tubemq_home=$BASE_DIR
export CLASSPATH=$CLASSPATH:$BASE_DIR/conf:$(ls $BASE_DIR/lib/*.jar | tr '\n' :)

#Master jvm args
if [ -z "$MASTER_JVM_SIZE" ]; then
  MASTER_JVM_SIZE="-Xmx10g -Xms6g"
fi
MASTER_JVM_ARGS="$MASTER_JVM_SIZE -server -Dtubemq.home=$tubemq_home -cp $CLASSPATH "
#Broker jvm args
if [ -z "$MASTER_JVM_SIZE" ]; then
  BROKER_JVM_SIZE="-Xmx16g -Xms8g"
fi
BROKER_JVM_ARGS="$BROKER_JVM_SIZE -server -Dtubemq.home=$tubemq_home -cp $CLASSPATH "
#Tools jvm args,you don't have to modify this at all.
TOOLS_JVM_ARGS="-Xmx512m -Xms512m -Dtubemq.home=$tubemq_home -cp $CLASSPATH "
#Tool repair jvm args
TOOL_REPAIR_JVM_ARGS="-Xmx24g -Xms8g -Dtubemq.home=$tubemq_home -cp $CLASSPATH "

if [ -z "$MASTER_ARGS" ]; then
  export MASTER_ARGS="$MASTER_JVM_ARGS -Dlog4j.configuration=file:$BASE_DIR/conf/master.log4j.properties"
fi

if [ -z "$BROKER_ARGS" ]; then
  export BROKER_ARGS="$BROKER_JVM_ARGS -Dlog4j.configuration=file:$BASE_DIR/conf/log4j.properties"
fi

if [ -z "$TOOLS_ARGS" ]; then
  export TOOLS_ARGS="$TOOLS_JVM_ARGS -Dlog4j.configuration=file:$BASE_DIR/conf/tools.log4j.properties"
fi

if [ -z "$TOOL_REPAIR_ARGS" ]; then
  export TOOL_REPAIR_ARGS="$TOOL_REPAIR_JVM_ARGS -Dlog4j.configuration=file:$BASE_DIR/conf/tools.log4j.properties"
fi





