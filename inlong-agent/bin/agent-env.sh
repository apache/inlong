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


# Opentelemetry startup parameter configuration
export OTEL_SERVICE_NAME=inlong_agent
export OTEL_VERSION=1.28.0
export OTEL_LOGS_EXPORTER=otlp
# Whether to enable observability. true:enable; others:disable.
export ENABLE_OBSERVABILITY=false
# OTEL_EXPORTER_OTLP_ENDPOINT must be configured as a URL when ENABLE_OBSERVABILITY=true.
export OTEL_EXPORTER_OTLP_ENDPOINT=
export TDW_SECURITY_URL_NULL

#project directory
BASE_DIR=$(cd "$(dirname "$0")"/../;pwd)

AS_USER=`whoami`
export LOG_DIR="$BASE_DIR/logs"

mkdir -p $LOG_DIR
chown -R $AS_USER $LOG_DIR

# find java home
if [ -z "$JAVA_HOME" ]; then
  export JAVA=$(which java)
  export JPS=$(which jps)
else
  export JAVA="$JAVA_HOME/bin/java"
  export JPS="$JAVA_HOME/bin/jps"
fi

if [ -z "$AGENT_JVM_HEAP_OPTS" ]; then
  HEAP_OPTS=" -Xmx2048m -Xms512m -Xss512k "
else
  HEAP_OPTS="$AGENT_JVM_HEAP_OPTS"
fi

GVM_OPTS=" -Djava.net.preferIPv4Stack=true  -Dfile.encoding=UTF-8 "
OOM_HANDLER=" -XX:OnOutOfMemoryError=$BASE_DIR/bin/oom.sh"
GC_OPTS=" -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+TraceClassLoading -XX:InitiatingHeapOccupancyPercent=45 -XX:G1HeapRegionSize=16m -XX:G1MixedGCCountTarget=16 -XX:G1HeapWastePercent=10 -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
LOG_OPTS=" -Xloggc:$BASE_DIR/logs/gc.log -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=20M"
if [ -n "$NEED_TRACK_NATIVE_MEMORY" ] && [ "$NEED_TRACK_NATIVE_MEMORY" = "true" ]; then
    GC_OPTS="$GC_OPTS -XX:NativeMemoryTracking"
fi
AGENT_JVM_ARGS="$HEAP_OPTS $GVM_OPTS $GC_OPTS $LOG_OPTS $OOM_HANDLER"
# Add Agent Rmi Args when necessary
AGENT_RMI_ARGS="-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=18080 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
CONFIG_DIR=${BASE_DIR}"/conf/"
JAR_LIBS=${BASE_DIR}"/lib/*"
CLASSPATH=${CONFIG_DIR}:${JAR_LIBS}

JMX_ENABLED=$(grep -c "agent.domainListeners=org.apache.inlong.agent.metrics.AgentJmxMetricListener" $BASE_DIR/conf/agent.properties)
if [[ $JMX_ENABLED == 1 ]]; then
  export AGENT_ARGS="$AGENT_JVM_ARGS $AGENT_RMI_ARGS -cp $CLASSPATH -Dagent.home=$BASE_DIR"
else
  export AGENT_ARGS="$AGENT_JVM_ARGS -cp $CLASSPATH -Dagent.home=$BASE_DIR"
fi

# Opentelemetry java agent path
OTEL_AGENT="${BASE_DIR}/lib/opentelemetry-javaagent-${OTEL_VERSION}.jar"
