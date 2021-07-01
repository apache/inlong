#! /bin/sh

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# <p>
# http://www.apache.org/licenses/LICENSE-2.0
# <p>
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#======================================================================
# Project start shell script
# config directory: configuration file directory
# log directory: project operation log directory
# log/startup.log: record startup log
# log/back directory: Project running log backup directory
# nohup background process
#
#======================================================================

# Project name
APPLICATION="inlong-manager-openapi"

# Project startup jar package name
APPLICATION_JAR="manager-openapi.jar"

JAVA_HOME=
export PATH=$PATH:$JAVA_HOME/bin

# Absolute path of bin directory
BIN_PATH=$(cd $(dirname $0); pwd)

# Enter the root directory path
cd "$BIN_PATH"
cd ../

# Print the absolute path of the project root directory
BASE_PATH=$(pwd)

# The absolute directory of the external configuration file, if the directory needs / end, you can also directly specify the file
# If you specify a directory, spring will read all configuration files in the directory
CONFIG_DIR=${BASE_PATH}"/conf/"
JAR_LIBS=${BASE_PATH}"/lib/*"
JAR_MAIN=${BASE_PATH}"/lib/"${APPLICATION_JAR}
CLASSPATH=${CONFIG_DIR}:${JAR_LIBS}:${JAR_MAIN}
MAIN_CLASS=org.apache.inlong.manager.openapi.InLongOpenApiApplication

# Project log output absolute path
LOG_DIR=${BASE_PATH}"/log"
LOG_FILE="${LOG_DIR}/${APPLICATION}_sout.log"
# Log backup directory
LOG_BACK_DIR="${LOG_DIR}/back/"

# Project startup log output absolute path
LOG_STARTUP_PATH="${LOG_DIR}/startup.log"

# current time
NOW=$(date +'%Y-%m-%m-%H-%M-%S')
NOW_PRETTY=$(date +'%Y-%m-%m %H:%M:%S')

# Startup log
STARTUP_LOG="================================================ ${NOW_PRETTY} ================================================\n"

# If the logs folder does not exist, create the folder
if [[ ! -d "${LOG_DIR}" ]]; then
  mkdir "${LOG_DIR}"
fi

# If the log/back folder does not exist, create a folder
if [[ ! -d "${LOG_BACK_DIR}" ]]; then
  mkdir "${LOG_BACK_DIR}"
fi

# If the project log exists, rename the backup
if [[ -f "${LOG_FILE}" ]]; then
  mv ${LOG_FILE} "${LOG_BACK_DIR}/${APPLICATION}_back_${NOW}.log"
fi

# Create a new project run log
echo "" >${LOG_FILE}

# JVM Configuration
JAVA_OPT="-server -Xms2g -Xmx2g -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=256m -XX:-OmitStackTraceInFastThrow "

#gc options
JAVA_OPT="${JAVA_OPT} -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:${LOG_DIR}/gc.log"

#jmx metrics
#JAVA_OPT="${JAVA_OPT} -Dcom.sun.management.jmxremote  -Dcom.sun.management.jmxremote.port=8011  -Dcom.sun.management.jmxremote.ssl=false  -Dcom.sun.management.jmxremote.authenticate=false"

#remote debugger
#JAVA_OPT="${JAVA_OPT} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8081"

#=======================================================
# Append command startup related logs to the log file
#=======================================================

# Output project name
STARTUP_LOG="${STARTUP_LOG}application name: ${APPLICATION}\n"
# Output jar package name
STARTUP_LOG="${STARTUP_LOG}application jar name: ${APPLICATION_JAR}\n"
# Output project root directory
STARTUP_LOG="${STARTUP_LOG}application root path: ${BASE_PATH}\n"
# Output project bin path
STARTUP_LOG="${STARTUP_LOG}application bin path: ${BIN_PATH}\n"
# Output project config path
STARTUP_LOG="${STARTUP_LOG}application config path: ${CONFIG_DIR}\n"
# Print log path
STARTUP_LOG="${STARTUP_LOG}application log path: ${LOG_DIR}\n"
# Print JVM configuration
STARTUP_LOG="${STARTUP_LOG}application JAVA_OPT: ${JAVA_OPT}\n"

# Print start command
STARTUP_LOG="${STARTUP_LOG}application startup command: nohup java ${JAVA_OPT} -cp ${CLASSPATH} ${MAIN_CLASS} 1>${LOG_FILE} 2>${LOG_DIR}/error.log &\n"

#======================================================================
# Execute the startup command: start the project in the background, and output the log to the logs folder under the project root directory
#======================================================================
nohup java ${JAVA_OPT} -cp ${CLASSPATH} ${MAIN_CLASS} 1>${LOG_FILE} 2>${LOG_DIR}/error.log &

# Process ID
PID="$!"
STARTUP_LOG="${STARTUP_LOG}application pid: ${PID}\n"

# The startup log is appended to the startup log file
echo -e ${STARTUP_LOG} >>${LOG_STARTUP_PATH}
# Print startup log
echo -e ${STARTUP_LOG}
