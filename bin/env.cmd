REM Licensed to the Apache Software Foundation (ASF) under one or more
REM contributor license agreements.  See the NOTICE file distributed with
REM this work for additional information regarding copyright ownership.
REM The ASF licenses this file to You under the Apache License, Version 2.0
REM (the "License"); you may not use this file except in compliance with
REM the License.  You may obtain a copy of the License at
REM <p>
REM http://www.apache.org/licenses/LICENSE-2.0
REM <p>
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

REM Windows Startup Script about Environment Settings
REM Java runtime evironment could be specified here.

set BASE_DIR=%~dp0..
set CLASSPATH=%BASE_DIR%\lib\*;%BASE_DIR%\tubemq-server\target\*;%CLASSPATH%
set GENERIC_ARGS="-Dtubemq.home=%BASE_DIR%" -cp "%CLASSPATH%" "-Dlog4j.configuration=file:%BASE_DIR%\conf\master.log4j.properties"

REM If there's no system-wide JAVA_HOME or there's need to run on specific Java,
REM please uncomment the following JAVA_HOME line, and specify the java home path.
REM set JAVA_HOME=C:\Program Files\Java\jdk1.8.0_241

set JAVA="%JAVA_HOME%\bin\java"

REM One may add extra Java runtime flags in addition to each role: Master or Broker
set MASTER_JVM_OPTS=-Xmx1g -Xms256m -server
set BROKER_JVM_OPTS=-Xmx1g -Xms512m -server