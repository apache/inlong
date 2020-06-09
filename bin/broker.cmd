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

REM Windows Startup Script for Broker Node
REM please do not change any command or variable in this script, check out
REM env.cmd for details.

setlocal
call "%~dp0env.cmd"

set BROKERMAIN=org.apache.tubemq.server.tools.BrokerStartup
set BROKERCFG=%~dp0../conf/broker.ini

echo on
call %JAVA% %BROKER_JVM_OPTS% %GENERIC_ARGS% "%BROKERMAIN%" -f "%BROKERCFG%"
endlocal