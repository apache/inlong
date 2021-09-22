REM Licensed to the Apache Software Foundation (ASF) under one
REM or more contributor license agreements.  See the NOTICE file
REM distributed with this work for additional information
REM regarding copyright ownership.  The ASF licenses this file
REM to you under the Apache License, Version 2.0 (the
REM "License"); you may not use this file except in compliance
REM with the License.  You may obtain a copy of the License at
REM
REM   http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing,
REM software distributed under the License is distributed on an
REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
REM KIND, either express or implied.  See the License for the
REM specific language governing permissions and limitations
REM under the License.

REM Windows Startup Script for Master Node
REM please do not change any command or variable in this script, check out
REM env.cmd for details.

setlocal
call "%~dp0env.cmd"

set MASTERMAIN=org.apache.inlong.tubemq.server.tools.MasterStartup
set MASTERCFG=%~dp0..\conf\master.ini

echo on
call %JAVA% %MASTER_JVM_OPTS% %GENERIC_ARGS% "%MASTERMAIN%" -f "%MASTERCFG%"
endlocal