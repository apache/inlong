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