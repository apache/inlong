REM Windows Startup Script for Master Node
REM please do not change any command or variable in this script, check out
REM env.cmd for details.

setlocal
call "%~dp0env.cmd"

set MASTERMAIN=org.apache.tubemq.server.tools.MasterStartup
set MASTERCFG=%~dp0..\conf\master.ini

echo on
call %JAVA% %MASTER_JVM_OPTS% %GENERIC_ARGS% "%MASTERMAIN%" -f "%MASTERCFG%"
endlocal