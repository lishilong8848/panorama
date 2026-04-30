@echo off
setlocal EnableExtensions
cd /d "%~dp0"
set "QJPT_FORCE_ROLE_MODE=internal"
set "QJPT_MAIN_FILE=main_internal.py"
call "%~dp0_启动基础程序.bat" %*
endlocal & exit /b %ERRORLEVEL%
