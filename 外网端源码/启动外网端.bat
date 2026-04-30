@echo off
setlocal EnableExtensions
cd /d "%~dp0"
set "QJPT_FORCE_ROLE_MODE=external"
set "QJPT_MAIN_FILE=main_external.py"
call "%~dp0_启动基础程序.bat" %*
endlocal & exit /b %ERRORLEVEL%
