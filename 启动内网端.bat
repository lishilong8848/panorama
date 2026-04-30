@echo off
setlocal EnableExtensions
cd /d "%~dp0"
call "%~dp0内网端源码\启动程序.bat" %*
endlocal & exit /b %ERRORLEVEL%
