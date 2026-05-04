@echo off
setlocal EnableExtensions
set "QJPT_STARTUP_SOURCE_DIR=%~dp0"
if not defined QJPT_STARTUP_LOG set "QJPT_STARTUP_LOG=%TEMP%\QJPT_external_startup.log"
> "%QJPT_STARTUP_LOG%" echo [INFO] QJPT external startup invoked at %DATE% %TIME%
>> "%QJPT_STARTUP_LOG%" echo [INFO] Source dir: %QJPT_STARTUP_SOURCE_DIR%
echo [INFO] Startup log: %QJPT_STARTUP_LOG%
pushd "%~dp0" || (
    echo [ERROR] Cannot enter project directory: %~dp0
    >> "%QJPT_STARTUP_LOG%" echo [ERROR] Cannot enter project directory: %~dp0
    if /i not "%QJPT_NO_PAUSE%"=="1" pause
    endlocal & exit /b 1
)
set "QJPT_FORCE_ROLE_MODE=external"
set "QJPT_MAIN_FILE=main_external.py"
set "MONTHLY_REPORT_CONFIG=%CD%\表格计算配置.json"
call "%CD%\_启动基础程序.bat" %*
set "EXIT_CODE=%ERRORLEVEL%"
popd >nul 2>nul
endlocal & exit /b %EXIT_CODE%
