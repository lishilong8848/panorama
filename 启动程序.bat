@echo off
setlocal EnableDelayedExpansion
title QJPT Web Console Log Window
cd /d "%~dp0"
set "PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple"
set "PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn"
set "PYTHON_EXE=%CD%\runtime\python\python.exe"
echo [INFO] Web console log window is open.
echo [INFO] Startup is checking runtime dependencies. First launch may take several minutes.
echo [INFO] Keep this window open. Press Ctrl+C to stop.
if exist "%PYTHON_EXE%" goto run_main
set "PYTHON_EXE=%CD%\.venv\Scripts\python.exe"
if exist "%PYTHON_EXE%" goto run_main
echo [ERROR] Python runtime not found in project folder.
echo [ERROR] Please restore runtime\python or .venv inside the project directory.
pause
exit /b 1
:run_main
"%PYTHON_EXE%" -u "portable_launcher.py" %*
set "EXIT_CODE=%ERRORLEVEL%"
echo.
echo [INFO] Program exited with code %EXIT_CODE%.
pause
endlocal & exit /b %EXIT_CODE%
