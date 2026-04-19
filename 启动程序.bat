@echo off
setlocal EnableExtensions EnableDelayedExpansion
title QJPT Web Console Log Window
cd /d "%~dp0"
chcp 65001 >nul 2>nul

set "PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple"
set "PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn"
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
set "NODE_NO_WARNINGS=1"

echo [INFO] Web console log window is open.
echo [INFO] Source mode expects you to update code with git pull before restart.
echo [INFO] Keep this window open. Press Ctrl+C to stop.

if not exist "main.py" (
    echo [ERROR] main.py not found in project root: %CD%
    pause
    exit /b 1
)

if not exist "portable_launcher.py" (
    echo [ERROR] portable_launcher.py not found in project root: %CD%
    pause
    exit /b 1
)

set "PYTHON_EXE="
set "USE_PY_LAUNCHER="

if exist ".venv\Scripts\python.exe" (
    set "PYTHON_EXE=%CD%\.venv\Scripts\python.exe"
    goto run_main
)

if exist "runtime\python\python.exe" (
    set "PYTHON_EXE=%CD%\runtime\python\python.exe"
    goto run_main
)

where py >nul 2>nul
if not errorlevel 1 (
    set "USE_PY_LAUNCHER=1"
    goto run_main
)

where python >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_EXE=python"
    goto run_main
)

echo [ERROR] Python runtime not found.
echo [ERROR] Please install Python 3 and ensure it is available, or create .venv in the project directory.
pause
exit /b 1

:run_main
if defined USE_PY_LAUNCHER (
    echo [INFO] Using Python launcher: py -3
    py -3 -u "portable_launcher.py" %*
) else (
    echo [INFO] Using Python executable: %PYTHON_EXE%
    "%PYTHON_EXE%" -u "portable_launcher.py" %*
)

set "EXIT_CODE=%ERRORLEVEL%"
echo.
echo [INFO] Program exited with code %EXIT_CODE%.
pause
endlocal & exit /b %EXIT_CODE%
