@echo off
setlocal EnableExtensions
cd /d "%~dp0"
chcp 65001 >nul 2>nul

echo [INFO] Preparing bundled Python runtime under runtime\python
echo [INFO] This is for developer machines before sending the project folder to users.

if not exist "scripts\prepare_runtime_python.py" (
    echo [ERROR] scripts\prepare_runtime_python.py not found.
    goto fail_exit
)

set "PYTHON_EXE="
set "USE_PY_LAUNCHER="

if exist ".venv\Scripts\python.exe" (
    set "PYTHON_EXE=%CD%\.venv\Scripts\python.exe"
    goto run_script
)

where python >nul 2>nul
if errorlevel 1 goto try_py_launcher
python -c "import sys" >nul 2>nul
if errorlevel 1 goto try_py_launcher
set "PYTHON_EXE=python"
goto run_script

:try_py_launcher
where py >nul 2>nul
if errorlevel 1 goto no_python
py -3 -c "import sys" >nul 2>nul
if errorlevel 1 goto no_python
set "USE_PY_LAUNCHER=1"
goto run_script

:no_python
echo [ERROR] Python runtime not found on this developer machine.
echo [ERROR] Please install Python 3.11+ or create .venv first.
goto fail_exit

:run_script
if defined USE_PY_LAUNCHER goto run_with_py_launcher
echo [INFO] Using Python executable: %PYTHON_EXE%
"%PYTHON_EXE%" -u "scripts\prepare_runtime_python.py" --clear %*
goto finish

:run_with_py_launcher
echo [INFO] Using Python launcher: py -3
py -3 -u "scripts\prepare_runtime_python.py" --clear %*
goto finish

:finish
set "EXIT_CODE=%ERRORLEVEL%"
echo.
echo [INFO] Runtime prepare exited with code %EXIT_CODE%.
if /i not "%QJPT_NO_PAUSE%"=="1" pause
endlocal & exit /b %EXIT_CODE%

:fail_exit
if /i not "%QJPT_NO_PAUSE%"=="1" pause
endlocal & exit /b 1
