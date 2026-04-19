@echo off
setlocal EnableExtensions
title QJPT Web Console Log Window
cd /d "%~dp0"
chcp 65001 >nul 2>nul

set "PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple"
set "PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn"
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
set "NODE_NO_WARNINGS=1"

echo [INFO] Web console log window is open.
echo [INFO] Source mode will not auto-pull code at startup.
echo [INFO] Use the console button to pull code, or run git pull manually before restart.
echo [INFO] Keep this window open. Press Ctrl+C to stop.

if not exist "main.py" goto missing_main
if not exist "portable_launcher.py" goto missing_launcher

set "PYTHON_EXE="
set "PYTHON_DESC="
set "USE_PY_LAUNCHER="

if exist ".venv\Scripts\python.exe" (
    set "PYTHON_EXE=%CD%\.venv\Scripts\python.exe"
    set "PYTHON_DESC=project .venv"
    goto run_main
)

if exist "runtime\python\python.exe" (
    set "PYTHON_EXE=%CD%\runtime\python\python.exe"
    set "PYTHON_DESC=embedded runtime"
    goto run_main
)

where python >nul 2>nul
if errorlevel 1 goto try_registry_python
python -c "import sys" >nul 2>nul
if errorlevel 1 goto try_registry_python
set "PYTHON_EXE=python"
set "PYTHON_DESC=python on PATH"
goto run_main

:try_registry_python
for /f "usebackq delims=" %%I in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='SilentlyContinue'; $roots=@('HKCU:\\Software\\Python\\PythonCore','HKLM:\\Software\\Python\\PythonCore','HKLM:\\Software\\WOW6432Node\\Python\\PythonCore'); $candidates=@(); foreach($root in $roots){ if(Test-Path $root){ Get-ChildItem $root | ForEach-Object { $installKey = Join-Path $_.PsPath 'InstallPath'; if(Test-Path $installKey){ $installDir = (Get-ItemProperty -Path $installKey).'(default)'; if($installDir){ $exe = Join-Path $installDir 'python.exe'; if(Test-Path $exe){ $candidates += $exe } } } } } }; $candidates | Select-Object -First 1"` ) do set "PYTHON_EXE=%%~I"
if defined PYTHON_EXE goto verify_registry_python
goto try_common_python

:verify_registry_python
"%PYTHON_EXE%" -c "import sys" >nul 2>nul
if errorlevel 1 (
    set "PYTHON_EXE="
    goto try_common_python
)
set "PYTHON_DESC=registered Python"
goto run_main

:try_common_python
for /f "usebackq delims=" %%I in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='SilentlyContinue'; $candidates=@(); $bases=@($env:LOCALAPPDATA + '\\Programs\\Python','C:\\Python313','C:\\Python312','C:\\Python311','D:\\Python313','D:\\Python312','D:\\Python311'); foreach($base in $bases){ if(Test-Path $base){ $direct = Join-Path $base 'python.exe'; if(Test-Path $direct){ $candidates += $direct }; Get-ChildItem -Path $base -Directory | ForEach-Object { $exe = Join-Path $_.FullName 'python.exe'; if(Test-Path $exe){ $candidates += $exe } } } }; $candidates | Select-Object -First 1"` ) do set "PYTHON_EXE=%%~I"
if defined PYTHON_EXE goto verify_common_python
goto try_py_launcher

:verify_common_python
"%PYTHON_EXE%" -c "import sys" >nul 2>nul
if errorlevel 1 (
    set "PYTHON_EXE="
    goto try_py_launcher
)
set "PYTHON_DESC=detected local Python"
goto run_main

:try_py_launcher
where py >nul 2>nul
if errorlevel 1 goto no_python
py -3 -c "import sys" >nul 2>nul
if errorlevel 1 goto no_python
set "USE_PY_LAUNCHER=1"
set "PYTHON_DESC=py launcher"
goto run_main

:missing_main
echo [ERROR] main.py not found in project root: %CD%
goto fail_exit

:missing_launcher
echo [ERROR] portable_launcher.py not found in project root: %CD%
goto fail_exit

:no_python
echo [ERROR] Python runtime not found.
echo [ERROR] This source-run version requires a local Python runtime or bundled runtime\python.
echo [ERROR] If you are preparing a delivery folder, run the runtime-prepare BAT on the developer machine first.
echo [ERROR] Otherwise install Python 3.11+ on this computer, or create .venv in the project directory.
echo [ERROR] Current fallback order: .venv ^> runtime\python ^> python(PATH) ^> registered Python ^> common install paths ^> py -3
goto fail_exit

:run_main
if defined USE_PY_LAUNCHER goto run_with_py_launcher
echo [INFO] Using Python executable (%PYTHON_DESC%): %PYTHON_EXE%
"%PYTHON_EXE%" -u "portable_launcher.py" %*
goto finish

:run_with_py_launcher
echo [INFO] Using Python launcher: py -3
py -3 -u "portable_launcher.py" %*
goto finish

:finish
set "EXIT_CODE=%ERRORLEVEL%"
echo.
echo [INFO] Program exited with code %EXIT_CODE%.
if /i not "%QJPT_NO_PAUSE%"=="1" pause
endlocal & exit /b %EXIT_CODE%

:fail_exit
if /i not "%QJPT_NO_PAUSE%"=="1" pause
endlocal & exit /b 1
