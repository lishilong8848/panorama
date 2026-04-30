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
set "PYTHON_DESC="
set "PYTHON_HEALTH_PROBE=import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)"

if defined QJPT_PREPARE_PYTHON_EXE (
    set "PYTHON_EXE=%QJPT_PREPARE_PYTHON_EXE%"
    goto verify_explicit_python
)

:try_venv

if exist ".venv\Scripts\python.exe" (
    set "PYTHON_EXE=%CD%\.venv\Scripts\python.exe"
    set "PYTHON_DESC=.venv"
    goto run_script
)

where python >nul 2>nul
if errorlevel 1 goto try_registry_python
python -c "%PYTHON_HEALTH_PROBE%" >nul 2>nul
if errorlevel 1 goto try_registry_python
set "PYTHON_EXE=python"
set "PYTHON_DESC=PATH"
goto run_script

:verify_explicit_python
if not exist "%PYTHON_EXE%" (
    echo [WARN] QJPT_PREPARE_PYTHON_EXE does not exist: %PYTHON_EXE%
    set "PYTHON_EXE="
    goto try_venv
)
"%PYTHON_EXE%" -c "%PYTHON_HEALTH_PROBE%" >nul 2>nul
if errorlevel 1 (
    echo [WARN] QJPT_PREPARE_PYTHON_EXE is not a usable Python 3.11+: %PYTHON_EXE%
    set "PYTHON_EXE="
    goto try_venv
)
set "PYTHON_DESC=QJPT_PREPARE_PYTHON_EXE"
goto run_script

:try_registry_python
for /f "usebackq delims=" %%I in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='SilentlyContinue'; $roots=@('HKCU:\Software\Python\PythonCore','HKLM:\Software\Python\PythonCore','HKLM:\Software\WOW6432Node\Python\PythonCore'); $candidates=@(); foreach($root in $roots){ if(Test-Path $root){ Get-ChildItem $root | Sort-Object PSChildName -Descending | ForEach-Object { $installKey = Join-Path $_.PsPath 'InstallPath'; if(Test-Path $installKey){ $installDir = (Get-ItemProperty -Path $installKey).'(default)'; if($installDir){ $exe = Join-Path $installDir 'python.exe'; if(Test-Path $exe){ $candidates += $exe } } } } } }; $candidates | Select-Object -First 1"` ) do set "PYTHON_EXE=%%~I"
if defined PYTHON_EXE goto verify_registry_python
goto try_common_python

:verify_registry_python
"%PYTHON_EXE%" -c "%PYTHON_HEALTH_PROBE%" >nul 2>nul
if errorlevel 1 (
    set "PYTHON_EXE="
    goto try_common_python
)
set "PYTHON_DESC=registered Python"
goto run_script

:try_common_python
for /f "usebackq delims=" %%I in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='SilentlyContinue'; $candidates=@(); $bases=@(); if($env:USERPROFILE){ $bases += (Join-Path $env:USERPROFILE 'python-sdk') }; if($env:LOCALAPPDATA){ $bases += (Join-Path $env:LOCALAPPDATA 'Programs\Python') }; if($env:ProgramFiles){ $bases += $env:ProgramFiles }; if(${env:ProgramFiles(x86)}){ $bases += ${env:ProgramFiles(x86)} }; $bases += @('C:\Python313','C:\Python312','C:\Python311','D:\Python313','D:\Python312','D:\Python311'); foreach($base in $bases){ if(Test-Path $base){ $direct = Join-Path $base 'python.exe'; if(Test-Path $direct){ $candidates += $direct }; Get-ChildItem -Path $base -Directory -Filter 'python*' | Sort-Object Name -Descending | ForEach-Object { $exe = Join-Path $_.FullName 'python.exe'; if(Test-Path $exe){ $candidates += $exe } } } }; $candidates | Select-Object -First 1"` ) do set "PYTHON_EXE=%%~I"
if defined PYTHON_EXE goto verify_common_python
goto try_py_launcher

:verify_common_python
"%PYTHON_EXE%" -c "%PYTHON_HEALTH_PROBE%" >nul 2>nul
if errorlevel 1 (
    set "PYTHON_EXE="
    goto try_py_launcher
)
set "PYTHON_DESC=detected local Python"
goto run_script

:try_py_launcher
where py >nul 2>nul
if errorlevel 1 goto no_python
py -3 -c "%PYTHON_HEALTH_PROBE%" >nul 2>nul
if errorlevel 1 goto no_python
set "USE_PY_LAUNCHER=1"
goto run_script

:no_python
echo [ERROR] Python runtime not found on this developer machine.
echo [ERROR] Please install Python 3.11+ or create .venv first.
goto fail_exit

:run_script
if defined USE_PY_LAUNCHER goto run_with_py_launcher
echo [INFO] Using Python executable (%PYTHON_DESC%): %PYTHON_EXE%
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
