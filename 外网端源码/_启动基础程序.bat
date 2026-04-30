@echo off
setlocal EnableExtensions
title QJPT Web Console Log Window
pushd "%~dp0" || (
    echo [ERROR] Cannot enter project directory: %~dp0
    if defined QJPT_STARTUP_LOG echo [ERROR] Cannot enter project directory: %~dp0>> "%QJPT_STARTUP_LOG%"
    if /i not "%QJPT_NO_PAUSE%"=="1" pause
    endlocal & exit /b 1
)
chcp 65001 >nul 2>nul
if defined QJPT_STARTUP_LOG echo [INFO] Base launcher cwd: %CD%>> "%QJPT_STARTUP_LOG%"

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
set "PYTHON_HEALTH_PROBE=import encodings, json, sqlite3, ssl, sys"
set "EMBEDDED_RUNTIME_PRESENT="
set "EMBEDDED_RUNTIME_BROKEN="
set "RUNTIME_REPAIR_ATTEMPTED="

if defined QJPT_PYTHON_EXE (
    set "PYTHON_EXE=%QJPT_PYTHON_EXE%"
    goto verify_explicit_python
)
if defined QJPT_PREPARE_PYTHON_EXE (
    set "PYTHON_EXE=%QJPT_PREPARE_PYTHON_EXE%"
    goto verify_explicit_python
)

:try_venv
if exist ".venv\Scripts\python.exe" (
    "%CD%\.venv\Scripts\python.exe" -c "%PYTHON_HEALTH_PROBE%" >nul 2>nul
    if not errorlevel 1 (
        set "PYTHON_EXE=%CD%\.venv\Scripts\python.exe"
        set "PYTHON_DESC=project .venv"
        goto run_main
    )
    echo [WARN] Ignoring broken project .venv Python: %CD%\.venv\Scripts\python.exe
)

if exist "runtime\python\python.exe" (
    set "EMBEDDED_RUNTIME_PRESENT=1"
    "%CD%\runtime\python\python.exe" -c "%PYTHON_HEALTH_PROBE%" >nul 2>nul
    if not errorlevel 1 (
        set "PYTHON_EXE=%CD%\runtime\python\python.exe"
        set "PYTHON_DESC=embedded runtime"
        goto run_main
    )
    set "EMBEDDED_RUNTIME_BROKEN=1"
    echo [WARN] Detected incomplete embedded runtime: %CD%\runtime\python\python.exe
    echo [WARN] Startup will skip runtime\python and continue searching for a healthy Python runtime.
)

where python >nul 2>nul
if errorlevel 1 goto try_registry_python
python -c "%PYTHON_HEALTH_PROBE%" >nul 2>nul
if errorlevel 1 goto try_registry_python
set "PYTHON_EXE=python"
set "PYTHON_DESC=python on PATH"
goto run_main

:verify_explicit_python
if not exist "%PYTHON_EXE%" (
    echo [WARN] Explicit Python path does not exist: %PYTHON_EXE%
    set "PYTHON_EXE="
    goto try_venv
)
"%PYTHON_EXE%" -c "%PYTHON_HEALTH_PROBE%" >nul 2>nul
if errorlevel 1 (
    echo [WARN] Explicit Python path is not a healthy Python runtime: %PYTHON_EXE%
    set "PYTHON_EXE="
    goto try_venv
)
set "PYTHON_DESC=explicit Python path"
goto run_main

:try_registry_python
for /f "usebackq delims=" %%I in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='SilentlyContinue'; $roots=@('HKCU:\\Software\\Python\\PythonCore','HKLM:\\Software\\Python\\PythonCore','HKLM:\\Software\\WOW6432Node\\Python\\PythonCore'); $candidates=@(); foreach($root in $roots){ if(Test-Path $root){ Get-ChildItem $root | ForEach-Object { $installKey = Join-Path $_.PsPath 'InstallPath'; if(Test-Path $installKey){ $installDir = (Get-ItemProperty -Path $installKey).'(default)'; if($installDir){ $exe = Join-Path $installDir 'python.exe'; if(Test-Path $exe){ $candidates += $exe } } } } } }; $candidates | Select-Object -First 1"` ) do set "PYTHON_EXE=%%~I"
if defined PYTHON_EXE goto verify_registry_python
goto try_common_python

:verify_registry_python
"%PYTHON_EXE%" -c "%PYTHON_HEALTH_PROBE%" >nul 2>nul
if errorlevel 1 (
    set "PYTHON_EXE="
    goto try_common_python
)
set "PYTHON_DESC=registered Python"
goto run_main

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
goto run_main

:try_py_launcher
where py >nul 2>nul
if errorlevel 1 goto try_repair_embedded_runtime
py -3 -c "%PYTHON_HEALTH_PROBE%" >nul 2>nul
if errorlevel 1 goto try_repair_embedded_runtime
set "USE_PY_LAUNCHER=1"
set "PYTHON_DESC=py launcher"
goto run_main

:try_repair_embedded_runtime
if defined RUNTIME_REPAIR_ATTEMPTED goto no_python
set "RUNTIME_REPAIR_ATTEMPTED=1"
if not exist "scripts\repair_embedded_runtime.ps1" goto no_python
echo [INFO] No healthy Python runtime found. Attempting to repair runtime\python automatically...
powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\repair_embedded_runtime.ps1" -ProjectRoot "%CD%" -TargetRoot "%CD%\runtime\python" -PythonVersion "3.11.9"
if errorlevel 1 (
    echo [ERROR] Automatic runtime repair failed.
    goto no_python
)
if exist "runtime\python\python.exe" (
    "runtime\python\python.exe" -c "%PYTHON_HEALTH_PROBE%" >nul 2>nul
    if not errorlevel 1 (
        set "PYTHON_EXE=%CD%\runtime\python\python.exe"
        set "PYTHON_DESC=repaired embedded runtime"
        set "EMBEDDED_RUNTIME_PRESENT=1"
        set "EMBEDDED_RUNTIME_BROKEN="
        goto run_main
    )
)
goto no_python

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
echo [ERROR] You can also set QJPT_PYTHON_EXE to the full python.exe path before launching.
if defined EMBEDDED_RUNTIME_BROKEN echo [ERROR] Detected runtime\python, but it is incomplete and cannot import standard library modules.
echo [ERROR] Current fallback order: QJPT_PYTHON_EXE ^> .venv ^> runtime\python ^> python(PATH) ^> registered Python ^> common install paths ^> py -3
goto fail_exit

:run_main
if defined USE_PY_LAUNCHER goto run_with_py_launcher
echo [INFO] Using Python executable (%PYTHON_DESC%): %PYTHON_EXE%
if defined EMBEDDED_RUNTIME_BROKEN (
    echo [INFO] Startup skipped a broken runtime\python and will use the healthy Python above.
    echo [INFO] Missing startup dependencies will be checked and installed automatically before the web console starts.
)
"%PYTHON_EXE%" -u "portable_launcher.py" %*
goto finish

:run_with_py_launcher
echo [INFO] Using Python launcher: py -3
if defined EMBEDDED_RUNTIME_BROKEN (
    echo [INFO] Startup skipped a broken runtime\python and will use the healthy Python resolved by py -3.
    echo [INFO] Missing startup dependencies will be checked and installed automatically before the web console starts.
)
py -3 -u "portable_launcher.py" %*
goto finish

:finish
set "EXIT_CODE=%ERRORLEVEL%"
echo.
echo [INFO] Program exited with code %EXIT_CODE%.
if defined QJPT_STARTUP_LOG echo [INFO] Program exited with code %EXIT_CODE%.>> "%QJPT_STARTUP_LOG%"
if /i not "%QJPT_NO_PAUSE%"=="1" pause
popd >nul 2>nul
endlocal & exit /b %EXIT_CODE%

:fail_exit
if defined QJPT_STARTUP_LOG echo [ERROR] Startup failed.>> "%QJPT_STARTUP_LOG%"
if /i not "%QJPT_NO_PAUSE%"=="1" pause
popd >nul 2>nul
endlocal & exit /b 1
