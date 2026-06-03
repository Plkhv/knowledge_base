@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PS1=%SCRIPT_DIR%start-lakehouse.ps1"

if not exist "%PS1%" (
  echo Script not found: "%PS1%"
  exit /b 1
)

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%PS1%" %*
set "EXITCODE=%ERRORLEVEL%"

if not "%EXITCODE%"=="0" (
  echo.
  echo start-lakehouse failed with exit code %EXITCODE%
)

exit /b %EXITCODE%
