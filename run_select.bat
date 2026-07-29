@echo off
setlocal
cd /d "%~dp0"
set "PYTHON_EXE=%LocalAppData%\Programs\Python\Python313\python.exe"
if not exist "%PYTHON_EXE%" set "PYTHON_EXE=python"
call "%PYTHON_EXE%" daily_run.py
set "EXIT_CODE=%ERRORLEVEL%"
exit /b %EXIT_CODE%
