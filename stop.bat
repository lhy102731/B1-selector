@echo off
REM One-click STOP for AutonomousRunnerV1. Double-click this file.
REM Creates the STOP sentinel; the runner exits cleanly after its current experiment.
set "STOPDIR=%~dp0research_automation\_output"
if not exist "%STOPDIR%" mkdir "%STOPDIR%"
type nul > "%STOPDIR%\STOP"
echo STOP requested. The running research cycle will finish the current experiment and exit.
echo (sentinel: %STOPDIR%\STOP)
pause
