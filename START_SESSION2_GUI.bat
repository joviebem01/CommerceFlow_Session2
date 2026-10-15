@echo off
cd /d "%~dp0"
python session2_gui.py
if errorlevel 1 pause
