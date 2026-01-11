@echo off
cd /d %~dp0
python main.py >> scanner_runtime.log 2>&1
