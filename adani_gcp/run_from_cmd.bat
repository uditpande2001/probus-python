@echo off
rem set path to virtual environment
set VIRTUAL_ENV=D:\python\adani_gcp\venv
rem activate virtual environment
call %VIRTUAL_ENV%\Scripts\activate.bat

rem run Python script
python aditya_sir_script.py

rem deactivate virtual environment
call %VIRTUAL_ENV%\Scripts\deactivate.bat

rem pause script execution to keep the command window open
pause