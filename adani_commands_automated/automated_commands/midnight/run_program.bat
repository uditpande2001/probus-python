@echo off
rem set path to virtual environment
set VIRTUAL_ENV=D:\python\adani_commands_automated\venv
rem activate virtual environment
call %VIRTUAL_ENV%\Scripts\activate.bat

rem run Python script
python midnight_automate.py

rem deactivate virtual environment
call %VIRTUAL_ENV%\Scripts\deactivate.bat

rem pause script execution to keep the command window open
pause