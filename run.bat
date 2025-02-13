rem @echo off
cd  %~dp0%
conda activate data ^
  && python -m bot.chat_app
pause
