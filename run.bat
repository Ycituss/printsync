@echo off
cd /d "%~dp0"

REM Auto-install dependency if missing, then launch the local workbench.
python -c "import paho.mqtt.client" 2>nul
if errorlevel 1 (
    echo Installing dependency paho-mqtt ...
    python -m pip install --quiet --disable-pip-version-check paho-mqtt
)

python app.py
pause
