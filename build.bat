@echo off

pip install -r requirements.txt
pyinstaller --hidden-import uuid -i ./logo.ico -F Sage50_ETL.py
pyinstaller --hidden-import uuid -i ./logo.ico -F Sage50_Connections.py