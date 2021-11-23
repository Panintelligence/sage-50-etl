@echo off

pip install -r requirements.txt
pyinstaller -i ./logo.ico -F Sage50_ETL.py
pyinstaller -i ./logo.ico -F Sage50_Connections.py