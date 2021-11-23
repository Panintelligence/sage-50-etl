@echo off

pyinstaller -i ./logo.ico -F Sage50_ETL.py
pyinstaller -i ./logo.ico -F Sage50_Connections.py