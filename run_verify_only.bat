@echo off
chcp 65001 >nul 2>&1
title OCR Pipeline
echo ============================================
echo   OCR Pipeline — Sverka s BD
echo ============================================
echo.
py -3 "%~dp0run_pipeline.py" --skip-ocr
echo.
echo ============================================
if exist "%~dp0clients_database.xlsx" start "" "%~dp0clients_database.xlsx"
echo.
pause
