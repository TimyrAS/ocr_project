@echo off
chcp 65001 >nul 2>&1
title OCR Pipeline

echo ============================================
echo   OCR Pipeline — Оцифровка клиентских карточек
echo ============================================
echo.

:: --- Поиск Python ---
py -3 -V >nul 2>&1
if %errorlevel%==0 (
    set "PYTHON=py -3"
    goto :run
)

python -V >nul 2>&1
if %errorlevel%==0 (
    set "PYTHON=python"
    goto :run
)

echo [ОШИБКА] Python не найден.
echo Установите Python 3 с https://www.python.org/downloads/
echo При установке отметьте "Add Python to PATH".
echo.
pause
exit /b 1

:run
echo Используется: %PYTHON%
echo.

%PYTHON% "%~dp0run_pipeline.py"
set "EXITCODE=%errorlevel%"

echo.
echo ============================================

if %EXITCODE% neq 0 (
    echo [ОШИБКА] Пайплайн завершился с ошибкой (код %EXITCODE%).
    echo Проверьте лог выше.
    echo.
    pause
    exit /b %EXITCODE%
)

set "RESULT=%~dp0clients_database.xlsx"

if exist "%RESULT%" (
    echo [ГОТОВО] Результат: %RESULT%
    echo Открываю файл...
    start "" "%RESULT%"
) else (
    echo [ВНИМАНИЕ] Пайплайн завершился успешно, но файл не найден:
    echo %RESULT%
)

echo.
pause
