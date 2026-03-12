@echo off
chcp 65001 >nul 2>&1
title OCR Pipeline — Только сверка с БД

echo ============================================
echo   OCR Pipeline — Сверка с БД (без OCR)
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

%PYTHON% "%~dp0run_pipeline.py" --skip-ocr
set "EXITCODE=%errorlevel%"

echo.
echo ============================================

if %EXITCODE% neq 0 (
    echo [ОШИБКА] Сверка завершилась с ошибкой (код %EXITCODE%).
    echo Проверьте лог выше.
    goto :done
)

set "RESULT=%~dp0clients_database.xlsx"

if exist "%RESULT%" (
    echo [ГОТОВО] Результат: %RESULT%
    echo Открываю файл...
    start "" "%RESULT%"
) else (
    echo [ВНИМАНИЕ] Сверка завершилась успешно, но файл не найден:
    echo %RESULT%
)

:done
echo.
echo Нажмите любую клавишу для закрытия окна...
pause >nul
