@echo off
title Seohak-100 AI Report Generator
echo ============================================================
echo   Seohak-100 AI Market Report Generator
echo   %date% %time%
echo ============================================================
echo.

cd /d "C:\Users\1\OneDrive\Documents\seohak-index"

echo [1/1] Generating AI market report (Gemini API)...
python src/generate_report.py
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] AI report generation failed!
    pause
    exit /b 1
)
echo [1/1] Done
echo.

echo ============================================================
echo   AI Report Generated! %date% %time%
echo ============================================================
echo.
echo   View dashboard: streamlit run src/app.py
echo.
pause
