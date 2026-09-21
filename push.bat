@echo off
title Push SPT WhatsApp Worker to GitHub
cd /d "c:\Users\USER\Downloads\SPT\server\whatsapp-worker"
echo ========================================================
echo  Pushing SPT Cloud WhatsApp Gateway to GitHub
echo  Repository: https://github.com/ArunPvsilk/spt-whatsapp-worker.git
echo ========================================================
echo.
git push -u origin main
echo.
if %ERRORLEVEL% equ 0 (
    echo [SUCCESS] Code pushed successfully to GitHub!
) else (
    echo [ERROR] Push failed. Please verify credentials.
)
echo.
pause
