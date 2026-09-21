@echo off
if "%~1"=="" (
    echo Usage: deploy_worker.bat ^<your_github_repo_url^>
    echo Example: deploy_worker.bat https://github.com/myuser/spt-whatsapp-worker.git
    exit /b 1
)

echo [1/3] Adding Git Remote...
git remote remove origin 2>nul
git remote add origin %~1

echo [2/3] Setting Main Branch...
git branch -M main

echo [3/3] Pushing to Remote Repository...
git push -u origin main

echo.
echo [DONE] Repository successfully pushed to GitHub!
echo Now you can connect it to Render.com.
