@echo off
chcp 65001 >nul
title GLEIF LEI Matcher

echo.
echo ============================================================
echo   GLEIF LEI Matcher - Demarrage
echo ============================================================
echo.

:: ── Recherche de Python (standard puis Anaconda/conda) ──────────────────────
set PYTHON_EXE=

:: 1. Python standard dans le PATH
python --version >nul 2>&1
IF NOT ERRORLEVEL 1 (
    set PYTHON_EXE=python
    goto :found
)

:: 2. Anaconda - emplacements courants Windows
for %%P in (
    "%USERPROFILE%\Anaconda3\python.exe"
    "%USERPROFILE%\anaconda3\python.exe"
    "%USERPROFILE%\Miniconda3\python.exe"
    "%USERPROFILE%\miniconda3\python.exe"
    "C:\ProgramData\Anaconda3\python.exe"
    "C:\ProgramData\anaconda3\python.exe"
    "C:\ProgramData\Miniconda3\python.exe"
    "%LOCALAPPDATA%\Continuum\anaconda3\python.exe"
    "%PROGRAMFILES%\Anaconda3\python.exe"
    "%PROGRAMFILES(X86)%\Anaconda3\python.exe"
) do (
    IF EXIST %%P (
        set PYTHON_EXE=%%P
        goto :found
    )
)

:: 3. conda activate base en dernier recours
where conda >nul 2>&1
IF NOT ERRORLEVEL 1 (
    echo Activation de l'environnement conda base...
    call conda activate base >nul 2>&1
    python --version >nul 2>&1
    IF NOT ERRORLEVEL 1 (
        set PYTHON_EXE=python
        goto :found
    )
)

:: Aucun Python trouve
echo [ERREUR] Python / Anaconda introuvable sur ce poste.
echo.
echo  Solutions :
echo  - Verifiez qu'Anaconda est bien installe via le catalogue IT
echo  - Contactez votre support informatique
echo.
pause
exit /b 1

:found
echo Python detecte : %PYTHON_EXE%
echo.

:: ── Installation des dependances (silencieux si deja presentes) ─────────────
echo Verification des dependances...
%PYTHON_EXE% -m pip install pandas openpyxl rapidfuzz --quiet --disable-pip-version-check 2>nul
IF ERRORLEVEL 1 (
    echo  [AVERTISSEMENT] Impossible d'installer les dependances.
    echo  Si l'application ne demarre pas, contactez le support IT.
)

:: ── Lancement ────────────────────────────────────────────────────────────────
echo Lancement de GLEIF LEI Matcher...
echo.
%PYTHON_EXE% "%~dp0gleif_gui.py"

IF ERRORLEVEL 1 (
    echo.
    echo [ERREUR] L'application a rencontre un probleme au demarrage.
    echo Transmettez ce message a votre equipe IT ou au gestionnaire du projet.
    echo.
    pause
)
