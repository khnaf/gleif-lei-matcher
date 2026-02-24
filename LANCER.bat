@echo off
chcp 65001 >nul
title GLEIF LEI Matcher

echo.
echo ============================================================
echo   GLEIF LEI Matcher - Demarrage
echo ============================================================
echo.

:: ── Recherche de Python / Miniforge / Anaconda ───────────────────────────────
set PYTHON_EXE=

:: 1. Python standard dans le PATH systeme
python --version >nul 2>&1
IF NOT ERRORLEVEL 1 (
    set PYTHON_EXE=python
    goto :found
)

:: 2. Miniforge — chemin specifique au poste (ajustez le nom de dossier si besoin)
if exist "%USERPROFILE%\bin\mf-26.1.0-0\inst\python.exe" (
    set PYTHON_EXE=%USERPROFILE%\bin\mf-26.1.0-0\inst\python.exe
    goto :found
)

:: 3. Miniforge — glob generique mf-* dans %USERPROFILE%\bin
for /d %%D in ("%USERPROFILE%\bin\mf-*") do (
    if exist "%%D\inst\python.exe" (
        set PYTHON_EXE=%%D\inst\python.exe
        goto :found
    )
)

:: 4. Emplacements Miniforge / Miniconda / Anaconda courants
for %%P in (
    "%USERPROFILE%\Miniforge3\python.exe"
    "%USERPROFILE%\miniforge3\python.exe"
    "%USERPROFILE%\Miniconda3\python.exe"
    "%USERPROFILE%\miniconda3\python.exe"
    "%USERPROFILE%\Anaconda3\python.exe"
    "%USERPROFILE%\anaconda3\python.exe"
    "%LOCALAPPDATA%\miniforge3\python.exe"
    "%LOCALAPPDATA%\Miniconda3\python.exe"
    "C:\ProgramData\Miniforge3\python.exe"
    "C:\ProgramData\miniforge3\python.exe"
    "C:\ProgramData\Anaconda3\python.exe"
    "%LOCALAPPDATA%\Continuum\anaconda3\python.exe"
    "%PROGRAMFILES%\Anaconda3\python.exe"
) do (
    IF EXIST %%P (
        set PYTHON_EXE=%%P
        goto :found
    )
)

:: 5. Activation conda base en dernier recours
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
echo [ERREUR] Python / Miniforge / Anaconda introuvable.
echo.
echo  Solutions possibles :
echo  1. Verifiez que Miniforge est installe via le catalogue IT
echo  2. Si le dossier bin\mf-* n'est pas dans %USERPROFILE%, editez ce fichier
echo     et mettez a jour le chemin a la ligne "chemin specifique au poste"
echo  3. Contactez votre support informatique
echo.
pause
exit /b 1

:found
:: ── Informations sur le Python detecte ──────────────────────────────────────
echo Python detecte :
%PYTHON_EXE% -c "import sys; b='64-bit' if sys.maxsize>2**32 else '32-bit (ATTENTION: risque memoire)'; print(f'  Executable : {sys.executable}'); print(f'  Version    : {sys.version.split()[0]}  {b}')"
echo.

:: Avertissement si 32-bit (risque OOM sur le CSV GLEIF de 450 Mo)
%PYTHON_EXE% -c "import sys; sys.exit(0 if sys.maxsize>2**32 else 1)" >nul 2>&1
IF ERRORLEVEL 1 (
    echo [ATTENTION] Python 32-bit detecte.
    echo  Le chargement du fichier GLEIF (~450 Mo) peut provoquer une erreur memoire.
    echo  Utilisez de preference la base SLIM dans l'interface.
    echo.
)

:: ── Installation des dependances via CE Python ────────────────────────────────
echo Verification / installation des dependances...
%PYTHON_EXE% -m pip install pandas openpyxl rapidfuzz --quiet --disable-pip-version-check 2>nul
IF ERRORLEVEL 1 (
    echo  [AVERTISSEMENT] pip n'a pas pu verifier les dependances.
    echo  Si l'application ne demarre pas, contactez le support IT.
)
echo.

:: ── Lancement ────────────────────────────────────────────────────────────────
echo Lancement de GLEIF LEI Matcher...
echo.
%PYTHON_EXE% "%~dp0gleif_gui.py"

IF ERRORLEVEL 1 (
    echo.
    echo [ERREUR] L'application a rencontre un probleme.
    echo Transmettez ce message a votre equipe IT.
    echo.
    pause
)
