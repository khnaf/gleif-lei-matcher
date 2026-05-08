@echo off
chcp 65001 >nul
title GLEIF LEI Matcher
cd /d "%~dp0"

echo.
echo ============================================================
echo   GLEIF LEI Matcher - Demarrage
echo ============================================================
echo.

:: ── 0. PRIORITE : environnement portable ────────────────────────────────────
:: Si setup_portable.py a ete execute, on utilise CE Python en priorite,
:: ce qui rend le dossier auto-suffisant (cle USB, repertoire reseau, etc.).
set PYTHON_EXE=
set PORTABLE_MODE=0

if exist "%~dp0.venv_portable\Scripts\python.exe" (
    set PYTHON_EXE=%~dp0.venv_portable\Scripts\python.exe
    set PORTABLE_MODE=1
    echo  [Mode PORTABLE] Python local detecte dans .venv_portable\
    goto :found
)

:: ── 1. Python standard dans le PATH systeme ─────────────────────────────────
python --version >nul 2>&1
IF NOT ERRORLEVEL 1 (
    set PYTHON_EXE=python
    goto :found
)

:: ── 2. Miniforge — chemin specifique au poste ───────────────────────────────
if exist "%USERPROFILE%\bin\mf-26.1.0-0\inst\python.exe" (
    set PYTHON_EXE=%USERPROFILE%\bin\mf-26.1.0-0\inst\python.exe
    goto :found
)

:: ── 3. Miniforge — glob generique mf-* ──────────────────────────────────────
for /d %%D in ("%USERPROFILE%\bin\mf-*") do (
    if exist "%%D\inst\python.exe" (
        set PYTHON_EXE=%%D\inst\python.exe
        goto :found
    )
)

:: ── 4. Emplacements Miniforge / Miniconda / Anaconda courants ───────────────
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

:: ── 5. Activation conda base en dernier recours ─────────────────────────────
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

:: ── Aucun Python trouve ─────────────────────────────────────────────────────
echo [ERREUR] Python introuvable.
echo.
echo  Solutions possibles :
echo  1. Mode PORTABLE recommande : sur un poste avec Python, executez
echo     "python setup_portable.py" pour creer le bundle .venv_portable\,
echo     puis copiez le dossier complet sur ce poste.
echo  2. Installer Miniforge via le catalogue IT.
echo  3. Contacter votre support informatique.
echo.
pause
exit /b 1

:found
:: ── Informations sur le Python detecte ──────────────────────────────────────
echo Python detecte :
"%PYTHON_EXE%" -c "import sys; b='64-bit' if sys.maxsize>2**32 else '32-bit (ATTENTION: risque memoire)'; print(f'  Executable : {sys.executable}'); print(f'  Version    : {sys.version.split()[0]}  {b}')"
echo.

:: Avertissement si 32-bit
"%PYTHON_EXE%" -c "import sys; sys.exit(0 if sys.maxsize>2**32 else 1)" >nul 2>&1
IF ERRORLEVEL 1 (
    echo [ATTENTION] Python 32-bit detecte.
    echo  Le chargement du fichier GLEIF (~450 Mo) peut provoquer une erreur memoire.
    echo  Utilisez de preference la base SLIM dans l'interface.
    echo.
)

:: ── Installation des dependances (sauf en mode portable : deja preinstallees) ─
IF "%PORTABLE_MODE%"=="1" (
    echo  [Mode PORTABLE] Dependances pre-installees dans le venv local.
    echo.
) ELSE (
    echo Verification / installation des dependances...
    "%PYTHON_EXE%" -m pip install pandas openpyxl rapidfuzz customtkinter --quiet --disable-pip-version-check 2>nul
    IF ERRORLEVEL 1 (
        echo  [AVERTISSEMENT] pip n'a pas pu verifier les dependances.
        echo  Si l'application ne demarre pas, contactez le support IT.
    )
    echo.
)

:: ── Lancement ────────────────────────────────────────────────────────────────
echo Lancement de GLEIF LEI Matcher...
echo.
"%PYTHON_EXE%" "%~dp0gleif_gui.py"

IF ERRORLEVEL 1 (
    echo.
    echo [ERREUR] L'application a rencontre un probleme.
    echo Transmettez ce message a votre equipe IT.
    echo.
    pause
)
