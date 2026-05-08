"""
setup_portable.py
=================
Construit un environnement Python portable dans `.venv_portable/` à côté du
projet, prêt à être copié sur clé USB ou répertoire réseau et exécuté sur
n'importe quel poste Windows SANS installation de Python.

Usage :
    python setup_portable.py             # construction standard
    python setup_portable.py --rebuild   # supprime et reconstruit le venv
    python setup_portable.py --clean     # supprime aussi les __pycache__/

═══════════════════════════════════════════════════════════════════════════════
IMPORTANT — Compatibilité plateforme
═══════════════════════════════════════════════════════════════════════════════
Un environnement virtuel Python est SPÉCIFIQUE à la plateforme et à
l'architecture qui l'a créé. Pour produire un bundle distribuable aux postes
Windows SG, exécutez ce script SUR UN POSTE WINDOWS 64-bit. Un venv généré
sur macOS/Linux ne fonctionnera pas sur Windows.

Procédure de déploiement type :
  1. Sur un poste Windows : `python setup_portable.py`
  2. Vérifier que .venv_portable/Scripts/python.exe existe
  3. Copier l'INTÉGRALITÉ du dossier projet (avec .venv_portable/) sur :
       • clé USB
       • répertoire réseau partagé
       • machine cible
  4. Sur la machine cible : double-clic sur LANCER.bat
     → LANCER.bat détecte automatiquement .venv_portable et l'utilise

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import venv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
VENV_DIR     = PROJECT_ROOT / ".venv_portable"
ASSETS_DIR   = PROJECT_ROOT / "assets"
REQUIREMENTS = PROJECT_ROOT / "requirements.txt"

# Dépendances minimales attendues dans le bundle
REQUIRED_PACKAGES = [
    "pandas>=2.0.0",
    "openpyxl>=3.1.0",
    "rapidfuzz>=3.0.0",
    "customtkinter>=5.2.0",
]

# Fichiers/dossiers requis pour que l'app fonctionne sur la machine cible.
# Si un fichier de cette liste est absent à la fin du build, on alerte.
RUNTIME_FILES = [
    "gleif_gui.py",
    "gleif_matcher.py",
    "gleif_updater.py",
    "LANCER.bat",
    "requirements.txt",
]

# Patterns à nettoyer pour produire un bundle "lean"
LEAN_GLOBS = [
    "**/__pycache__",
    "**/*.pyc",
    "**/*.pyo",
    "**/.pytest_cache",
    "**/.mypy_cache",
    "**/.DS_Store",
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _print_step(title: str):
    print()
    print("─" * 70)
    print(f"  {title}")
    print("─" * 70)


def _venv_python(venv_dir: Path) -> Path:
    """Chemin vers le python.exe du venv (Windows) ou bin/python (POSIX)."""
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _check_host_python() -> None:
    if sys.version_info < (3, 9):
        print(f"❌  Python 3.9+ requis (actuel : {sys.version.split()[0]}).")
        sys.exit(2)
    bits = "64-bit" if sys.maxsize > 2**32 else "32-bit"
    if sys.maxsize <= 2**32:
        print(f"⚠️  Python hôte 32-bit détecté.")
        print("   Le bundle hérite de cette architecture — l'app risque l'erreur")
        print("   mémoire sur le GLEIF complet (450 Mo). Préférez Python 64-bit.")
    print(f"  Python hôte : {sys.executable}  ({sys.version.split()[0]} {bits})")


def _platform_warning() -> None:
    if os.name != "nt":
        print()
        print("⚠️  Plateforme non-Windows détectée.")
        print(f"   Le venv sera créé pour {sys.platform} et NE FONCTIONNERA PAS")
        print("   sur les postes Windows. Lancez ce script sur un poste Windows")
        print("   pour générer un bundle distribuable.")
        print()


# ─────────────────────────────────────────────────────────────────────────────
# Étapes
# ─────────────────────────────────────────────────────────────────────────────

def step_create_venv(rebuild: bool) -> Path:
    _print_step("1. Création de l'environnement virtuel portable")
    if VENV_DIR.exists():
        if rebuild:
            print(f"  Suppression de l'ancien venv : {VENV_DIR}")
            shutil.rmtree(VENV_DIR, ignore_errors=True)
        else:
            print(f"  Venv existant détecté : {VENV_DIR}")
            print(f"  → Réinstallation des dépendances seulement.")
            return _venv_python(VENV_DIR)

    print(f"  Création de {VENV_DIR}…")
    # --copies : pas de symlinks (Windows utilise déjà des copies, mais sur
    # macOS/Linux on force la copie pour rendre le dossier déplaçable).
    builder = venv.EnvBuilder(
        with_pip=True,
        clear=False,
        symlinks=False,
        copies=True,
        upgrade_deps=False,
    )
    builder.create(str(VENV_DIR))

    py = _venv_python(VENV_DIR)
    if not py.exists():
        print(f"❌  Échec : python introuvable dans {VENV_DIR}")
        sys.exit(3)
    print(f"  ✓ Venv prêt : {py}")
    return py


def step_install_deps(venv_python: Path) -> None:
    _print_step("2. Installation des dépendances")

    # Mise à jour de pip pour éviter les warnings de version obsolète.
    print("  Mise à jour de pip…")
    subprocess.run(
        [str(venv_python), "-m", "pip", "install", "--upgrade", "pip", "--quiet"],
        check=False,
    )

    # On installe d'abord requirements.txt s'il existe, puis on s'assure que
    # les paquets critiques sont présents (filet de sécurité).
    if REQUIREMENTS.exists():
        print(f"  Installation depuis {REQUIREMENTS.name}…")
        rc = subprocess.run(
            [str(venv_python), "-m", "pip", "install", "-r", str(REQUIREMENTS)],
        ).returncode
        if rc != 0:
            print(f"❌  pip a retourné {rc}.")
            sys.exit(4)
    else:
        print(f"⚠️  {REQUIREMENTS.name} absent — fallback sur la liste codée en dur.")
        rc = subprocess.run(
            [str(venv_python), "-m", "pip", "install", *REQUIRED_PACKAGES],
        ).returncode
        if rc != 0:
            sys.exit(4)

    # sqlite3 fait partie de la stdlib — on vérifie juste qu'il est importable.
    rc = subprocess.run(
        [str(venv_python), "-c", "import sqlite3, pandas, openpyxl, rapidfuzz, customtkinter"],
    ).returncode
    if rc != 0:
        print("❌  Une dépendance n'est pas importable. Voir les logs pip ci-dessus.")
        sys.exit(5)
    print("  ✓ Toutes les dépendances sont importables.")


def step_static_assets() -> None:
    _print_step("3. Préparation des fichiers statiques")
    # assets/ pour le futur logo SG
    if not ASSETS_DIR.exists():
        ASSETS_DIR.mkdir(parents=True, exist_ok=True)
        readme = ASSETS_DIR / "README.txt"
        readme.write_text(
            "Déposez ici votre logo SG (logo_sg.png, 200×60 px max).\n"
            "S'il est absent, l'application affiche un placeholder textuel.\n",
            encoding="utf-8",
        )
        print(f"  ✓ Dossier assets/ créé avec README.")
    else:
        print(f"  ✓ assets/ déjà présent.")

    # gleif_config.json est optionnel (config d'équipe). On ne le crée pas
    # automatiquement pour éviter d'écraser une config existante.
    cfg = PROJECT_ROOT / "gleif_config.json"
    if not cfg.exists():
        print(f"  ℹ  gleif_config.json absent — l'app utilisera ses défauts.")
    else:
        print(f"  ✓ gleif_config.json détecté.")

    # Template Excel (optionnel)
    tpl = PROJECT_ROOT / "template_societes.xlsx"
    if tpl.exists():
        print(f"  ✓ Template Excel détecté : {tpl.name}")
    else:
        print(f"  ℹ  Template Excel absent ({tpl.name}) — non bloquant.")


def step_clean_lean(also_pycache: bool) -> None:
    _print_step("4. Nettoyage (build lean)")
    deleted = 0
    for pattern in LEAN_GLOBS if also_pycache else ("**/.DS_Store",):
        for path in PROJECT_ROOT.glob(pattern):
            # On NE supprime PAS dans le venv (il a son propre __pycache__
            # qui doit rester pour les performances)
            if VENV_DIR in path.parents or path == VENV_DIR:
                continue
            try:
                if path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    path.unlink()
                deleted += 1
            except Exception:
                pass
    print(f"  ✓ {deleted} fichier(s)/dossier(s) supprimé(s).")


def step_verify_runtime() -> None:
    _print_step("5. Vérification des fichiers runtime")
    missing = [f for f in RUNTIME_FILES if not (PROJECT_ROOT / f).exists()]
    if missing:
        print(f"⚠️  Fichiers manquants : {missing}")
        print("   Le bundle ne fonctionnera pas sans ces fichiers.")
    else:
        print("  ✓ Tous les fichiers runtime sont présents.")


def step_smoke_test(venv_python: Path) -> None:
    _print_step("6. Test d'import (smoke test)")
    rc = subprocess.run(
        [str(venv_python), "-c",
         "import gleif_matcher, gleif_updater; "
         "print('  ✓ gleif_matcher OK'); "
         "print('  ✓ gleif_updater OK'); "
         "import customtkinter; print('  ✓ customtkinter OK'); "
         "import sqlite3; print(f'  ✓ sqlite3 {sqlite3.sqlite_version}')"],
        cwd=str(PROJECT_ROOT),
    ).returncode
    if rc != 0:
        print(f"❌  Import en échec — code {rc}.")
        sys.exit(6)


def step_summary(venv_python: Path) -> None:
    _print_step("✓ BUNDLE PORTABLE PRÊT")
    size_mb = sum(
        f.stat().st_size for f in VENV_DIR.rglob("*") if f.is_file()
    ) / 1_048_576
    print()
    print(f"  Dossier portable : {PROJECT_ROOT}")
    print(f"  Venv Python      : {venv_python}")
    print(f"  Taille du venv   : {size_mb:.0f} Mo")
    print()
    print("  PROCHAINES ÉTAPES :")
    print(f"    1. Vérifier que LANCER.bat utilise bien le venv portable.")
    print(f"    2. Copier le dossier complet sur :")
    print(f"         • clé USB")
    print(f"         • répertoire réseau partagé")
    print(f"         • poste cible")
    print(f"    3. Sur le poste cible : double-clic sur LANCER.bat")
    print()
    print("  Le poste cible n'a PAS besoin d'avoir Python installé.")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Construit un bundle Python portable pour LEI Matcher.",
    )
    parser.add_argument(
        "--rebuild", action="store_true",
        help="Supprime le venv existant avant reconstruction.",
    )
    parser.add_argument(
        "--clean", action="store_true",
        help="Nettoie aussi les __pycache__/ (build lean).",
    )
    parser.add_argument(
        "--no-test", action="store_true",
        help="Saute le smoke test d'import (rare).",
    )
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║         LEI Matcher — Constructeur de bundle portable            ║")
    print("║         Société Générale Middle Office                           ║")
    print("╚══════════════════════════════════════════════════════════════════╝")

    _check_host_python()
    _platform_warning()

    venv_python = step_create_venv(rebuild=args.rebuild)
    step_install_deps(venv_python)
    step_static_assets()
    step_clean_lean(also_pycache=args.clean)
    step_verify_runtime()
    if not args.no_test:
        step_smoke_test(venv_python)
    step_summary(venv_python)
    return 0


if __name__ == "__main__":
    sys.exit(main())
