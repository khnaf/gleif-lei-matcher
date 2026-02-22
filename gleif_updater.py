"""
gleif_updater.py
================
Gestion des mises à jour automatiques de la base GLEIF Golden Copy.

Fonctionnement :
  1. Interroge l'API GLEIF pour connaître la date et la taille du dernier fichier
  2. Compare avec la version locale (fichier .gleif_version.json)
  3. Si une version plus récente existe, propose le téléchargement
  4. Télécharge le ZIP en streaming avec suivi de progression
  5. Extrait le CSV et met à jour le fichier de version local

URL API : https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2
"""

import json
import logging
import os
import urllib.request
import urllib.error
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional, Tuple

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

API_URL        = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2"
VERSION_FILE   = ".gleif_version.json"   # stocké à côté du fichier GLEIF
CHUNK_SIZE     = 1024 * 256              # 256 Ko par chunk
CONNECT_TIMEOUT = 15                     # secondes


# ─────────────────────────────────────────────────────────────────────────────
# Détection du proxy
# ─────────────────────────────────────────────────────────────────────────────

def detect_system_proxy() -> Optional[str]:
    """
    Détecte automatiquement le proxy configuré sur le poste Windows 11.

    Ordre de priorité :
      1. Variables d'environnement HTTPS_PROXY / HTTP_PROXY (poussées par GPO)
      2. Registre Windows — Internet Settings (utilisé par Edge / IE / WinHTTP)
         → Proxy manuel   : ProxyEnable=1 + ProxyServer
         → Fichier PAC    : AutoConfigURL  (détecté, mais non interprété)
      3. urllib.request.getproxies() — fallback multi-plateforme
      4. Aucun proxy → None (connexion directe)

    Retourne :
      str   → adresse proxy  ex: "http://proxy.corp.com:8080"
      ""    → PAC détecté mais non résolvable (urllib utilisera le proxy système auto)
      None  → aucun proxy
    """
    # ── 1. Variables d'environnement ────────────────────────────────────────
    for var in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        val = os.environ.get(var, "").strip()
        if val:
            log.debug(f"Proxy via env {var} : {val}")
            return val

    # ── 2. Registre Windows (Windows 11 / Edge) ──────────────────────────────
    try:
        import winreg
        reg_path = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, reg_path) as key:

            # Proxy manuel activé ?
            try:
                enabled, _ = winreg.QueryValueEx(key, "ProxyEnable")
                if enabled:
                    server, _ = winreg.QueryValueEx(key, "ProxyServer")
                    server = server.strip()
                    if server:
                        # Peut être "proxy:8080" ou "http=proxy:80;https=proxy:443"
                        # On extrait la valeur https ou http en priorité
                        if "=" in server:
                            parts = dict(
                                p.split("=", 1)
                                for p in server.split(";")
                                if "=" in p
                            )
                            server = parts.get("https", parts.get("http", server))
                        if not server.startswith("http"):
                            server = "http://" + server
                        log.debug(f"Proxy via registre Windows (manuel) : {server}")
                        return server
            except FileNotFoundError:
                pass

            # Fichier PAC configuré ?
            try:
                pac_url, _ = winreg.QueryValueEx(key, "AutoConfigURL")
                if pac_url:
                    log.debug(f"PAC détecté : {pac_url} — urllib utilisera le proxy système")
                    # On retourne None : urllib.request.build_opener() sans handler
                    # laissera Windows résoudre le PAC automatiquement via WinHTTP
                    return None
            except FileNotFoundError:
                pass

    except ImportError:
        pass  # winreg absent (non-Windows) → on continue

    # ── 3. Fallback urllib ───────────────────────────────────────────────────
    proxies = urllib.request.getproxies()
    for key in ("https", "http"):
        val = proxies.get(key, "").strip()
        if val:
            log.debug(f"Proxy via urllib.getproxies ({key}) : {val}")
            return val

    log.debug("Aucun proxy détecté — connexion directe.")
    return None


def _build_opener(proxy: Optional[str]) -> urllib.request.OpenerDirector:
    """
    Construit un opener urllib en respectant la priorité :
      - proxy None  → urllib utilise le proxy système (registre Windows / PAC)
      - proxy ""    → connexion directe forcée (bypass proxy)
      - proxy str   → proxy explicitement fourni (ex: "http://proxy:8080")

    Sur Windows 11, quand proxy=None, urllib.request s'appuie sur WinHTTP
    qui résout automatiquement les fichiers PAC configurés dans Edge / GPO.
    """
    if proxy is None:
        # Comportement par défaut : urllib lit le proxy système Windows
        return urllib.request.build_opener()
    if proxy.strip() == "":
        # Bypass explicite demandé par l'utilisateur
        return urllib.request.build_opener(
            urllib.request.ProxyHandler({})
        )
    # Proxy explicitement fourni dans le champ de la GUI
    p = proxy.strip()
    if not p.startswith("http"):
        p = "http://" + p
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": p, "https": p})
    )


# ─────────────────────────────────────────────────────────────────────────────
# Interrogation de l'API GLEIF
# ─────────────────────────────────────────────────────────────────────────────

def fetch_latest_metadata(proxy: Optional[str] = None) -> dict:
    """
    Interroge l'API GLEIF et retourne les métadonnées de la dernière publication.

    Retourne un dict avec :
      - publish_date  : str  "YYYY-MM-DD HH:MM:SS"
      - size_bytes    : int
      - size_human    : str  "447.34 MB"
      - download_url  : str
      - record_count  : int

    Lève urllib.error.URLError en cas d'échec réseau.
    """
    opener = _build_opener(proxy)
    req = urllib.request.Request(
        API_URL,
        headers={"Accept": "application/json", "User-Agent": "GLEIF-LEI-Matcher/1.0"},
    )

    with opener.open(req, timeout=CONNECT_TIMEOUT) as resp:
        raw = json.loads(resp.read().decode("utf-8"))

    publications = raw.get("data", [])
    if not publications:
        raise ValueError("L'API GLEIF n'a retourné aucune publication.")

    latest = publications[0]
    csv_info = latest.get("full_file", {}).get("csv", {})

    return {
        "publish_date":  latest.get("publish_date", ""),
        "size_bytes":    csv_info.get("size", 0),
        "size_human":    csv_info.get("size_human_readable", "?"),
        "download_url":  csv_info.get("url", ""),
        "record_count":  csv_info.get("record_count", 0),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Gestion de la version locale
# ─────────────────────────────────────────────────────────────────────────────

def _version_path(gleif_dir: Path) -> Path:
    return gleif_dir / VERSION_FILE


def read_local_version(gleif_dir: Path) -> Optional[str]:
    """
    Lit la date de la version locale depuis .gleif_version.json.
    Retourne None si le fichier n'existe pas.
    """
    vp = _version_path(gleif_dir)
    if not vp.exists():
        return None
    try:
        with open(vp, encoding="utf-8") as f:
            return json.load(f).get("publish_date")
    except Exception:
        return None


def write_local_version(gleif_dir: Path, publish_date: str, filename: str) -> None:
    """Sauvegarde la date et le nom du fichier de la version téléchargée."""
    vp = _version_path(gleif_dir)
    with open(vp, "w", encoding="utf-8") as f:
        json.dump(
            {
                "publish_date":  publish_date,
                "filename":      filename,
                "downloaded_at": datetime.now().isoformat(timespec="seconds"),
            },
            f, indent=2,
        )


def is_update_available(local_date: Optional[str], remote_date: str) -> bool:
    """Retourne True si la version distante est plus récente que la locale."""
    if not local_date:
        return True
    try:
        fmt = "%Y-%m-%d %H:%M:%S"
        return datetime.strptime(remote_date, fmt) > datetime.strptime(local_date, fmt)
    except ValueError:
        return remote_date != local_date


# ─────────────────────────────────────────────────────────────────────────────
# Téléchargement et extraction
# ─────────────────────────────────────────────────────────────────────────────

def download_gleif(
    url: str,
    dest_dir: Path,
    total_bytes: int,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    proxy: Optional[str] = None,
) -> Path:
    """
    Télécharge le fichier GLEIF (ZIP) en streaming vers dest_dir.

    progress_cb(bytes_downloaded, total_bytes) est appelé régulièrement.
    Retourne le chemin du ZIP téléchargé.
    """
    opener = _build_opener(proxy)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "GLEIF-LEI-Matcher/1.0"},
    )

    zip_path = dest_dir / "gleif_golden_copy_download.zip"

    with opener.open(req) as resp, open(zip_path, "wb") as out:
        downloaded = 0
        while True:
            chunk = resp.read(CHUNK_SIZE)
            if not chunk:
                break
            out.write(chunk)
            downloaded += len(chunk)
            if progress_cb:
                progress_cb(downloaded, total_bytes)

    return zip_path


def extract_csv(zip_path: Path, dest_dir: Path) -> Path:
    """
    Extrait le fichier CSV du ZIP GLEIF.
    Retourne le chemin du CSV extrait.
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Trouver le fichier CSV dans l'archive (peut avoir un nom avec date)
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError("Aucun fichier CSV trouvé dans l'archive GLEIF.")
        csv_name = csv_names[0]

        # Extraire vers le dossier destination
        zf.extract(csv_name, dest_dir)
        extracted = dest_dir / csv_name

        # Renommer en nom standard
        final_path = dest_dir / "gleif_golden_copy.csv"
        if final_path.exists():
            final_path.unlink()
        extracted.rename(final_path)

    # Supprimer le ZIP après extraction
    zip_path.unlink(missing_ok=True)
    return final_path


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée principal (utilisé par la GUI)
# ─────────────────────────────────────────────────────────────────────────────

def check_and_download(
    gleif_file_path: str,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    status_cb: Optional[Callable[[str], None]] = None,
    proxy: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Vérifie si une mise à jour est disponible et télécharge si nécessaire.

    Paramètres
    ----------
    gleif_file_path : chemin actuel du fichier GLEIF local
    progress_cb     : callback(bytes_done, total_bytes) pour la progression
    status_cb       : callback(message) pour les messages de statut
    proxy           : URL du proxy HTTP(S) d'entreprise (ex: "http://proxy:8080")

    Retourne
    --------
    (status, message)
      status = "up_to_date" | "updated" | "error" | "no_update_needed"
    """
    def _status(msg):
        log.info(msg)
        if status_cb:
            status_cb(msg)

    try:
        _status("Vérification de la dernière version GLEIF…")
        meta = fetch_latest_metadata(proxy=proxy)

        gleif_path = Path(gleif_file_path) if gleif_file_path else None
        dest_dir   = gleif_path.parent if (gleif_path and gleif_path.parent.exists()) \
                     else Path.cwd()

        local_date = read_local_version(dest_dir)

        if not is_update_available(local_date, meta["publish_date"]):
            msg = f"Base GLEIF déjà à jour (version du {meta['publish_date'][:10]})."
            _status(msg)
            return "up_to_date", msg

        _status(
            f"Nouvelle version disponible : {meta['publish_date'][:10]}  "
            f"({meta['size_human']}, {meta['record_count']:,} entités)"
        )

        # ── Téléchargement ──
        _status(f"Téléchargement en cours ({meta['size_human']})…")
        zip_path = download_gleif(
            url=meta["download_url"],
            dest_dir=dest_dir,
            total_bytes=meta["size_bytes"],
            progress_cb=progress_cb,
            proxy=proxy,
        )

        # ── Extraction ──
        _status("Extraction du fichier CSV…")
        final_csv = extract_csv(zip_path, dest_dir)

        # ── Mise à jour de la version locale ──
        write_local_version(dest_dir, meta["publish_date"], final_csv.name)

        msg = (
            f"Mise à jour réussie — version du {meta['publish_date'][:10]}\n"
            f"Fichier : {final_csv}"
        )
        _status(msg)
        return "updated", str(final_csv)

    except urllib.error.URLError as e:
        msg = (
            f"Impossible de contacter le serveur GLEIF.\n\n"
            f"Détail : {e.reason}\n\n"
            f"Vérifiez votre connexion ou configurez le proxy dans les paramètres."
        )
        log.error(msg)
        return "error", msg

    except Exception as e:
        msg = f"Erreur lors de la mise à jour : {e}"
        log.error(msg, exc_info=True)
        return "error", msg
