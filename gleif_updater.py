"""
gleif_updater.py
================
Gestion des mises à jour de la base GLEIF Golden Copy.

Proxy — comportement :
  proxy = None  → urllib utilise le proxy système Windows (PAC/WinHTTP/Kerberos)
                   C'est le mode par défaut recommandé en entreprise.
  proxy = ""    → connexion directe forcée (bypass proxy)
  proxy = "http://host:port" → proxy explicite SANS authentification Windows.
                   ⚠️  Peut renvoyer HTTP 407 si le proxy requiert NTLM/Kerberos.
                   Dans ce cas, laissez le champ vide (mode système).

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

API_URL         = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2"
VERSION_FILE    = ".gleif_version.json"
CHUNK_SIZE      = 1024 * 256   # 256 Ko par chunk
CONNECT_TIMEOUT = 15            # secondes

# Avertissement affiché quand l'utilisateur force un proxy explicite
PROXY_AUTH_WARNING = (
    "⚠️  Proxy explicite configuré.\n"
    "Si vous obtenez une erreur HTTP 407 (Proxy Authentication Required),\n"
    "l'authentification NTLM/Kerberos n'est pas supportée en mode explicite.\n"
    "Solution : videz le champ proxy pour utiliser le proxy système Windows."
)


# ─────────────────────────────────────────────────────────────────────────────
# Détection du proxy système
# ─────────────────────────────────────────────────────────────────────────────

def detect_system_proxy() -> Optional[str]:
    """
    Détecte le proxy configuré au niveau du système Windows 11.

    Ordre de priorité :
      1. Variables d'environnement HTTPS_PROXY / HTTP_PROXY (GPO)
      2. Registre Windows — Internet Settings :
         - ProxyEnable=1 + ProxyServer  → proxy manuel
         - AutoConfigURL               → fichier PAC (urllib gère via WinHTTP)
      3. urllib.request.getproxies()   → fallback multi-plateforme
      4. None                          → connexion directe

    Retourne l'adresse du proxy (str) ou None si PAC/direct.
    Quand None est retourné, urllib utilisera automatiquement le proxy système.
    """
    # 1. Variables d'environnement
    for var in ("HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"):
        val = os.environ.get(var, "").strip()
        if val:
            log.debug(f"Proxy via env {var} : {val}")
            return val

    # 2. Registre Windows
    try:
        import winreg
        reg_path = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, reg_path) as key:
            # Proxy manuel
            try:
                enabled, _ = winreg.QueryValueEx(key, "ProxyEnable")
                if enabled:
                    server, _ = winreg.QueryValueEx(key, "ProxyServer")
                    server = server.strip()
                    if server:
                        if "=" in server:
                            parts = dict(
                                p.split("=", 1) for p in server.split(";") if "=" in p
                            )
                            server = parts.get("https", parts.get("http", server))
                        if not server.startswith("http"):
                            server = "http://" + server
                        log.debug(f"Proxy registre Windows (manuel) : {server}")
                        return server
            except FileNotFoundError:
                pass
            # Fichier PAC → on retourne None, urllib/WinHTTP le gère
            try:
                pac_url, _ = winreg.QueryValueEx(key, "AutoConfigURL")
                if pac_url:
                    log.debug(f"PAC détecté : {pac_url} — délégation à WinHTTP")
                    return None
            except FileNotFoundError:
                pass
    except ImportError:
        pass  # non-Windows

    # 3. Fallback urllib
    proxies = urllib.request.getproxies()
    for k in ("https", "http"):
        val = proxies.get(k, "").strip()
        if val:
            log.debug(f"Proxy urllib.getproxies ({k}) : {val}")
            return val

    log.debug("Aucun proxy détecté — connexion directe.")
    return None


def _build_opener(proxy: Optional[str]) -> urllib.request.OpenerDirector:
    """
    Construit l'opener urllib selon la valeur du proxy :

    None    → urllib gère le proxy système (PAC/WinHTTP/Kerberos transparent)
              ✅ Mode recommandé en entreprise avec NTLM/Kerberos.
    ""      → Connexion directe forcée (bypass proxy).
    "http://..." → Proxy explicite SANS auth Windows.
              ⚠️  Peut échouer avec HTTP 407 si le proxy exige NTLM/Kerberos.
    """
    if proxy is None:
        # Mode système : urllib lit automatiquement le proxy Windows
        return urllib.request.build_opener()

    if proxy.strip() == "":
        # Bypass explicite
        return urllib.request.build_opener(urllib.request.ProxyHandler({}))

    # Proxy explicite fourni par l'utilisateur
    p = proxy.strip()
    if not p.startswith("http"):
        p = "http://" + p
    log.warning(PROXY_AUTH_WARNING)
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": p, "https": p})
    )


# ─────────────────────────────────────────────────────────────────────────────
# API GLEIF
# ─────────────────────────────────────────────────────────────────────────────

def fetch_latest_metadata(proxy: Optional[str] = None) -> dict:
    """
    Interroge l'API GLEIF et retourne les métadonnées du dernier Golden Copy.

    Retourne :
      publish_date, size_bytes, size_human, download_url, record_count
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

    latest   = publications[0]
    csv_info = latest.get("full_file", {}).get("csv", {})

    return {
        "publish_date": latest.get("publish_date", ""),
        "size_bytes":   csv_info.get("size", 0),
        "size_human":   csv_info.get("size_human_readable", "?"),
        "download_url": csv_info.get("url", ""),
        "record_count": csv_info.get("record_count", 0),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Gestion de la version locale
# ─────────────────────────────────────────────────────────────────────────────

def _version_path(gleif_dir: Path) -> Path:
    return gleif_dir / VERSION_FILE


def read_local_version(gleif_dir: Path) -> Optional[str]:
    vp = _version_path(gleif_dir)
    if not vp.exists():
        return None
    try:
        with open(vp, encoding="utf-8") as f:
            return json.load(f).get("publish_date")
    except Exception:
        return None


def write_local_version(gleif_dir: Path, publish_date: str, filename: str) -> None:
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
    """Télécharge le ZIP GLEIF en streaming. Retourne le chemin du ZIP."""
    opener   = _build_opener(proxy)
    req      = urllib.request.Request(url, headers={"User-Agent": "GLEIF-LEI-Matcher/1.0"})
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
    """Extrait le CSV du ZIP GLEIF. Retourne le chemin du CSV extrait."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError("Aucun fichier CSV dans l'archive GLEIF.")
        csv_name = csv_names[0]
        zf.extract(csv_name, dest_dir)
        extracted = dest_dir / csv_name

    final_path = dest_dir / "gleif_golden_copy.csv"
    if final_path.exists():
        final_path.unlink()
    extracted.rename(final_path)
    zip_path.unlink(missing_ok=True)
    return final_path


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────────────────────────────────────

def check_and_download(
    gleif_file_path: str,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    status_cb: Optional[Callable[[str], None]] = None,
    proxy: Optional[str] = None,
    prepare_slim_after: bool = False,
    slim_progress_cb: Optional[Callable[[int, int], None]] = None,
    slim_status_cb: Optional[Callable[[str], None]] = None,
) -> Tuple[str, str]:
    """
    Vérifie et télécharge la dernière version GLEIF si nécessaire.

    Paramètres
    ----------
    gleif_file_path    : chemin local du fichier GLEIF
    progress_cb        : progression du téléchargement (bytes_done, total)
    status_cb          : messages de statut texte
    proxy              : None = proxy système, "" = direct, "http://..." = explicite
    prepare_slim_after : si True, génère un slim CSV après extraction
    slim_progress_cb   : progression de la génération slim
    slim_status_cb     : statut de la génération slim

    Retourne (status, message_ou_chemin_fichier)
      status ∈ {"up_to_date", "updated", "error"}
    """
    def _status(msg):
        log.info(msg)
        if status_cb:
            status_cb(msg)

    try:
        _status("Vérification de la dernière version GLEIF…")
        meta = fetch_latest_metadata(proxy=proxy)

        gleif_path = Path(gleif_file_path) if gleif_file_path else None
        dest_dir   = (
            gleif_path.parent
            if (gleif_path and gleif_path.parent.exists())
            else Path.cwd()
        )
        local_date = read_local_version(dest_dir)

        if not is_update_available(local_date, meta["publish_date"]):
            msg = f"Base GLEIF déjà à jour (version du {meta['publish_date'][:10]})."
            _status(msg)
            return "up_to_date", msg

        _status(
            f"Nouvelle version : {meta['publish_date'][:10]}  "
            f"({meta['size_human']}, {meta['record_count']:,} entités)"
        )

        _status(f"Téléchargement ({meta['size_human']})…")
        zip_path = download_gleif(
            url=meta["download_url"],
            dest_dir=dest_dir,
            total_bytes=meta["size_bytes"],
            progress_cb=progress_cb,
            proxy=proxy,
        )

        _status("Extraction du CSV…")
        final_csv = extract_csv(zip_path, dest_dir)
        write_local_version(dest_dir, meta["publish_date"], final_csv.name)

        if prepare_slim_after:
            from gleif_matcher import prepare_slim
            slim_path = dest_dir / "gleif_slim.csv"
            prepare_slim(
                str(final_csv), str(slim_path),
                active_only=True,
                progress_cb=slim_progress_cb,
                status_cb=slim_status_cb,
            )

        _status(f"Mise à jour réussie — {meta['publish_date'][:10]}")
        return "updated", str(final_csv)

    except urllib.error.HTTPError as e:
        if e.code == 407:
            msg = (
                "❌  HTTP 407 – Proxy Authentication Required.\n\n"
                "Le proxy d'entreprise exige une authentification NTLM/Kerberos\n"
                "que urllib ne peut pas gérer en mode proxy explicite.\n\n"
                "✅  Solution : videz le champ proxy et relancez.\n"
                "     (Le proxy système Windows gérera l'authentification automatiquement.)"
            )
        else:
            msg = f"❌  Erreur HTTP {e.code} : {e.reason}"
        log.error(msg)
        return "error", msg

    except urllib.error.URLError as e:
        msg = (
            f"❌  Impossible de contacter les serveurs GLEIF.\n"
            f"Détail : {e.reason}\n\n"
            "Vérifiez votre connexion réseau ou la configuration proxy."
        )
        log.error(msg)
        return "error", msg

    except Exception as e:
        msg = f"❌  Erreur inattendue : {e}"
        log.error(msg, exc_info=True)
        return "error", msg
