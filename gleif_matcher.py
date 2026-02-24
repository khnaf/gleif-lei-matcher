"""
gleif_matcher.py
================
Module de rapprochement LEI GLEIF pour fichiers Excel.

Workflow :
  1. Recherche exacte par numéro RCS (normalisé)
  2. Si non trouvé → fuzzy matching par nom d'entreprise + pays
  3. Export Excel enrichi avec colonnes LEI, statut, type de correspondance

Usage CLI :
  python gleif_matcher.py --input societes.xlsx --gleif gleif_golden_copy.csv --output resultats.xlsx

Colonnes GLEIF attendues (Golden Copy CSV) — variantes gérées automatiquement :
  LEI, Entity.LegalName, Entity.LegalAddress.Country,
  Entity.EntityStatus, Registration.RegistrationStatus,
  Registration.RegistrationAuthorityID  (ou Entity.RegistrationAuthority.*)
  Registration.RegistrationAuthorityEntityID (ou Entity.RegistrationAuthority.*)

Statuts :
  Entity.EntityStatus             → ACTIVE / INACTIVE / MERGED  (statut société)
  Registration.RegistrationStatus → ISSUED / LAPSED / RETIRED   (statut LEI)
"""

import argparse
import logging
import re
import sys
import shutil
import tempfile
import unicodedata
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd
from rapidfuzz import fuzz, process

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Mapping pays → ISO 3166-1 alpha-2
# ─────────────────────────────────────────────────────────────────────────────
COUNTRY_MAP: Dict[str, str] = {
    "france": "FR", "fr": "FR",
    "allemagne": "DE", "germany": "DE", "de": "DE",
    "italie": "IT", "italy": "IT", "it": "IT",
    "espagne": "ES", "spain": "ES", "es": "ES",
    "belgique": "BE", "belgium": "BE", "be": "BE",
    "suisse": "CH", "switzerland": "CH", "ch": "CH",
    "luxembourg": "LU", "lu": "LU",
    "pays-bas": "NL", "netherlands": "NL", "nl": "NL", "hollande": "NL",
    "royaume-uni": "GB", "united kingdom": "GB", "uk": "GB", "gb": "GB",
    "angleterre": "GB", "england": "GB",
    "etats-unis": "US", "états-unis": "US", "united states": "US", "usa": "US", "us": "US",
    "portugal": "PT", "pt": "PT",
    "autriche": "AT", "austria": "AT", "at": "AT",
    "suede": "SE", "suède": "SE", "sweden": "SE", "se": "SE",
    "danemark": "DK", "denmark": "DK", "dk": "DK",
    "norvege": "NO", "norvège": "NO", "norway": "NO", "no": "NO",
    "finlande": "FI", "finland": "FI", "fi": "FI",
    "pologne": "PL", "poland": "PL", "pl": "PL",
    "republique tcheque": "CZ", "czech republic": "CZ", "czechia": "CZ", "cz": "CZ",
    "irlande": "IE", "ireland": "IE", "ie": "IE",
    "grece": "GR", "grèce": "GR", "greece": "GR", "gr": "GR",
    "roumanie": "RO", "romania": "RO", "ro": "RO",
    "hongrie": "HU", "hungary": "HU", "hu": "HU",
    "japon": "JP", "japan": "JP", "jp": "JP",
    "chine": "CN", "china": "CN", "cn": "CN",
    "canada": "CA", "ca": "CA",
    "australie": "AU", "australia": "AU", "au": "AU",
    "singapour": "SG", "singapore": "SG", "sg": "SG",
    "emirats arabes unis": "AE", "uae": "AE", "ae": "AE",
    "monaco": "MC", "mc": "MC",
    "liechtenstein": "LI", "li": "LI",
    "andorre": "AD", "andorra": "AD", "ad": "AD",
    "ile maurice": "MU", "mauritius": "MU", "mu": "MU",
    "maroc": "MA", "morocco": "MA", "ma": "MA",
}

_ISO_PATTERN = re.compile(r"^[A-Z]{2}$")

_LEGAL_FORMS_RE = re.compile(
    r"\bS\.?A\.?S\.?U?\b|\bS\.?A\.?R\.?L\.?\b|\bS\.?A\.?\b"
    r"|\bS\.?N\.?C\.?\b|\bS\.?C\.?I\.?\b|\bE\.?U\.?R\.?L\.?\b"
    r"|\bG\.?I\.?E\.?\b|\bS\.?C\.?M\.?\b|\bS\.?C\.?P\.?\b"
    r"|\bS\.?C\.?S\.?\b|\bS\.?C\.?\b|\bG\.?M\.?B\.?H\.?\b"
    r"|\bA\.?G\.?\b|\bS\.?E\.?\b|\bL\.?T\.?D\.?\b|\bP\.?L\.?C\.?\b"
    r"|\bI\.?N\.?C\.?\b|\bL\.?L\.?C\.?\b|\bB\.?V\.?\b"
    r"|\bN\.?V\.?\b|\bS\.?P\.?A\.?\b|\bS\.?R\.?L\.?\b",
    re.IGNORECASE,
)

# ─────────────────────────────────────────────────────────────────────────────
# Schéma GLEIF — candidats par ordre de priorité pour chaque colonne logique
# Gère les variantes de nommage entre versions du Golden Copy
# ─────────────────────────────────────────────────────────────────────────────
GLEIF_COLUMN_CANDIDATES: Dict[str, List[str]] = {
    "lei": [
        "LEI",
    ],
    "name": [
        "Entity.LegalName",
        "Entity.LegalName.name",
    ],
    "country": [
        "Entity.LegalAddress.Country",
        "Entity.LegalAddress.country",
    ],
    "entity_status": [
        "Entity.EntityStatus",
        "Entity.Status",
    ],
    "lei_status": [
        "Registration.RegistrationStatus",
        "Registration.Status",
    ],
    "ra_id": [
        "Registration.RegistrationAuthorityID",
        "Entity.RegistrationAuthority.RegistrationAuthorityID",
        "Registration.RegistrationAuthority.RegistrationAuthorityID",
        "RegistrationAuthority.RegistrationAuthorityID",
    ],
    "ra_entity": [
        "Registration.RegistrationAuthorityEntityID",
        "Entity.RegistrationAuthority.RegistrationAuthorityEntityID",
        "Registration.RegistrationAuthority.RegistrationAuthorityEntityID",
        "RegistrationAuthority.RegistrationAuthorityEntityID",
    ],
}

# Noms standardisés dans le DataFrame interne et dans le slim CSV
SLIM_COLUMNS = list(GLEIF_COLUMN_CANDIDATES.keys())  # ordre stable

# Taille des chunks pour la lecture du CSV complet (~450 Mo)
GLEIF_CHUNK_SIZE = 100_000


# ─────────────────────────────────────────────────────────────────────────────
# Détection du schéma réel du fichier GLEIF
# ─────────────────────────────────────────────────────────────────────────────

def _detect_gleif_columns(available_cols: List[str]) -> Tuple[Dict[str, str], List[str]]:
    """
    Mappe les colonnes logiques vers les noms réels présents dans le fichier.

    Retourne :
      col_map  : {logical_name → actual_column_name}   pour les colonnes trouvées
      missing  : liste des colonnes logiques non trouvées (non bloquant)
    """
    available_set = set(available_cols)
    col_map: Dict[str, str] = {}
    missing: List[str] = []

    for logical, candidates in GLEIF_COLUMN_CANDIDATES.items():
        found = next((c for c in candidates if c in available_set), None)
        if found:
            col_map[logical] = found
        else:
            missing.append(logical)
            log.warning(
                f"Colonne GLEIF non trouvée : '{logical}' "
                f"(candidats essayés : {candidates}). Colonne laissée vide."
            )

    log.info(f"Mapping colonnes GLEIF : { {k: v for k, v in col_map.items()} }")
    if missing:
        log.warning(f"Colonnes absentes (seront vides) : {missing}")

    return col_map, missing


# ─────────────────────────────────────────────────────────────────────────────
# Normalisation
# ─────────────────────────────────────────────────────────────────────────────

def normalize_rcs(value) -> str:
    if pd.isna(value) or str(value).strip() == "":
        return ""
    raw = str(value).upper()
    raw = re.sub(r"^RCS\s+[A-ZÉÈÀÂÊÎÔÙÛÇ\s]+\s+", "", raw).strip()
    return re.sub(r"[^0-9A-Z]", "", raw)


def normalize_name(value) -> str:
    if pd.isna(value) or str(value).strip() == "":
        return ""
    name = str(value).upper()
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = _LEGAL_FORMS_RE.sub(" ", name)
    name = re.sub(r"[^A-Z0-9\s]", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def country_to_iso(value) -> str:
    if pd.isna(value) or str(value).strip() == "":
        return ""
    raw = str(value).strip().upper()
    if _ISO_PATTERN.match(raw):
        return raw
    key = raw.lower()
    key_no_accent = "".join(
        c for c in unicodedata.normalize("NFD", key)
        if unicodedata.category(c) != "Mn"
    )
    return COUNTRY_MAP.get(key_no_accent, COUNTRY_MAP.get(key, ""))


# ─────────────────────────────────────────────────────────────────────────────
# Lecture sécurisée d'un fichier Excel (gestion OneDrive Entreprise)
# ─────────────────────────────────────────────────────────────────────────────

def _safe_read_excel(path: str) -> pd.DataFrame:
    """
    Lit un fichier Excel en gérant les erreurs de permission OneDrive.

    Si PermissionError détecté (fichier cloud-only ou verrouillé par OneDrive),
    copie le fichier dans %TEMP%\\gleif_match\\ avant lecture.
    """
    p = Path(path)
    try:
        return pd.read_excel(path, dtype=str)
    except PermissionError:
        is_onedrive = "onedrive" in str(p).lower()
        if is_onedrive:
            log.warning(
                "Fichier OneDrive Entreprise inaccessible (cloud-only ou verrouillé). "
                "Copie temporaire en cours..."
            )
        else:
            log.warning(f"Permission refusée sur '{p.name}'. Tentative via copie temp...")

        tmp_dir = Path(tempfile.gettempdir()) / "gleif_match"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = tmp_dir / p.name
        shutil.copy2(path, str(tmp_path))
        log.info(f"Lecture depuis copie temporaire : {tmp_path}")
        return pd.read_excel(str(tmp_path), dtype=str)
    except Exception:
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Chargement GLEIF — lecture en chunks pour gérer les ~450 Mo
# ─────────────────────────────────────────────────────────────────────────────

def load_gleif(
    gleif_path: str,
    active_only: bool = True,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    status_cb: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    """
    Charge le fichier Golden Copy GLEIF (CSV ou JSON) en mémoire minimale.

    Stratégie CSV :
      1. Lecture de l'en-tête uniquement pour détecter le schéma réel
      2. Construction de la liste usecols avec les colonnes effectivement présentes
      3. Lecture en chunks (GLEIF_CHUNK_SIZE lignes) avec filtrage à la volée
      4. Concaténation des chunks filtrés → DataFrame final compact

    Paramètres
    ----------
    gleif_path  : chemin vers le fichier GLEIF (CSV ou JSON)
    active_only : filtre Entity=ACTIVE et LEI=ISSUED
    progress_cb : callback(chunks_lus, total_estimé)  [optionnel]
    status_cb   : callback(message_texte)             [optionnel]
    """
    def _status(msg: str):
        log.info(msg)
        if status_cb:
            status_cb(msg)

    path = Path(gleif_path)
    suffix = path.suffix.lower()
    _status(f"Chargement GLEIF : {path.name} …")

    # ── JSON ──────────────────────────────────────────────────────────────────
    if suffix == ".json":
        _status("Format JSON — lecture complète en mémoire…")
        raw = pd.read_json(gleif_path, dtype=str)
        if "LEI" not in raw.columns:
            raw = pd.json_normalize(raw.to_dict(orient="records"))
        return _finalize_gleif_df(raw, active_only)

    # ── CSV — lecture en chunks ───────────────────────────────────────────────
    # Étape 1 : détecter le schéma en lisant uniquement la première ligne
    header_df = pd.read_csv(gleif_path, nrows=0, dtype=str, low_memory=False)
    available_cols = list(header_df.columns)
    col_map, _missing = _detect_gleif_columns(available_cols)

    # Colonnes à lire (uniquement celles trouvées → réduit la mémoire ~10x)
    usecols = list(set(col_map.values()))
    _status(f"Colonnes retenues : {len(usecols)} / {len(available_cols)} — lecture par chunks…")

    # Estimation de la taille totale pour la progression
    try:
        file_size = path.stat().st_size
        # Estimation grossière : ~200 octets par ligne dans le CSV complet
        estimated_total_chunks = max(1, file_size // (200 * GLEIF_CHUNK_SIZE))
    except Exception:
        estimated_total_chunks = 200  # fallback

    # Étape 2 : lecture chunked
    chunks: List[pd.DataFrame] = []
    chunks_read = 0

    reader = pd.read_csv(
        gleif_path,
        usecols=usecols,
        dtype=str,
        low_memory=False,
        chunksize=GLEIF_CHUNK_SIZE,
        on_bad_lines="skip",   # ignore les lignes malformées plutôt que crash
    )

    for chunk in reader:
        # Renommer vers les noms logiques standardisés
        rename_map = {v: k for k, v in col_map.items()}
        chunk = chunk.rename(columns=rename_map)

        # Ajouter les colonnes absentes (non trouvées dans le schéma)
        for logical in SLIM_COLUMNS:
            if logical not in chunk.columns:
                chunk[logical] = ""

        chunk = chunk[SLIM_COLUMNS].fillna("")

        # Filtrage à la volée (évite d'accumuler des données inutiles)
        if active_only:
            mask = (
                (chunk["entity_status"].str.upper() == "ACTIVE") &
                (chunk["lei_status"].str.upper() == "ISSUED")
            )
            chunk = chunk[mask]

        if not chunk.empty:
            chunks.append(chunk)

        chunks_read += 1
        if progress_cb:
            progress_cb(chunks_read, estimated_total_chunks)

    if not chunks:
        log.warning("Aucune entité retenue après filtrage.")
        return pd.DataFrame(columns=SLIM_COLUMNS)

    df = pd.concat(chunks, ignore_index=True)

    n_total = chunks_read * GLEIF_CHUNK_SIZE
    _status(
        f"  Chargement terminé : {len(df):,} entités retenues "
        f"({'filtre ACTIVE+ISSUED' if active_only else 'tous statuts'})"
    )
    return df


def _finalize_gleif_df(raw: pd.DataFrame, active_only: bool) -> pd.DataFrame:
    """Post-traitement commun pour le JSON et les petits CSV."""
    col_map, _ = _detect_gleif_columns(list(raw.columns))
    rename_map = {v: k for k, v in col_map.items()}
    df = raw.rename(columns=rename_map)
    for logical in SLIM_COLUMNS:
        if logical not in df.columns:
            df[logical] = ""
    df = df[SLIM_COLUMNS].fillna("")
    if active_only:
        mask = (
            (df["entity_status"].str.upper() == "ACTIVE") &
            (df["lei_status"].str.upper() == "ISSUED")
        )
        df = df[mask]
    return df.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# Préparation d'une base SLIM (CSV léger, colonnes essentielles, filtrée)
# ─────────────────────────────────────────────────────────────────────────────

def prepare_slim(
    input_csv: str,
    output_csv: str,
    active_only: bool = True,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    status_cb: Optional[Callable[[str], None]] = None,
) -> int:
    """
    Génère un CSV allégé depuis le Golden Copy complet.

    Le slim CSV ne contient que les 7 colonnes utiles et (optionnellement)
    uniquement les entités ACTIVE + ISSUED. Taille typique : 200-400 Mo → 80-150 Mo.

    Retourne le nombre de lignes écrites.
    """
    def _status(msg: str):
        log.info(msg)
        if status_cb:
            status_cb(msg)

    path_in = Path(input_csv)
    path_out = Path(output_csv)

    _status(f"Préparation base slim : {path_in.name} → {path_out.name} …")

    header_df = pd.read_csv(str(path_in), nrows=0, dtype=str, low_memory=False)
    col_map, _ = _detect_gleif_columns(list(header_df.columns))
    usecols = list(set(col_map.values()))

    try:
        file_size = path_in.stat().st_size
        estimated_chunks = max(1, file_size // (200 * GLEIF_CHUNK_SIZE))
    except Exception:
        estimated_chunks = 200

    reader = pd.read_csv(
        str(path_in),
        usecols=usecols,
        dtype=str,
        low_memory=False,
        chunksize=GLEIF_CHUNK_SIZE,
        on_bad_lines="skip",
    )

    total_written = 0
    chunks_read = 0
    first_chunk = True

    for chunk in reader:
        rename_map = {v: k for k, v in col_map.items()}
        chunk = chunk.rename(columns=rename_map)
        for logical in SLIM_COLUMNS:
            if logical not in chunk.columns:
                chunk[logical] = ""
        chunk = chunk[SLIM_COLUMNS].fillna("")

        if active_only:
            mask = (
                (chunk["entity_status"].str.upper() == "ACTIVE") &
                (chunk["lei_status"].str.upper() == "ISSUED")
            )
            chunk = chunk[mask]

        if not chunk.empty:
            chunk.to_csv(
                str(path_out),
                mode="w" if first_chunk else "a",
                header=first_chunk,
                index=False,
                encoding="utf-8",
            )
            total_written += len(chunk)
            first_chunk = False

        chunks_read += 1
        if progress_cb:
            progress_cb(chunks_read, estimated_chunks)

    _status(f"Base slim générée : {total_written:,} entités → {path_out.name}")
    return total_written


# ─────────────────────────────────────────────────────────────────────────────
# Index de recherche
# ─────────────────────────────────────────────────────────────────────────────

def build_indices(
    df: pd.DataFrame,
) -> Tuple[Dict[str, List[int]], Dict[str, Dict[str, List[int]]]]:
    log.info("Construction des index …")
    rcs_index: Dict[str, List[int]] = {}
    for i, row in enumerate(df["ra_entity"]):
        key = normalize_rcs(row)
        if key:
            rcs_index.setdefault(key, []).append(i)

    name_index: Dict[str, Dict[str, List[int]]] = {}
    for i, (country, name) in enumerate(zip(df["country"], df["name"])):
        c = str(country).strip().upper()
        n = normalize_name(name)
        if c and n:
            name_index.setdefault(c, {}).setdefault(n, []).append(i)

    log.info(
        f"  Index RCS : {len(rcs_index):,} entrées | "
        f"Index noms : {sum(len(v) for v in name_index.values()):,} entrées"
    )
    return rcs_index, name_index


def search_by_rcs(
    rcs_norm: str,
    rcs_index: Dict[str, List[int]],
    df: pd.DataFrame,
) -> Optional[pd.Series]:
    if not rcs_norm:
        return None
    indices = rcs_index.get(rcs_norm)
    return df.iloc[indices[0]] if indices else None


def search_by_name_country(
    name_norm: str,
    iso_country: str,
    name_index: Dict[str, Dict[str, List[int]]],
    df: pd.DataFrame,
    threshold: int = 80,
) -> Tuple[Optional[pd.Series], int]:
    if not name_norm or not iso_country:
        return None, 0
    country_names = name_index.get(iso_country, {})
    if not country_names:
        return None, 0
    result = process.extractOne(
        name_norm,
        list(country_names.keys()),
        scorer=fuzz.token_sort_ratio,
        score_cutoff=threshold,
    )
    if result is None:
        return None, 0
    best_name, score, _ = result
    return df.iloc[country_names[best_name][0]], int(score)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline de rapprochement principal
# ─────────────────────────────────────────────────────────────────────────────

def match_companies(
    input_path: str,
    gleif_path: str,
    output_path: str,
    col_rcs: str = "RCS",
    col_name: str = "NomEntreprise",
    col_pays: str = "Pays",
    fuzzy_threshold: int = 80,
    active_only: bool = True,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    status_cb: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:

    def _status(msg):
        log.info(msg)
        if status_cb:
            status_cb(msg)

    _status(f"Lecture du fichier d'entrée : {input_path}")
    df_input = _safe_read_excel(input_path).fillna("")
    _status(f"  {len(df_input):,} lignes chargées")

    missing_cols = [c for c in [col_rcs, col_name, col_pays] if c not in df_input.columns]
    if missing_cols:
        raise ValueError(
            f"Colonnes manquantes dans le fichier d'entrée : {missing_cols}\n"
            f"Colonnes disponibles : {list(df_input.columns)}"
        )

    df_gleif = load_gleif(
        gleif_path,
        active_only=active_only,
        status_cb=status_cb,
    )
    rcs_index, name_index = build_indices(df_gleif)

    results = []
    n_total = len(df_input)
    n_exact = n_approx = n_miss = 0

    _status("Rapprochement en cours …")

    for idx, row in df_input.iterrows():
        rcs_norm  = normalize_rcs(str(row[col_rcs]))
        name_norm = normalize_name(str(row[col_name]))
        iso       = country_to_iso(str(row[col_pays]))

        gleif_row  = None
        match_type = "Non trouvé"
        match_score = ""

        if rcs_norm:
            gleif_row = search_by_rcs(rcs_norm, rcs_index, df_gleif)
            if gleif_row is not None:
                match_type  = "Exact – RCS"
                match_score = 100
                n_exact    += 1

        if gleif_row is None and name_norm:
            gleif_row, score = search_by_name_country(
                name_norm, iso, name_index, df_gleif, fuzzy_threshold
            )
            if gleif_row is not None:
                match_type  = "Approx – Nom/Pays"
                match_score = score
                n_approx   += 1

        if gleif_row is None:
            n_miss += 1

        if gleif_row is not None:
            results.append({
                "LEI":                    gleif_row["lei"],
                "GLEIF_NomLegal":         gleif_row["name"],
                "GLEIF_Pays":             gleif_row["country"],
                "GLEIF_StatutSociete":    gleif_row["entity_status"],
                "GLEIF_StatutLEI":        gleif_row["lei_status"],
                "GLEIF_AutoriteRegistre": gleif_row["ra_id"],
                "GLEIF_NumRegistre":      gleif_row["ra_entity"],
                "TypeCorrespondance":     match_type,
                "ScoreCorrespondance":    match_score,
            })
        else:
            results.append({
                "LEI": "", "GLEIF_NomLegal": "", "GLEIF_Pays": "",
                "GLEIF_StatutSociete": "", "GLEIF_StatutLEI": "",
                "GLEIF_AutoriteRegistre": "", "GLEIF_NumRegistre": "",
                "TypeCorrespondance": match_type, "ScoreCorrespondance": "",
            })

        if progress_cb and ((idx + 1) % 10 == 0 or (idx + 1) == n_total):
            progress_cb(idx + 1, n_total)

    df_results = pd.DataFrame(results)
    df_output  = pd.concat([df_input.reset_index(drop=True), df_results], axis=1)

    _status(f"Export vers : {output_path}")
    _export_excel(df_output, output_path, fuzzy_threshold)

    log.info(
        f"\n{'='*50}\n"
        f"  Total        : {n_total:>6,}\n"
        f"  Exact RCS    : {n_exact:>6,}  ({n_exact/n_total*100:.1f}%)\n"
        f"  Approx Nom   : {n_approx:>6,}  ({n_approx/n_total*100:.1f}%)\n"
        f"  Non trouvé   : {n_miss:>6,}  ({n_miss/n_total*100:.1f}%)\n"
        f"{'='*50}"
    )
    return df_output


# ─────────────────────────────────────────────────────────────────────────────
# Export Excel
# ─────────────────────────────────────────────────────────────────────────────

def _export_excel(df: pd.DataFrame, output_path: str, threshold: int) -> None:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Résultats LEI"

    HEADER_FILL = PatternFill("solid", fgColor="1F4E79")
    GLEIF_FILL  = PatternFill("solid", fgColor="D6E4F0")
    EXACT_FILL  = PatternFill("solid", fgColor="D9EAD3")
    APPROX_FILL = PatternFill("solid", fgColor="FFF2CC")
    MISS_FILL   = PatternFill("solid", fgColor="FCE4D6")

    thin   = Side(style="thin", color="AAAAAA")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    hfont  = Font(name="Arial", bold=True, color="FFFFFF", size=10)
    dfont  = Font(name="Arial", size=10)

    gleif_cols = [
        "LEI", "GLEIF_NomLegal", "GLEIF_Pays",
        "GLEIF_StatutSociete", "GLEIF_StatutLEI",
        "GLEIF_AutoriteRegistre", "GLEIF_NumRegistre",
        "TypeCorrespondance", "ScoreCorrespondance",
    ]
    columns = list(df.columns)

    for ci, cn in enumerate(columns, 1):
        cell = ws.cell(row=1, column=ci, value=cn)
        cell.font      = hfont
        cell.fill      = HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = border

    for ri, row in enumerate(df.itertuples(index=False), 2):
        mt = getattr(row, "TypeCorrespondance", "")
        rf = EXACT_FILL if mt == "Exact – RCS" else \
             APPROX_FILL if mt == "Approx – Nom/Pays" else \
             MISS_FILL   if mt == "Non trouvé" else None

        for ci, (cn, val) in enumerate(zip(columns, row), 1):
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.font   = dfont
            cell.border = border
            cell.alignment = Alignment(vertical="center")
            if cn in gleif_cols and rf:
                cell.fill = rf
            elif cn in gleif_cols:
                cell.fill = GLEIF_FILL

    col_widths = {
        "LEI": 25, "GLEIF_NomLegal": 35, "GLEIF_Pays": 10,
        "GLEIF_StatutSociete": 16, "GLEIF_StatutLEI": 14,
        "GLEIF_AutoriteRegistre": 18, "GLEIF_NumRegistre": 20,
        "TypeCorrespondance": 20, "ScoreCorrespondance": 12,
    }
    for ci, cn in enumerate(columns, 1):
        ws.column_dimensions[get_column_letter(ci)].width = col_widths.get(cn, 22)

    ws.row_dimensions[1].height = 30
    ws.freeze_panes = "A2"

    ws_legend = wb.create_sheet("Légende")
    for r, (a, b) in enumerate([
        ("Couleur", "Signification"),
        ("Vert",    "Correspondance exacte par numéro RCS"),
        (f"Jaune",  f"Correspondance approximative nom/pays (score ≥ {threshold})"),
        ("Rouge",   "Aucune correspondance trouvée"),
        ("Bleu",    "Colonnes issues de la base GLEIF"),
    ], 1):
        ws_legend.cell(r, 1, a).font = Font(bold=(r == 1))
        ws_legend.cell(r, 2, b).font = Font(bold=(r == 1))
    ws_legend.column_dimensions["A"].width = 15
    ws_legend.column_dimensions["B"].width = 55

    wb.save(output_path)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser(description="GLEIF LEI Matcher")
    p.add_argument("--input",             required=True)
    p.add_argument("--gleif",             required=True)
    p.add_argument("--output",            required=True)
    p.add_argument("--col-rcs",           default="RCS")
    p.add_argument("--col-name",          default="NomEntreprise")
    p.add_argument("--col-pays",          default="Pays")
    p.add_argument("--fuzzy-threshold",   type=int, default=80)
    p.add_argument("--active-only",       action="store_true", default=True)
    p.add_argument("--all-statuses",      dest="active_only", action="store_false")
    p.add_argument("--prepare-slim",      action="store_true",
                   help="Préparer une base slim avant le matching")
    p.add_argument("--slim-output",       default=None,
                   help="Chemin du CSV slim (défaut : gleif_slim.csv à côté du fichier GLEIF)")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    gleif_path = args.gleif
    if args.prepare_slim:
        slim_path = args.slim_output or str(Path(args.gleif).parent / "gleif_slim.csv")
        log.info(f"Préparation de la base slim → {slim_path}")
        prepare_slim(args.gleif, slim_path, active_only=args.active_only)
        gleif_path = slim_path

    match_companies(
        input_path      = args.input,
        gleif_path      = gleif_path,
        output_path     = args.output,
        col_rcs         = args.col_rcs,
        col_name        = args.col_name,
        col_pays        = args.col_pays,
        fuzzy_threshold = args.fuzzy_threshold,
        active_only     = args.active_only,
    )
