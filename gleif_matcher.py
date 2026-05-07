"""
gleif_matcher.py
================
Module de rapprochement LEI GLEIF — version 2.0 "Middle Office Edition".

Évolutions v2.0 :
  • Index RCS composite (RCS, Pays_ISO) : un même numéro de registre dans deux
    pays distincts ne peut plus produire de faux positif silencieux.
  • Colonne "Fiabilite" à 3 niveaux (OK / À vérifier / KO) et "ActionRequise"
    explicite pour les équipes Middle Office.
  • Export Excel restructuré par blocs thématiques (Identité / Légal / LEI /
    Synthèse), code couleur strict ligne entière, onglet Instructions.
  • Discordances reformulées en messages métier ("Nom différent : X vs Y")
    sans score brut.
  • Suppression du code mort (_check_lei_discordance).

Workflow :
  1. Si un LEI existant est fourni → mode validation (lookup direct, fallback
     RCS+Pays / Nom+Pays si LEI introuvable dans GLEIF).
  2. Sinon → recherche par RCS+Pays exact, RCS+Pays approché, puis Nom+Pays
     (avec affinage par code postal si fourni).

Toute correspondance qui n'est pas "Exact RCS + Pays" est marquée
"À vérifier" et générera un avertissement dans la GUI.
"""

import argparse
import datetime
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
    "france": "FR",
    "allemagne": "DE", "germany": "DE",
    "italie": "IT", "italy": "IT",
    "espagne": "ES", "spain": "ES",
    "belgique": "BE", "belgium": "BE",
    "suisse": "CH", "switzerland": "CH",
    "luxembourg": "LU",
    "pays-bas": "NL", "netherlands": "NL", "hollande": "NL",
    "royaume-uni": "GB", "united kingdom": "GB", "uk": "GB",
    "angleterre": "GB", "england": "GB", "grande-bretagne": "GB", "great britain": "GB",
    "etats-unis": "US", "états-unis": "US", "united states": "US",
    "united states of america": "US", "usa": "US",
    "portugal": "PT",
    "autriche": "AT", "austria": "AT",
    "suede": "SE", "suède": "SE", "sweden": "SE",
    "danemark": "DK", "denmark": "DK",
    "norvege": "NO", "norvège": "NO", "norway": "NO",
    "finlande": "FI", "finland": "FI",
    "pologne": "PL", "poland": "PL",
    "republique tcheque": "CZ", "république tchèque": "CZ",
    "czech republic": "CZ", "czechia": "CZ",
    "irlande": "IE", "ireland": "IE",
    "grece": "GR", "grèce": "GR", "greece": "GR",
    "roumanie": "RO", "romania": "RO",
    "hongrie": "HU", "hungary": "HU",
    "bulgarie": "BG", "bulgaria": "BG",
    "croatie": "HR", "croatia": "HR",
    "slovaquie": "SK", "slovakia": "SK",
    "slovenie": "SI", "slovénie": "SI", "slovenia": "SI",
    "serbie": "RS", "serbia": "RS",
    "bosnie-herzegovine": "BA", "bosnie-herzégovine": "BA",
    "bosnia and herzegovina": "BA", "bosnia": "BA",
    "montenegro": "ME", "monténégro": "ME",
    "macedoine du nord": "MK", "macédoine du nord": "MK",
    "north macedonia": "MK", "macedoine": "MK", "macedonia": "MK",
    "albanie": "AL", "albania": "AL",
    "kosovo": "XK",
    "ukraine": "UA",
    "bielorussie": "BY", "biélorussie": "BY", "belarus": "BY",
    "moldavie": "MD", "moldova": "MD",
    "lituanie": "LT", "lithuania": "LT",
    "lettonie": "LV", "latvia": "LV",
    "estonie": "EE", "estonia": "EE",
    "islande": "IS", "iceland": "IS",
    "malte": "MT", "malta": "MT",
    "chypre": "CY", "cyprus": "CY",
    "monaco": "MC",
    "liechtenstein": "LI",
    "andorre": "AD", "andorra": "AD",
    "saint-marin": "SM", "san marino": "SM",
    "vatican": "VA", "saint-siege": "VA", "saint-siège": "VA", "holy see": "VA",
    "georgie": "GE", "géorgie": "GE", "georgia": "GE",
    "armenie": "AM", "arménie": "AM", "armenia": "AM",
    "azerbaidjan": "AZ", "azerbaïdjan": "AZ", "azerbaijan": "AZ",
    "russie": "RU", "russia": "RU",
    "canada": "CA",
    "mexique": "MX", "mexico": "MX",
    "bresil": "BR", "brésil": "BR", "brazil": "BR",
    "argentine": "AR", "argentina": "AR",
    "chili": "CL", "chile": "CL",
    "colombie": "CO", "colombia": "CO",
    "perou": "PE", "pérou": "PE", "peru": "PE",
    "venezuela": "VE",
    "equateur": "EC", "équateur": "EC", "ecuador": "EC",
    "bolivie": "BO", "bolivia": "BO",
    "paraguay": "PY", "uruguay": "UY",
    "guyane": "GY", "guyana": "GY",
    "suriname": "SR",
    "cuba": "CU",
    "haiti": "HT", "haïti": "HT",
    "republique dominicaine": "DO", "république dominicaine": "DO",
    "dominican republic": "DO",
    "jamaique": "JM", "jamaïque": "JM", "jamaica": "JM",
    "trinite-et-tobago": "TT", "trinidad and tobago": "TT", "trinidad": "TT",
    "barbade": "BB", "barbados": "BB",
    "bahamas": "BS", "belize": "BZ",
    "guatemala": "GT", "honduras": "HN",
    "salvador": "SV", "el salvador": "SV",
    "nicaragua": "NI", "costa rica": "CR",
    "panama": "PA", "panamá": "PA",
    "porto rico": "PR", "puerto rico": "PR",
    "chine": "CN", "china": "CN",
    "japon": "JP", "japan": "JP",
    "coree du sud": "KR", "corée du sud": "KR", "south korea": "KR", "korea": "KR",
    "coree du nord": "KP", "corée du nord": "KP", "north korea": "KP",
    "inde": "IN", "india": "IN",
    "pakistan": "PK", "bangladesh": "BD", "sri lanka": "LK",
    "nepal": "NP", "népal": "NP", "bhoutan": "BT", "bhutan": "BT",
    "afghanistan": "AF", "iran": "IR",
    "irak": "IQ", "iraq": "IQ",
    "syrie": "SY", "syria": "SY",
    "liban": "LB", "lebanon": "LB",
    "israel": "IL", "israël": "IL",
    "jordanie": "JO", "jordan": "JO",
    "arabie saoudite": "SA", "saudi arabia": "SA",
    "emirats arabes unis": "AE", "uae": "AE", "united arab emirates": "AE",
    "qatar": "QA",
    "koweit": "KW", "koweït": "KW", "kuwait": "KW",
    "bahrein": "BH", "bahreïn": "BH", "bahrain": "BH",
    "oman": "OM",
    "yemen": "YE", "yémen": "YE",
    "turquie": "TR", "turkey": "TR", "turkiye": "TR", "türkiye": "TR",
    "kazakhstan": "KZ",
    "ouzbekistan": "UZ", "ouzbékistan": "UZ", "uzbekistan": "UZ",
    "turkmenistan": "TM", "turkménistan": "TM",
    "kirghizistan": "KG", "kyrgyzstan": "KG",
    "tadjikistan": "TJ", "tajikistan": "TJ",
    "mongolie": "MN", "mongolia": "MN",
    "myanmar": "MM", "birmanie": "MM", "burma": "MM",
    "thaïlande": "TH", "thailande": "TH", "thailand": "TH",
    "vietnam": "VN", "viet nam": "VN",
    "cambodge": "KH", "cambodia": "KH",
    "laos": "LA",
    "malaisie": "MY", "malaysia": "MY",
    "singapour": "SG", "singapore": "SG",
    "indonesie": "ID", "indonésie": "ID", "indonesia": "ID",
    "philippines": "PH", "taiwan": "TW",
    "hong kong": "HK", "macao": "MO", "macau": "MO",
    "brunei": "BN",
    "timor oriental": "TL", "timor-leste": "TL", "east timor": "TL",
    "maldives": "MV",
    "maroc": "MA", "morocco": "MA",
    "algerie": "DZ", "algérie": "DZ", "algeria": "DZ",
    "tunisie": "TN", "tunisia": "TN",
    "libye": "LY", "libya": "LY",
    "egypte": "EG", "égypte": "EG", "egypt": "EG",
    "soudan": "SD", "sudan": "SD",
    "soudan du sud": "SS", "south sudan": "SS",
    "ethiopie": "ET", "éthiopie": "ET", "ethiopia": "ET",
    "erythree": "ER", "érythrée": "ER", "eritrea": "ER",
    "djibouti": "DJ",
    "somalie": "SO", "somalia": "SO",
    "kenya": "KE",
    "tanzanie": "TZ", "tanzania": "TZ",
    "ouganda": "UG", "uganda": "UG",
    "rwanda": "RW", "burundi": "BI",
    "mozambique": "MZ", "zimbabwe": "ZW",
    "zambie": "ZM", "zambia": "ZM",
    "malawi": "MW", "madagascar": "MG",
    "ile maurice": "MU", "maurice": "MU", "mauritius": "MU",
    "comores": "KM", "comoros": "KM",
    "seychelles": "SC",
    "afrique du sud": "ZA", "south africa": "ZA",
    "namibie": "NA", "namibia": "NA",
    "botswana": "BW", "lesotho": "LS",
    "swaziland": "SZ", "eswatini": "SZ",
    "angola": "AO",
    "congo": "CG", "republique du congo": "CG", "republic of the congo": "CG",
    "republique democratique du congo": "CD", "rdc": "CD",
    "democratic republic of the congo": "CD", "drc": "CD", "zaire": "CD",
    "gabon": "GA",
    "cameroun": "CM", "cameroon": "CM",
    "guinee equatoriale": "GQ", "equatorial guinea": "GQ",
    "sao tome-et-principe": "ST", "sao tome and principe": "ST",
    "nigeria": "NG", "ghana": "GH",
    "cote d'ivoire": "CI", "cote divoire": "CI", "ivory coast": "CI",
    "liberia": "LR", "sierra leone": "SL",
    "guinee": "GN", "guinée": "GN", "guinea": "GN",
    "guinee-bissau": "GW", "guinea-bissau": "GW",
    "senegal": "SN", "sénégal": "SN",
    "gambie": "GM", "gambia": "GM",
    "mauritanie": "MR", "mauritania": "MR",
    "mali": "ML", "burkina faso": "BF",
    "niger": "NE",
    "tchad": "TD", "chad": "TD",
    "togo": "TG",
    "benin": "BJ", "bénin": "BJ",
    "cap-vert": "CV", "cape verde": "CV", "cabo verde": "CV",
    "centrafrique": "CF", "republique centrafricaine": "CF",
    "central african republic": "CF",
    "australie": "AU", "australia": "AU",
    "nouvelle-zelande": "NZ", "nouvelle-zélande": "NZ", "new zealand": "NZ",
    "papouasie-nouvelle-guinee": "PG", "papua new guinea": "PG",
    "fidji": "FJ", "fiji": "FJ",
    "vanuatu": "VU",
    "salomon": "SB", "solomon islands": "SB",
    "samoa": "WS", "tonga": "TO",
    "kiribati": "KI", "tuvalu": "TV", "nauru": "NR",
    "marshall": "MH", "marshall islands": "MH",
    "micronesie": "FM", "micronésie": "FM", "micronesia": "FM",
    "palaos": "PW", "palau": "PW",
    "curacao": "CW", "curaçao": "CW",
    "sint maarten": "SX", "saint-martin neerlandais": "SX",
    "antilles neerlandaises": "AN", "netherlands antilles": "AN",
    "bermudes": "BM", "bermuda": "BM",
    "ile de man": "IM", "île de man": "IM", "isle of man": "IM",
    "anguilla": "AI",
    "nouvelle-caledonie": "NC", "nouvelle-calédonie": "NC", "new caledonia": "NC",
    "polynesie francaise": "PF", "polynésie française": "PF", "french polynesia": "PF",
    "supranational": "XD",
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
# Schéma GLEIF
# ─────────────────────────────────────────────────────────────────────────────
GLEIF_COLUMN_CANDIDATES: Dict[str, List[str]] = {
    "lei":           ["LEI"],
    "name":          ["Entity.LegalName", "Entity.LegalName.name"],
    "country":       ["Entity.LegalAddress.Country", "Entity.LegalAddress.country"],
    "entity_status": ["Entity.EntityStatus", "Entity.Status"],
    "lei_status":    ["Registration.RegistrationStatus", "Registration.Status"],
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
    "renewal_date": [
        "Registration.NextRenewalDate",
        "Registration.NextRenewal",
        "Entity.Registration.NextRenewalDate",
    ],
    "postal_code": [
        "Entity.LegalAddress.PostalCode",
        "Entity.LegalAddress.postalCode",
        "Entity.LegalAddress.PostCode",
        "Entity.LegalAddress.postal_code",
    ],
}

SLIM_COLUMNS = list(GLEIF_COLUMN_CANDIDATES.keys())
GLEIF_CHUNK_SIZE = 100_000

# Seuil DQ pour le calcul de la discordance "nom" (plus exigeant que le seuil de matching)
NAME_DQ_THRESHOLD = 85


# ─────────────────────────────────────────────────────────────────────────────
# Détection schéma GLEIF
# ─────────────────────────────────────────────────────────────────────────────

def _detect_gleif_columns(available_cols: List[str]) -> Tuple[Dict[str, str], List[str]]:
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
                f"(candidats : {candidates}). Colonne laissée vide."
            )
    log.info(f"Mapping colonnes GLEIF : {col_map}")
    if missing:
        log.warning(f"Colonnes absentes (vides) : {missing}")
    return col_map, missing


# ─────────────────────────────────────────────────────────────────────────────
# Normalisation
# ─────────────────────────────────────────────────────────────────────────────

def normalize_rcs(value) -> str:
    if pd.isna(value) or str(value).strip() == "":
        return ""
    raw = str(value)
    raw = unicodedata.normalize("NFKC", raw).upper()
    raw = "".join(
        str(unicodedata.digit(c, -1)) if unicodedata.category(c) == "Nd" and not c.isascii() else c
        for c in raw
    )
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


def normalize_date(value) -> Optional[datetime.date]:
    if pd.isna(value) or str(value).strip() in ("", "nan", "NaT", "None"):
        return None
    raw = str(value).strip()
    if "T" in raw:
        raw = raw.split("T")[0]
    elif len(raw) > 10 and raw[10] in (" ", "+"):
        raw = raw[:10]
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def normalize_postal_code(value) -> str:
    if pd.isna(value) or str(value).strip() == "":
        return ""
    return re.sub(r"[^0-9]", "", str(value))


# ─────────────────────────────────────────────────────────────────────────────
# Lecture Excel sécurisée (OneDrive)
# ─────────────────────────────────────────────────────────────────────────────

def _safe_read_excel(path: str) -> pd.DataFrame:
    p = Path(path)
    try:
        return pd.read_excel(path, dtype=str)
    except PermissionError:
        log.warning(f"Permission refusée sur '{p.name}'. Copie temporaire en cours…")
        tmp_dir = Path(tempfile.gettempdir()) / "gleif_match"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = tmp_dir / p.name
        shutil.copy2(path, str(tmp_path))
        log.info(f"Lecture depuis copie temporaire : {tmp_path}")
        return pd.read_excel(str(tmp_path), dtype=str)


# ─────────────────────────────────────────────────────────────────────────────
# Chargement GLEIF (CSV par chunks)
# ─────────────────────────────────────────────────────────────────────────────

def load_gleif(
    gleif_path: str,
    active_only: bool = True,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    status_cb: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    def _status(msg: str):
        log.info(msg)
        if status_cb:
            status_cb(msg)

    path = Path(gleif_path)
    suffix = path.suffix.lower()
    _status(f"Chargement GLEIF : {path.name} …")

    if suffix == ".json":
        _status("Format JSON — lecture complète en mémoire…")
        raw = pd.read_json(gleif_path, dtype=str)
        if "LEI" not in raw.columns:
            raw = pd.json_normalize(raw.to_dict(orient="records"))
        return _finalize_gleif_df(raw, active_only)

    header_df = pd.read_csv(gleif_path, nrows=0, dtype=str, low_memory=False)
    available_cols = list(header_df.columns)
    _slim_markers = {"lei", "name", "country", "entity_status", "lei_status"}
    is_slim_format = _slim_markers.issubset(set(available_cols))

    if is_slim_format:
        _status("Format slim détecté — chargement direct.")
        if not active_only:
            log.warning(
                "Base slim + mode validation : entités LAPSED absentes — "
                "fallback RCS/nom limité aux LEI ACTIFS."
            )
            if status_cb:
                status_cb("⚠ Base slim : LEI expirés non couverts.")
        usecols = [col for col in SLIM_COLUMNS if col in available_cols]
        col_map = None
    else:
        col_map, _ = _detect_gleif_columns(available_cols)
        usecols = list(set(col_map.values()))

    _status(f"Colonnes retenues : {len(usecols)} / {len(available_cols)} — lecture par chunks…")

    try:
        file_size = path.stat().st_size
        estimated_total_chunks = max(1, file_size // (200 * GLEIF_CHUNK_SIZE))
    except Exception:
        estimated_total_chunks = 200

    chunks: List[pd.DataFrame] = []
    chunks_read = 0
    reader = pd.read_csv(
        gleif_path, usecols=usecols, dtype=str, low_memory=False,
        chunksize=GLEIF_CHUNK_SIZE, on_bad_lines="skip",
    )

    for chunk in reader:
        if not is_slim_format and col_map:
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
            chunks.append(chunk)
        chunks_read += 1
        if progress_cb:
            progress_cb(chunks_read, estimated_total_chunks)

    if not chunks:
        log.warning("Aucune entité retenue après filtrage.")
        return pd.DataFrame(columns=SLIM_COLUMNS)

    df = pd.concat(chunks, ignore_index=True)
    _status(
        f"  Chargement terminé : {len(df):,} entités "
        f"({'ACTIVE+ISSUED' if active_only else 'tous statuts'})"
    )
    return df


def _finalize_gleif_df(raw: pd.DataFrame, active_only: bool) -> pd.DataFrame:
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
# Préparation base slim
# ─────────────────────────────────────────────────────────────────────────────

def prepare_slim(
    input_csv: str,
    output_csv: str,
    active_only: bool = True,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    status_cb: Optional[Callable[[str], None]] = None,
) -> int:
    def _status(msg: str):
        log.info(msg)
        if status_cb:
            status_cb(msg)

    path_in, path_out = Path(input_csv), Path(output_csv)
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
        str(path_in), usecols=usecols, dtype=str, low_memory=False,
        chunksize=GLEIF_CHUNK_SIZE, on_bad_lines="skip",
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
                str(path_out), mode="w" if first_chunk else "a",
                header=first_chunk, index=False, encoding="utf-8",
            )
            total_written += len(chunk)
            first_chunk = False
        chunks_read += 1
        if progress_cb:
            progress_cb(chunks_read, estimated_chunks)

    _status(f"Base slim générée : {total_written:,} entités → {path_out.name}")
    return total_written


# ─────────────────────────────────────────────────────────────────────────────
# Index — RCS composite (rcs_norm, iso_pays)
# ─────────────────────────────────────────────────────────────────────────────

def build_indices(
    df: pd.DataFrame,
) -> Tuple[
    Dict[Tuple[str, str], List[int]],   # rcs_index : (rcs_norm, iso) → indices
    Dict[str, Dict[str, List[int]]],     # name_index : iso → nom → indices
    Dict[str, int],                      # lei_index : LEI → indice
    Dict[str, List[int]],                # rcs_country_agnostic : rcs_norm → indices (pour audit)
]:
    """
    Construit les index de recherche.

    rcs_index est désormais COMPOSITE (rcs_norm, iso_pays). Un même numéro de
    registre dans deux pays différents n'est plus collisionnant — c'est le
    correctif fiabilité v2.0 demandé par le Middle Office SG.

    rcs_country_agnostic est conservé en parallèle pour permettre une recherche
    de fallback explicite quand le pays client est absent (avec flag de
    fiabilité dégradée à l'appelant).
    """
    log.info("Construction des index …")
    rcs_index: Dict[Tuple[str, str], List[int]] = {}
    rcs_country_agnostic: Dict[str, List[int]] = {}
    lei_index: Dict[str, int] = {}

    for i, (lei, ra_entity, country) in enumerate(zip(df["lei"], df["ra_entity"], df["country"])):
        key_rcs = normalize_rcs(ra_entity)
        iso = str(country).strip().upper()
        if key_rcs:
            rcs_country_agnostic.setdefault(key_rcs, []).append(i)
            if iso:
                rcs_index.setdefault((key_rcs, iso), []).append(i)
        key_lei = str(lei).strip().upper()
        if key_lei:
            lei_index[key_lei] = i

    name_index: Dict[str, Dict[str, List[int]]] = {}
    for i, (country, name) in enumerate(zip(df["country"], df["name"])):
        c = str(country).strip().upper()
        n = normalize_name(name)
        if c and n:
            name_index.setdefault(c, {}).setdefault(n, []).append(i)

    log.info(
        f"  Index RCS+Pays : {len(rcs_index):,}  | "
        f"Index LEI : {len(lei_index):,}  | "
        f"Index Nom : {sum(len(v) for v in name_index.values()):,}"
    )
    return rcs_index, name_index, lei_index, rcs_country_agnostic


# ─────────────────────────────────────────────────────────────────────────────
# Recherches
# ─────────────────────────────────────────────────────────────────────────────

def search_by_rcs(
    rcs_norm: str,
    iso_country: str,
    rcs_index: Dict[Tuple[str, str], List[int]],
    df: pd.DataFrame,
    rcs_country_agnostic: Optional[Dict[str, List[int]]] = None,
) -> Tuple[Optional[pd.Series], str]:
    """
    Recherche RCS exacte avec contrôle pays.

    Retourne (row, country_status) où country_status ∈ {"strict", "agnostic", "none"} :
      • "strict"    : match (RCS, Pays) exact dans l'index composite — fiable
      • "agnostic"  : pays client absent → fallback sur RCS seul (À vérifier)
      • "none"      : aucun match
    """
    if not rcs_norm:
        return None, "none"
    if iso_country:
        idxs = rcs_index.get((rcs_norm, iso_country))
        if idxs:
            return df.iloc[idxs[0]], "strict"
        return None, "none"
    # Pays client absent : fallback country-agnostic (signalé par le caller)
    if rcs_country_agnostic is not None:
        idxs = rcs_country_agnostic.get(rcs_norm)
        if idxs:
            return df.iloc[idxs[0]], "agnostic"
    return None, "none"


def search_by_rcs_fuzzy(
    rcs_norm: str,
    iso_country: str,
    rcs_index: Dict[Tuple[str, str], List[int]],
    df: pd.DataFrame,
    threshold: int = 88,
    rcs_country_agnostic: Optional[Dict[str, List[int]]] = None,
) -> Tuple[Optional[pd.Series], int, str]:
    """
    Recherche approximative par contenance, restreinte au pays cible si fourni.
    Score = len(client) / len(gleif) × 100.

    Retourne (row, score, country_status).
    """
    if not rcs_norm or len(rcs_norm) < 4:
        return None, 0, "none"
    n = len(rcs_norm)
    best_row, best_score, best_status = None, 0, "none"

    if iso_country:
        # Cherche uniquement parmi les RCS du pays cible
        for (key, key_iso), idxs in rcs_index.items():
            if key_iso != iso_country:
                continue
            key_len = len(key)
            if key_len < n or (key_len - n) > 2:
                continue
            if rcs_norm in key:
                score = round(n / key_len * 100)
                if score >= threshold and score > best_score:
                    best_score, best_row, best_status = score, df.iloc[idxs[0]], "strict"
        if best_row is not None:
            return best_row, best_score, best_status

    # Fallback country-agnostic
    if rcs_country_agnostic is not None:
        for key, idxs in rcs_country_agnostic.items():
            key_len = len(key)
            if key_len < n or (key_len - n) > 2:
                continue
            if rcs_norm in key:
                score = round(n / key_len * 100)
                if score >= threshold and score > best_score:
                    best_score, best_row, best_status = score, df.iloc[idxs[0]], "agnostic"

    return best_row, best_score, best_status


def search_by_lei(
    lei_val: str,
    lei_index: Dict[str, int],
    df: pd.DataFrame,
) -> Optional[pd.Series]:
    key = str(lei_val).strip().upper()
    if not key:
        return None
    idx = lei_index.get(key)
    return df.iloc[idx] if idx is not None else None


def search_by_name_country(
    name_norm: str,
    iso_country: str,
    name_index: Dict[str, Dict[str, List[int]]],
    df: pd.DataFrame,
    threshold: int = 90,
    client_postal_digits: str = "",
) -> Tuple[Optional[pd.Series], int]:
    if not name_norm or not iso_country:
        return None, 0
    country_names = name_index.get(iso_country, {})
    if not country_names:
        return None, 0

    if not client_postal_digits:
        result = process.extractOne(
            name_norm, list(country_names.keys()),
            scorer=fuzz.token_sort_ratio, score_cutoff=threshold,
        )
        if result is None:
            return None, 0
        best_name, score, _ = result
        return df.iloc[country_names[best_name][0]], int(score)

    candidates = process.extract(
        name_norm, list(country_names.keys()),
        scorer=fuzz.token_sort_ratio, score_cutoff=threshold, limit=10,
    )
    if not candidates:
        return None, 0
    for cand_name, name_score, _ in candidates:
        row = df.iloc[country_names[cand_name][0]]
        gleif_postal = str(row.get("postal_code", "")).strip()
        if gleif_postal and client_postal_digits in gleif_postal:
            return row, int(name_score)
    best_name, score, _ = candidates[0]
    return df.iloc[country_names[best_name][0]], int(score)


# ─────────────────────────────────────────────────────────────────────────────
# Discordance "métier" — messages lisibles par un humain
# ─────────────────────────────────────────────────────────────────────────────

def _trunc(s: str, max_len: int = 60) -> str:
    s = str(s).strip()
    return s if len(s) <= max_len else s[: max_len - 1] + "…"


def compute_discordances(
    gleif_row: pd.Series,
    client_name: str = "",
    client_rcs: str = "",
    client_pays_iso: str = "",
    client_lei: str = "",
    client_date: str = "",
    name_threshold: int = NAME_DQ_THRESHOLD,
) -> Dict[str, str]:
    """
    Génère 3 messages métier :
      • disc_nom : « Nom différent : "X" vs "Y" »   ou ""
      • disc_rcs : « RCS différent : "X" vs "Y" »  /  « Pays différent : FR vs DE »  ou ""
      • disc_lei : « LEI différent : "X" vs "Y" »  /  « LEI absent côté source »  ou ""

    La date est intégrée dans disc_lei pour réduire la surface (cohérence métier :
    la date de validité est attachée au LEI).
    """
    out = {"nom": "", "rcs": "", "lei": ""}

    name_g = str(gleif_row.get("name", "")).strip()
    rcs_g  = str(gleif_row.get("ra_entity", "")).strip()
    pays_g = str(gleif_row.get("country", "")).strip().upper()
    lei_g  = str(gleif_row.get("lei", "")).strip()
    date_g_raw = str(gleif_row.get("renewal_date", "")).strip()
    date_g = normalize_date(date_g_raw)

    # ── Discordance Nom ─────────────────────────────────────────────────────
    name_c = (client_name or "").strip()
    if name_c and name_g:
        n_c, n_g = normalize_name(name_c), normalize_name(name_g)
        if n_c and n_g and fuzz.token_sort_ratio(n_c, n_g) < name_threshold:
            out["nom"] = f'Nom différent : "{_trunc(name_c)}" vs "{_trunc(name_g)}"'
    elif name_c and not name_g:
        out["nom"] = f'Nom GLEIF absent (source : "{_trunc(name_c)}")'
    elif name_g and not name_c:
        out["nom"] = f'Nom source absent (GLEIF : "{_trunc(name_g)}")'

    # ── Discordance RCS / Pays ──────────────────────────────────────────────
    rcs_c = (client_rcs or "").strip()
    rcs_msgs: List[str] = []
    if rcs_c and rcs_g:
        if normalize_rcs(rcs_c) != normalize_rcs(rcs_g):
            rcs_msgs.append(f'RCS différent : "{_trunc(rcs_c, 30)}" vs "{_trunc(rcs_g, 30)}"')
    elif rcs_c and not rcs_g:
        rcs_msgs.append(f'RCS GLEIF absent (source : "{_trunc(rcs_c, 30)}")')
    elif rcs_g and not rcs_c:
        rcs_msgs.append(f'RCS source absent (GLEIF : "{_trunc(rcs_g, 30)}")')

    iso_c = (client_pays_iso or "").strip().upper()
    if iso_c and pays_g and iso_c != pays_g:
        rcs_msgs.append(f'Pays différent : {iso_c} vs {pays_g}')
    elif not iso_c and pays_g:
        rcs_msgs.append(f'Pays source absent (GLEIF : {pays_g})')

    out["rcs"] = " | ".join(rcs_msgs)

    # ── Discordance LEI / Date ──────────────────────────────────────────────
    lei_c = (client_lei or "").strip()
    lei_msgs: List[str] = []
    if lei_c and lei_g:
        if lei_c.upper() != lei_g.upper():
            lei_msgs.append(f'LEI différent : "{lei_c}" vs "{lei_g}"')
    elif lei_c and not lei_g:
        lei_msgs.append(f'LEI GLEIF absent (source : "{lei_c}")')
    elif lei_g and not lei_c:
        lei_msgs.append(f'LEI source absent (GLEIF : "{lei_g}")')

    date_c = normalize_date(client_date) if client_date else None
    if date_c and date_g and date_c != date_g:
        lei_msgs.append(
            f'Date validité différente : {date_c.strftime("%d-%m-%Y")} '
            f'vs {date_g.strftime("%d-%m-%Y")}'
        )
    elif not date_c and date_g and client_date:
        lei_msgs.append(f'Date validité source illisible (GLEIF : {date_g.strftime("%d-%m-%Y")})')

    out["lei"] = " | ".join(lei_msgs)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Fiabilité + action requise
# ─────────────────────────────────────────────────────────────────────────────

OK = "OK"
A_VERIFIER = "À vérifier"
KO = "KO"

ACTION_OK = "Aucune"
ACTION_VERIF = "Vérification manuelle des libellés/pays requise"
ACTION_VERIF_PAYS_ABSENT = "Vérification manuelle requise (pays source absent)"
ACTION_KO = "Rechercher manuellement sur le site GLEIF ou contacter l'entité"


def compute_fiabilite(
    match_type: str,
    country_status: str,           # "strict" | "agnostic" | "none" | "n/a"
    discordances: Dict[str, str],
    gleif_row: Optional[pd.Series],
) -> Tuple[str, str]:
    """
    Détermine (Fiabilite, ActionRequise) selon les règles métier SG.

    OK         → Match exact RCS + Pays cohérent + aucune discordance significative
                 OU LEI Valide (lookup direct + données cohérentes)
    À vérifier → Toute approximation, ou pays source absent, ou discordance résiduelle
    KO         → LEI Discordant, Non trouvé, ou tout cas où aucune entité GLEIF
                 fiable ne peut être proposée
    """
    has_disc = any(v for v in discordances.values())

    if match_type in ("Non trouvé", "Non trouvé (LEI invalide)"):
        return KO, ACTION_KO
    if match_type == "LEI Discordant":
        return KO, ACTION_KO

    if match_type == "LEI Valide":
        if has_disc:
            return A_VERIFIER, ACTION_VERIF
        return OK, ACTION_OK

    if match_type == "Exact – RCS":
        if country_status == "agnostic":
            return A_VERIFIER, ACTION_VERIF_PAYS_ABSENT
        if has_disc:
            return A_VERIFIER, ACTION_VERIF
        return OK, ACTION_OK

    if match_type in ("Approx – RCS", "Approx – Nom/Pays"):
        if country_status == "agnostic":
            return A_VERIFIER, ACTION_VERIF_PAYS_ABSENT
        return A_VERIFIER, ACTION_VERIF

    # Type inconnu → conservatisme
    return A_VERIFIER, ACTION_VERIF


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────────────────────────────────────

def match_companies(
    input_path: str,
    gleif_path: str,
    output_path: str,
    col_rcs: str = "RCS",
    col_name: str = "NomEntreprise",
    col_pays: str = "Pays",
    col_lei: Optional[str] = None,
    col_date: Optional[str] = None,
    col_postal: Optional[str] = None,
    fuzzy_threshold: int = 90,
    rcs_fuzzy_threshold: int = 88,
    active_only: bool = True,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    status_cb: Optional[Callable[[str], None]] = None,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Pipeline complet de rapprochement v2.0.

    Retourne (df_output, stats) où stats contient les compteurs par fiabilité
    et par type de correspondance.
    """
    def _status(msg):
        log.info(msg)
        if status_cb:
            status_cb(msg)

    _status(f"Lecture du fichier d'entrée : {input_path}")
    df_input = _safe_read_excel(input_path).fillna("")
    _status(f"  {len(df_input):,} lignes chargées")

    required = [c for c in [col_rcs, col_name, col_pays] if c]
    missing_cols = [c for c in required if c not in df_input.columns]
    if missing_cols:
        raise ValueError(
            f"Colonnes manquantes dans le fichier d'entrée : {missing_cols}\n"
            f"Colonnes disponibles : {list(df_input.columns)}"
        )

    has_lei_col    = bool(col_lei)    and col_lei    in df_input.columns
    has_date_col   = bool(col_date)   and col_date   in df_input.columns
    has_postal_col = bool(col_postal) and col_postal in df_input.columns

    if has_lei_col:
        _status(f"  Mode validation LEI activé via '{col_lei}' — chargement tous statuts.")
    _active_only_load = active_only if not has_lei_col else False

    df_gleif = load_gleif(gleif_path, active_only=_active_only_load, status_cb=status_cb)
    rcs_index, name_index, lei_index, rcs_agnostic = build_indices(df_gleif)

    results = []
    n_total = len(df_input)
    stats = {
        "total": n_total,
        "ok": 0, "a_verifier": 0, "ko": 0,
        "exact_rcs": 0, "approx_rcs": 0, "approx_nom": 0,
        "lei_valide": 0, "lei_discordant": 0, "lei_invalide": 0,
        "non_trouve": 0,
    }

    _status("Rapprochement en cours …")

    for idx, row in df_input.iterrows():
        rcs_raw    = str(row[col_rcs]).strip()    if col_rcs    in df_input.columns else ""
        name_raw   = str(row[col_name]).strip()   if col_name   in df_input.columns else ""
        pays_raw   = str(row[col_pays]).strip()   if col_pays   in df_input.columns else ""
        lei_exist  = str(row[col_lei]).strip()    if has_lei_col    else ""
        date_raw   = str(row[col_date]).strip()   if has_date_col   else ""
        postal_raw = str(row[col_postal]).strip() if has_postal_col else ""

        rcs_norm      = normalize_rcs(rcs_raw)
        name_norm     = normalize_name(name_raw)
        iso           = country_to_iso(pays_raw)
        postal_digits = normalize_postal_code(postal_raw) if postal_raw else ""

        gleif_row: Optional[pd.Series] = None
        match_type     = "Non trouvé"
        country_status = "n/a"

        # ── Mode 1 : validation LEI existant ─────────────────────────────────
        if lei_exist:
            gleif_row = search_by_lei(lei_exist, lei_index, df_gleif)
            if gleif_row is not None:
                lei_g = str(gleif_row.get("lei", "")).strip().upper()
                lei_c = lei_exist.strip().upper()
                if lei_c and lei_g and lei_c != lei_g:
                    match_type = "LEI Discordant"
                    stats["lei_discordant"] += 1
                else:
                    match_type = "LEI Valide"
                    stats["lei_valide"] += 1
                # Pour un lookup LEI direct, le pays GLEIF fait foi → strict
                country_status = "strict"
            else:
                # Fallback RCS+Pays / Nom+Pays
                fallback_row = None
                if rcs_norm:
                    fb_row, fb_status = search_by_rcs(
                        rcs_norm, iso, rcs_index, df_gleif, rcs_agnostic
                    )
                    if fb_row is None and rcs_fuzzy_threshold < 100:
                        fb_row, _, fb_status = search_by_rcs_fuzzy(
                            rcs_norm, iso, rcs_index, df_gleif,
                            rcs_fuzzy_threshold, rcs_agnostic,
                        )
                    if fb_row is not None:
                        fallback_row = fb_row
                        country_status = fb_status
                if fallback_row is None and name_norm and iso:
                    fb_row, _ = search_by_name_country(
                        name_norm, iso, name_index, df_gleif,
                        fuzzy_threshold, postal_digits,
                    )
                    if fb_row is not None:
                        fallback_row = fb_row
                        country_status = "strict"  # name_country est par construction iso-strict

                if fallback_row is not None:
                    gleif_row = fallback_row
                    match_type = "LEI Discordant"
                    stats["lei_discordant"] += 1
                else:
                    match_type = "Non trouvé (LEI invalide)"
                    stats["lei_invalide"] += 1

        # ── Mode 2 : recherche d'un LEI manquant ─────────────────────────────
        else:
            # 2a. RCS + Pays exact
            if rcs_norm:
                gleif_row, country_status = search_by_rcs(
                    rcs_norm, iso, rcs_index, df_gleif, rcs_agnostic
                )
                if gleif_row is not None:
                    if active_only:
                        es = str(gleif_row.get("entity_status", "")).upper()
                        ls = str(gleif_row.get("lei_status", "")).upper()
                        if es != "ACTIVE" or ls != "ISSUED":
                            gleif_row = None
                            country_status = "n/a"
                    if gleif_row is not None:
                        match_type = "Exact – RCS"
                        stats["exact_rcs"] += 1

            # 2b. RCS approché (zéros de tête, fautes mineures)
            if gleif_row is None and rcs_norm and rcs_fuzzy_threshold < 100:
                approx_r, _rcs_sc, fuzzy_status = search_by_rcs_fuzzy(
                    rcs_norm, iso, rcs_index, df_gleif,
                    rcs_fuzzy_threshold, rcs_agnostic,
                )
                if approx_r is not None:
                    if active_only:
                        es = str(approx_r.get("entity_status", "")).upper()
                        ls = str(approx_r.get("lei_status", "")).upper()
                        if es != "ACTIVE" or ls != "ISSUED":
                            approx_r = None
                    if approx_r is not None:
                        match_type = "Approx – RCS"
                        gleif_row = approx_r
                        country_status = fuzzy_status
                        stats["approx_rcs"] += 1

            # 2c. Nom + Pays
            if gleif_row is None and name_norm and iso:
                row_nm, _ = search_by_name_country(
                    name_norm, iso, name_index, df_gleif,
                    fuzzy_threshold, postal_digits,
                )
                if row_nm is not None:
                    if active_only:
                        es = str(row_nm.get("entity_status", "")).upper()
                        ls = str(row_nm.get("lei_status", "")).upper()
                        if es != "ACTIVE" or ls != "ISSUED":
                            row_nm = None
                    if row_nm is not None:
                        match_type = "Approx – Nom/Pays"
                        gleif_row = row_nm
                        country_status = "strict"
                        stats["approx_nom"] += 1

            if gleif_row is None:
                stats["non_trouve"] += 1

        # ── Discordances + Fiabilité ─────────────────────────────────────────
        if gleif_row is not None:
            disc = compute_discordances(
                gleif_row,
                client_name=name_raw, client_rcs=rcs_raw,
                client_pays_iso=iso, client_lei=lei_exist,
                client_date=date_raw,
            )
        else:
            disc = {"nom": "", "rcs": "", "lei": ""}

        fiabilite, action = compute_fiabilite(match_type, country_status, disc, gleif_row)
        if   fiabilite == OK:         stats["ok"] += 1
        elif fiabilite == A_VERIFIER: stats["a_verifier"] += 1
        else:                          stats["ko"] += 1

        # Construction de la ligne résultat (blocs thématiques)
        result_row = {
            # ── Bloc Identité ──
            "Nom_Source":       name_raw,
            "Nom_GLEIF":        gleif_row["name"] if gleif_row is not None else "",
            "Discordance_Nom":  disc["nom"],
            # ── Bloc Légal ──
            "RCS_Source":       rcs_raw,
            "Pays_Source":      iso or pays_raw,
            "RCS_GLEIF":        gleif_row["ra_entity"] if gleif_row is not None else "",
            "Pays_GLEIF":       gleif_row["country"]   if gleif_row is not None else "",
            "Discordance_RCS":  disc["rcs"],
            # ── Bloc LEI ──
            "LEI_Source":            lei_exist,
            "LEI_GLEIF":             gleif_row["lei"]            if gleif_row is not None else "",
            "Statut_LEI_GLEIF":      gleif_row["lei_status"]     if gleif_row is not None else "",
            "DateValidite_LEI_GLEIF": gleif_row["renewal_date"]  if gleif_row is not None else "",
            "Discordance_LEI":       disc["lei"],
            # ── Bloc Synthèse ──
            "TypeCorrespondance":  match_type,
            "Fiabilite":           fiabilite,
            "ActionRequise":       action,
        }
        results.append(result_row)

        if progress_cb and ((idx + 1) % 10 == 0 or (idx + 1) == n_total):
            progress_cb(idx + 1, n_total)

    df_results = pd.DataFrame(results)
    df_output = pd.concat([df_input.reset_index(drop=True), df_results], axis=1)

    _status(f"Export vers : {output_path}")
    _export_excel(df_output, output_path, list(df_input.columns), fuzzy_threshold, stats)

    log.info(
        f"\n{'='*55}\n"
        f"  Total           : {stats['total']:>6,}\n"
        f"  OK              : {stats['ok']:>6,}  ({stats['ok']/n_total*100:.1f}%)\n"
        f"  À vérifier      : {stats['a_verifier']:>6,}  ({stats['a_verifier']/n_total*100:.1f}%)\n"
        f"  KO              : {stats['ko']:>6,}  ({stats['ko']/n_total*100:.1f}%)\n"
        f"  ─ détail ─\n"
        f"  Exact RCS+Pays  : {stats['exact_rcs']:>6,}\n"
        f"  Approx RCS      : {stats['approx_rcs']:>6,}\n"
        f"  Approx Nom/Pays : {stats['approx_nom']:>6,}\n"
        f"  LEI Valide      : {stats['lei_valide']:>6,}\n"
        f"  LEI Discordant  : {stats['lei_discordant']:>6,}\n"
        f"  LEI Invalide    : {stats['lei_invalide']:>6,}\n"
        f"  Non trouvé      : {stats['non_trouve']:>6,}\n"
        f"{'='*55}"
    )
    return df_output, stats


# ─────────────────────────────────────────────────────────────────────────────
# Export Excel — blocs thématiques + Instructions
# ─────────────────────────────────────────────────────────────────────────────

# Codes couleur Société Générale
SG_RED   = "E60028"
SG_BLACK = "000000"
SG_GREY  = "6B7280"

# Couleurs lignes (pleines, harmonisées avec la GUI)
ROW_OK_FILL     = "D4EDDA"  # vert clair
ROW_VERIF_FILL  = "FFF3CD"  # jaune
ROW_KO_FILL     = "F8D7DA"  # rouge clair

# Couleurs en-têtes des blocs (plus saturées)
BLOCK_IDENT_HDR  = "1F4E79"  # bleu foncé — Identité
BLOCK_LEGAL_HDR  = "2E5984"  # bleu — Légal
BLOCK_LEI_HDR    = "5B5EA6"  # violet — LEI
BLOCK_SYNTH_HDR  = SG_RED    # rouge SG — Synthèse (focus)
BLOCK_INPUT_HDR  = "404040"  # gris foncé — Données source

DISCLAIMER_TEXT = (
    "⚠ AVERTISSEMENT — Outil d'aide à la décision. Les correspondances "
    "approximatives doivent être validées manuellement avant usage opérationnel. "
    "Cet outil ne remplace pas le contrôle humain réglementaire."
)


def _export_excel(
    df: pd.DataFrame,
    output_path: str,
    input_columns: List[str],
    threshold: int,
    stats: Dict[str, int],
) -> None:
    """
    Export Excel restructuré v2.0 :
      • Onglet Résultats : colonnes source + 4 blocs (Identité, Légal, LEI, Synthèse)
      • Onglet Instructions : disclaimer + légende couleurs + logique de fiabilité
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Résultats"

    thin   = Side(style="thin", color="AAAAAA")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    base_font  = Font(name="Calibri", size=10)
    bold_white = Font(name="Calibri", bold=True, color="FFFFFF", size=10)
    bold_black = Font(name="Calibri", bold=True, size=10)
    disc_font  = Font(name="Calibri", size=10, color="C00000", bold=True)

    # ── Définition des blocs ────────────────────────────────────────────────
    blocks = [
        ("DONNÉES SOURCE",                input_columns,                         BLOCK_INPUT_HDR),
        ("BLOC IDENTITÉ",                 ["Nom_Source", "Nom_GLEIF", "Discordance_Nom"],
                                                                                 BLOCK_IDENT_HDR),
        ("BLOC LÉGAL (RCS + Pays)",       ["RCS_Source", "Pays_Source",
                                           "RCS_GLEIF", "Pays_GLEIF",
                                           "Discordance_RCS"],                   BLOCK_LEGAL_HDR),
        ("BLOC LEI",                      ["LEI_Source", "LEI_GLEIF",
                                           "Statut_LEI_GLEIF",
                                           "DateValidite_LEI_GLEIF",
                                           "Discordance_LEI"],                   BLOCK_LEI_HDR),
        ("SYNTHÈSE",                      ["TypeCorrespondance",
                                           "Fiabilite", "ActionRequise"],        BLOCK_SYNTH_HDR),
    ]

    # ── Construction de l'ordre final des colonnes ──────────────────────────
    final_columns: List[str] = []
    for _, cols, _ in blocks:
        for c in cols:
            if c in df.columns and c not in final_columns:
                final_columns.append(c)
    # Filet de sécurité : ajouter toute colonne oubliée
    for c in df.columns:
        if c not in final_columns:
            final_columns.append(c)

    df = df[final_columns]
    disc_cols = {"Discordance_Nom", "Discordance_RCS", "Discordance_LEI"}

    # ── Ligne 1 : bandeau disclaimer ────────────────────────────────────────
    ws.cell(row=1, column=1, value=DISCLAIMER_TEXT)
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(final_columns))
    c = ws.cell(row=1, column=1)
    c.fill = PatternFill("solid", fgColor="FFF3CD")
    c.font = Font(name="Calibri", bold=True, size=10, color="856404")
    c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    ws.row_dimensions[1].height = 32

    # ── Ligne 2 : en-tête de bloc ───────────────────────────────────────────
    col_pointer = 1
    for block_name, cols, hdr_color in blocks:
        block_cols_present = [c for c in cols if c in final_columns]
        if not block_cols_present:
            continue
        n = len(block_cols_present)
        ws.cell(row=2, column=col_pointer, value=block_name)
        if n > 1:
            ws.merge_cells(start_row=2, start_column=col_pointer,
                           end_row=2, end_column=col_pointer + n - 1)
        cell = ws.cell(row=2, column=col_pointer)
        cell.fill = PatternFill("solid", fgColor=hdr_color)
        cell.font = bold_white
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = border
        col_pointer += n
    ws.row_dimensions[2].height = 22

    # ── Ligne 3 : noms de colonnes ──────────────────────────────────────────
    for ci, cn in enumerate(final_columns, 1):
        cell = ws.cell(row=3, column=ci, value=cn)
        cell.fill = PatternFill("solid", fgColor="D9D9D9")
        cell.font = bold_black
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = border
    ws.row_dimensions[3].height = 28

    # ── Lignes de données ───────────────────────────────────────────────────
    fiab_to_fill = {
        OK:         PatternFill("solid", fgColor=ROW_OK_FILL),
        A_VERIFIER: PatternFill("solid", fgColor=ROW_VERIF_FILL),
        KO:         PatternFill("solid", fgColor=ROW_KO_FILL),
    }
    for ri, row in enumerate(df.itertuples(index=False), 4):
        fiab = getattr(row, "Fiabilite", "")
        row_fill = fiab_to_fill.get(fiab, PatternFill("solid", fgColor="FFFFFF"))
        for ci, (cn, val) in enumerate(zip(final_columns, row), 1):
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.font = base_font
            cell.border = border
            cell.alignment = Alignment(vertical="center", wrap_text=True)
            cell.fill = row_fill
            if cn in disc_cols and val:
                cell.font = disc_font
            if cn == "Fiabilite" and val:
                cell.font = bold_black
                cell.alignment = Alignment(horizontal="center", vertical="center")

    # ── Largeurs de colonnes ────────────────────────────────────────────────
    widths = {
        "Nom_Source": 35, "Nom_GLEIF": 35, "Discordance_Nom": 50,
        "RCS_Source": 22, "Pays_Source": 12,
        "RCS_GLEIF": 22, "Pays_GLEIF": 12, "Discordance_RCS": 45,
        "LEI_Source": 24, "LEI_GLEIF": 24,
        "Statut_LEI_GLEIF": 16, "DateValidite_LEI_GLEIF": 22,
        "Discordance_LEI": 45,
        "TypeCorrespondance": 22, "Fiabilite": 14, "ActionRequise": 50,
    }
    for ci, cn in enumerate(final_columns, 1):
        ws.column_dimensions[get_column_letter(ci)].width = widths.get(cn, 22)

    ws.freeze_panes = "A4"

    # ── Onglet Instructions ─────────────────────────────────────────────────
    ws2 = wb.create_sheet("Instructions")
    ws2.column_dimensions["A"].width = 28
    ws2.column_dimensions["B"].width = 80

    sg_red_font  = Font(name="Calibri", bold=True, size=14, color=SG_RED)
    title_font   = Font(name="Calibri", bold=True, size=11, color=SG_BLACK)
    section_font = Font(name="Calibri", bold=True, size=11, color="FFFFFF")

    ws2.cell(1, 1, "SOCIÉTÉ GÉNÉRALE — Middle Office").font = sg_red_font
    ws2.cell(2, 1, "GLEIF LEI Matcher — Mode d'emploi").font = title_font

    rows = [
        ("", ""),
        ("⚠ DISCLAIMER",
         "Cet outil est une AIDE À LA DÉCISION et non une vérité absolue. "
         "Le rapprochement repose sur des heuristiques (normalisation, similarité fuzzy). "
         "Toute correspondance qualifiée 'À vérifier' doit être validée manuellement "
         "avant usage opérationnel ou réglementaire."),
        ("", ""),
        ("FIABILITÉ — 3 niveaux", ""),
        ("🟢 OK",
         "Correspondance exacte (RCS + Pays cohérent) ou LEI validé sans discordance. "
         "Aucune action requise."),
        ("🟡 À vérifier",
         "Correspondance approximative, ou pays source absent, ou écart résiduel détecté. "
         "Action : vérification manuelle des libellés/pays."),
        ("🔴 KO",
         "LEI Discordant, ou aucune correspondance fiable trouvée. "
         "Action : recherche manuelle sur https://www.gleif.org ou contact entité."),
        ("", ""),
        ("BLOCS DU FICHIER", ""),
        ("Données source",
         "Vos colonnes d'origine, conservées intactes pour l'auditabilité."),
        ("Bloc Identité",
         "Comparaison nom légal source vs GLEIF + diagnostic de discordance."),
        ("Bloc Légal",
         "RCS source vs GLEIF + Pays source vs GLEIF. La règle v2.0 exige que le "
         "RCS soit validé conjointement avec le pays — un même numéro de registre "
         "dans deux pays différents ne produit plus de match silencieux."),
        ("Bloc LEI",
         "LEI source (si fourni) vs LEI GLEIF + statut + date de prochain "
         "renouvellement."),
        ("Synthèse",
         "TypeCorrespondance (Exact, Approx, Discordant…), Fiabilité, et "
         "ActionRequise — la colonne décisionnelle pour le Middle Office."),
        ("", ""),
        ("LOGIQUE DE MATCHING", ""),
        ("1. LEI fourni",
         "Lookup direct dans GLEIF. Si introuvable : fallback RCS+Pays / Nom+Pays "
         "pour proposer le bon LEI (résultat = LEI Discordant)."),
        ("2. LEI absent — RCS+Pays exact",
         "Index composite (RCS, Pays_ISO). Match strict, fiabilité = OK."),
        ("3. LEI absent — RCS+Pays approché",
         "Contenance par sous-chaîne (ex: '1513210151' ⊆ '01513210151'). "
         "Restreint au pays cible si fourni."),
        ("4. LEI absent — Nom+Pays approché",
         f"Similarité fuzzy ≥ {threshold} % (token_sort_ratio), restreint au pays. "
         "Si code postal fourni, préférence aux entités dont le CP correspond."),
        ("", ""),
        ("PARAMÈTRES PAR DÉFAUT", ""),
        ("Seuil similarité nom",
         f"{threshold} % — conservateur. Préférer un 'Non trouvé' à un faux positif."),
        ("Seuil RCS approché",
         "88 % — détecte les zéros de tête manquants et fautes mineures."),
        ("Pays source absent",
         "Le match RCS est accepté en mode dégradé (country-agnostic) mais la "
         "fiabilité est forcée à 'À vérifier'."),
        ("", ""),
        ("STATISTIQUES DU LOT", ""),
        ("Total lignes traitées",        f"{stats.get('total', 0):,}"),
        ("OK (auto)",                    f"{stats.get('ok', 0):,}"),
        ("À vérifier (manuel léger)",    f"{stats.get('a_verifier', 0):,}"),
        ("KO (manuel approfondi)",       f"{stats.get('ko', 0):,}"),
    ]
    r = 4
    for k, v in rows:
        c1 = ws2.cell(r, 1, k)
        c2 = ws2.cell(r, 2, v)
        if k.startswith(("FIABILITÉ", "BLOCS", "LOGIQUE", "PARAMÈTRES", "STATISTIQUES")):
            c1.font = section_font
            c1.fill = PatternFill("solid", fgColor=SG_RED)
            ws2.merge_cells(start_row=r, start_column=1, end_row=r, end_column=2)
            ws2.cell(r, 1).alignment = Alignment(horizontal="left", vertical="center")
        elif k == "⚠ DISCLAIMER":
            c1.font = Font(name="Calibri", bold=True, size=11, color="856404")
            c2.font = Font(name="Calibri", size=10, color="856404")
            c1.fill = c2.fill = PatternFill("solid", fgColor="FFF3CD")
        else:
            c1.font = Font(name="Calibri", bold=True, size=10)
            c2.font = base_font
        c1.alignment = Alignment(vertical="top", wrap_text=True)
        c2.alignment = Alignment(vertical="top", wrap_text=True)
        r += 1

    wb.save(output_path)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser(description="GLEIF LEI Matcher v2.0 — Middle Office Edition")
    p.add_argument("--input",  required=True)
    p.add_argument("--gleif",  required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--col-rcs",    default="RCS")
    p.add_argument("--col-name",   default="NomEntreprise")
    p.add_argument("--col-pays",   default="Pays")
    p.add_argument("--col-lei",    default=None)
    p.add_argument("--col-date",   default=None)
    p.add_argument("--col-postal", default=None)
    p.add_argument("--fuzzy-threshold",     type=int, default=90,
                   help="Seuil similarité nom/pays (défaut 90)")
    p.add_argument("--rcs-fuzzy-threshold", type=int, default=88,
                   help="Seuil RCS approché (défaut 88, 0=désactivé)")
    p.add_argument("--active-only",  action="store_true", default=True)
    p.add_argument("--all-statuses", dest="active_only", action="store_false")
    p.add_argument("--prepare-slim", action="store_true")
    p.add_argument("--slim-output",  default=None)
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
        input_path          = args.input,
        gleif_path          = gleif_path,
        output_path         = args.output,
        col_rcs             = args.col_rcs,
        col_name            = args.col_name,
        col_pays            = args.col_pays,
        col_lei             = args.col_lei,
        col_date            = args.col_date,
        col_postal          = args.col_postal,
        fuzzy_threshold     = args.fuzzy_threshold,
        rcs_fuzzy_threshold = args.rcs_fuzzy_threshold,
        active_only         = args.active_only,
    )
