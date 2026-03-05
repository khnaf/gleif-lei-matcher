"""
gleif_matcher.py
================
Module de rapprochement LEI GLEIF pour fichiers Excel.

Workflow :
  1. Si colonne LEI_Existant présente et non vide → validation du LEI existant
       • Lookup direct par code LEI dans GLEIF
       • Comparaison des données (RCS, nom, pays) → détection de discordance
  2. Si LEI absent (ou colonne absente) → recherche par RCS puis fuzzy nom/pays

Types de correspondance :
  LEI Valide          — LEI existant confirmé par GLEIF, données cohérentes
  LEI Discordant      — LEI existant trouvé dans GLEIF mais données différentes
  LEI Inconnu – GLEIF — LEI existant introuvable dans la base GLEIF
  Exact – RCS         — correspondance exacte sur numéro de registre
  Approx – Nom/Pays   — correspondance approximative sur nom + pays
  Non trouvé          — aucune correspondance possible

Colonnes de sortie supplémentaires v1.2 :
  GLEIF_DateRenouvellement — date de prochaine échéance du LEI
  LEI_Discordance          — détail des divergences détectées

Colonnes de sortie supplémentaires v1.5 :
  GLEIF_CodePostal         — code postal de l'entité selon GLEIF

Usage CLI :
  python gleif_matcher.py --input societes.xlsx --gleif gleif_golden_copy.csv --output resultats.xlsx

Colonnes GLEIF gérées automatiquement (variantes selon version Golden Copy) :
  LEI, Entity.LegalName, Entity.LegalAddress.Country,
  Entity.LegalAddress.PostalCode,
  Entity.EntityStatus, Registration.RegistrationStatus,
  Registration.RegistrationAuthorityID, Registration.RegistrationAuthorityEntityID,
  Registration.NextRenewalDate
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
    # ── Europe ────────────────────────────────────────────────────────────────
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

    # ── Amériques ─────────────────────────────────────────────────────────────
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
    "paraguay": "PY",
    "uruguay": "UY",
    "guyane": "GY", "guyana": "GY",
    "suriname": "SR",
    "cuba": "CU",
    "haiti": "HT", "haïti": "HT",
    "republique dominicaine": "DO", "république dominicaine": "DO",
    "dominican republic": "DO",
    "jamaique": "JM", "jamaïque": "JM", "jamaica": "JM",
    "trinite-et-tobago": "TT", "trinidad and tobago": "TT", "trinidad": "TT",
    "barbade": "BB", "barbados": "BB",
    "bahamas": "BS",
    "belize": "BZ",
    "guatemala": "GT",
    "honduras": "HN",
    "salvador": "SV", "el salvador": "SV",
    "nicaragua": "NI",
    "costa rica": "CR",
    "panama": "PA", "panamá": "PA",
    "porto rico": "PR", "puerto rico": "PR",
    "sainte-lucie": "LC", "saint lucia": "LC",
    "saint-vincent": "VC", "saint vincent and the grenadines": "VC",
    "grenade": "GD", "grenada": "GD",
    "antigua-et-barbuda": "AG", "antigua and barbuda": "AG",
    "saint-kitts-et-nevis": "KN", "saint kitts and nevis": "KN",
    "dominique": "DM", "dominica": "DM",
    "guyana": "GY",

    # ── Asie ──────────────────────────────────────────────────────────────────
    "chine": "CN", "china": "CN",
    "japon": "JP", "japan": "JP",
    "coree du sud": "KR", "corée du sud": "KR", "south korea": "KR", "korea": "KR",
    "coree du nord": "KP", "corée du nord": "KP", "north korea": "KP",
    "inde": "IN", "india": "IN",
    "pakistan": "PK",
    "bangladesh": "BD",
    "sri lanka": "LK",
    "nepal": "NP", "népal": "NP",
    "bhoutan": "BT", "bhutan": "BT",
    "afghanistan": "AF",
    "iran": "IR",
    "irak": "IQ", "iraq": "IQ",
    "syrie": "SY", "syria": "SY",
    "liban": "LB", "lebanon": "LB",
    "israel": "IL", "israël": "IL",
    "jordanie": "JO", "jordan": "JO",
    "arabie saoudite": "SA", "saudi arabia": "SA",
    "emirats arabes unis": "AE", "uae": "AE",
    "united arab emirates": "AE",
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
    "philippines": "PH",
    "taiwan": "TW",
    "hong kong": "HK",
    "macao": "MO", "macau": "MO",
    "brunei": "BN",
    "timor oriental": "TL", "timor-leste": "TL", "east timor": "TL",
    "maldives": "MV",

    # ── Afrique ───────────────────────────────────────────────────────────────
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
    "rwanda": "RW",
    "burundi": "BI",
    "mozambique": "MZ",
    "zimbabwe": "ZW",
    "zambie": "ZM", "zambia": "ZM",
    "malawi": "MW",
    "madagascar": "MG",
    "ile maurice": "MU", "maurice": "MU", "mauritius": "MU",
    "comores": "KM", "comoros": "KM",
    "seychelles": "SC",
    "afrique du sud": "ZA", "south africa": "ZA",
    "namibie": "NA", "namibia": "NA",
    "botswana": "BW",
    "lesotho": "LS",
    "swaziland": "SZ", "eswatini": "SZ",
    "angola": "AO",
    "congo": "CG", "republique du congo": "CG", "republic of the congo": "CG",
    "republique democratique du congo": "CD", "rdc": "CD",
    "democratic republic of the congo": "CD", "drc": "CD", "zaire": "CD",
    "gabon": "GA",
    "cameroun": "CM", "cameroon": "CM",
    "guinee equatoriale": "GQ", "equatorial guinea": "GQ",
    "sao tome-et-principe": "ST", "sao tome and principe": "ST",
    "nigeria": "NG",
    "ghana": "GH",
    "cote d'ivoire": "CI", "cote divoire": "CI", "ivory coast": "CI",
    "liberia": "LR",
    "sierra leone": "SL",
    "guinee": "GN", "guinée": "GN", "guinea": "GN",
    "guinee-bissau": "GW", "guinea-bissau": "GW",
    "senegal": "SN", "sénégal": "SN",
    "gambie": "GM", "gambia": "GM",
    "mauritanie": "MR", "mauritania": "MR",
    "mali": "ML",
    "burkina faso": "BF",
    "niger": "NE",
    "tchad": "TD", "chad": "TD",
    "togo": "TG",
    "benin": "BJ", "bénin": "BJ",
    "cap-vert": "CV", "cape verde": "CV", "cabo verde": "CV",
    "centrafrique": "CF", "republique centrafricaine": "CF",
    "central african republic": "CF",
    "mozambique": "MZ",
    "libye": "LY",

    # ── Océanie ───────────────────────────────────────────────────────────────
    "australie": "AU", "australia": "AU",
    "nouvelle-zelande": "NZ", "nouvelle-zélande": "NZ", "new zealand": "NZ",
    "papouasie-nouvelle-guinee": "PG", "papua new guinea": "PG",
    "fidji": "FJ", "fiji": "FJ",
    "vanuatu": "VU",
    "salomon": "SB", "solomon islands": "SB",
    "samoa": "WS",
    "tonga": "TO",
    "kiribati": "KI",
    "tuvalu": "TV",
    "nauru": "NR",
    "marshall": "MH", "marshall islands": "MH",
    "micronesie": "FM", "micronésie": "FM", "micronesia": "FM",
    "palaos": "PW", "palau": "PW",
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
    """
    Normalise un numéro de registre pour la comparaison.

    Étapes :
      1. Unicode NFKC : chiffres pleine largeur (０-９), arabes-indics (٠-٩), etc.
      2. Conversion des chiffres Unicode non-ASCII restants
      3. Suppression du préfixe "RCS Ville" (ex : "RCS Paris 552 032 534")
      4. Suppression de tout caractère non alphanumérique

    Note : les zéros de tête sont intentionnellement conservés.
    Un RCS "1513210151" (base client) vs "01513210151" (GLEIF) n'est pas
    normalisé à l'identique — cette différence est détectée par search_by_rcs_fuzzy
    et signalée comme "Approx – RCS" pour que le Middle Office puisse corriger
    son référentiel source.
    """
    if pd.isna(value) or str(value).strip() == "":
        return ""
    raw = str(value)
    # Étape 1 : normalisation Unicode NFKC
    raw = unicodedata.normalize("NFKC", raw).upper()
    # Étape 2 : chiffres Unicode non-ASCII restants → ASCII
    raw = "".join(
        str(unicodedata.digit(c, -1)) if unicodedata.category(c) == "Nd" and not c.isascii() else c
        for c in raw
    )
    # Étape 3 : suppression du préfixe "RCS Ville"
    raw = re.sub(r"^RCS\s+[A-ZÉÈÀÂÊÎÔÙÛÇ\s]+\s+", "", raw).strip()
    # Étape 4 : garder uniquement les caractères alphanumériques ASCII
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
    """
    Normalise une date vers datetime.date quelle que soit sa représentation.

    Formats gérés :
      • GLEIF ISO 8601 : "2025-12-31T00:00:00Z" / "2025-12-31T00:00:00+00:00"
      • Date ISO courte : "2025-12-31"
      • Format client    : "31-12-2025" / "31/12/2025" / "31.12.2025"

    Retourne None si la valeur est vide, nulle ou non parsable.
    """
    if pd.isna(value) or str(value).strip() in ("", "nan", "NaT", "None"):
        return None
    raw = str(value).strip()
    # Tronquer au format yyyy-mm-dd si la date contient une partie heure
    if "T" in raw:
        raw = raw.split("T")[0]
    elif len(raw) > 10 and raw[10] in (" ", "+"):
        raw = raw[:10]
    # Essai des formats courants (ISO d'abord, puis formats client)
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def normalize_postal_code(value) -> str:
    """
    Extrait uniquement les chiffres d'un code postal pour la comparaison.

    Stratégie :
      • Supprime tout caractère non numérique (lettres, tirets, espaces, etc.)
      • Exemple : "L-1338" → "1338",  "75008" → "75008",  "B-1000" → "1000"

    Utilisée pour la contenance : les chiffres du code postal client doivent
    être contenus dans le code postal GLEIF brut (qui peut avoir un préfixe
    pays comme "L-", "B-", "D-", etc.).
    Cette logique est analogue à celle du RCS approché.

    Retourne une chaîne vide si la valeur est vide ou ne contient pas de chiffres.
    """
    if pd.isna(value) or str(value).strip() == "":
        return ""
    digits = re.sub(r"[^0-9]", "", str(value))
    return digits


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

    # Détection du format : slim (colonnes logiques) vs Golden Copy complet
    # Le slim CSV a des headers comme "lei", "name", "country"…
    # Le Golden Copy a des headers comme "Entity.LegalName", "Registration.RegistrationStatus"…
    _slim_markers = {"lei", "name", "country", "entity_status", "lei_status"}
    is_slim_format = _slim_markers.issubset(set(available_cols))

    if is_slim_format:
        # ── Format slim : colonnes déjà normalisées, pas de renommage ────────
        _status("Format slim détecté — chargement direct des colonnes logiques…")
        # Avertissement : la slim ne contient que ACTIVE+ISSUED ; en mode validation
        # (active_only=False), les entités LAPSED sont absentes → fallback limité.
        if not active_only:
            _warn = (
                "⚠  Base slim + mode validation LEI : la slim ne contient que "
                "les entités ACTIVE+ISSUED. Les entités LAPSED/INACTIVE absentes "
                "de la slim ne seront pas retrouvées par le fallback RCS/nom. "
                "Pour une couverture complète des LEI expirés, utilisez le "
                "Golden Copy complet ou régénérez la slim sans filtre actif."
            )
            log.warning(_warn)
            if status_cb:
                status_cb("⚠ Base slim : entités LAPSED absentes — validation LEI partielle")
        usecols = [col for col in SLIM_COLUMNS if col in available_cols]
        # Ajouter les colonnes slim manquantes (ex: renewal_date absent d'un ancien slim)
        _missing_slim = [c for c in SLIM_COLUMNS if c not in available_cols]
        if _missing_slim:
            log.warning(f"Colonnes absentes du slim (seront vides) : {_missing_slim}")
        col_map = None  # pas de renommage nécessaire
    else:
        # ── Format Golden Copy complet : détecter les noms GLEIF réels ───────
        col_map, _missing = _detect_gleif_columns(available_cols)
        usecols = list(set(col_map.values()))

    _status(f"Colonnes retenues : {len(usecols)} / {len(available_cols)} — lecture par chunks…")

    # Estimation de la taille totale pour la progression
    try:
        file_size = path.stat().st_size
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
        on_bad_lines="skip",
    )

    for chunk in reader:
        if not is_slim_format and col_map:
            # Golden Copy : renommer les colonnes GLEIF → noms logiques
            rename_map = {v: k for k, v in col_map.items()}
            chunk = chunk.rename(columns=rename_map)

        # Ajouter les colonnes logiques manquantes (vides)
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

    Le slim CSV ne contient que les colonnes utiles (incluant la date de
    renouvellement) et optionnellement uniquement les entités ACTIVE + ISSUED.

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
) -> Tuple[Dict[str, List[int]], Dict[str, Dict[str, List[int]]], Dict[str, int]]:
    """
    Construit trois index de recherche sur le DataFrame GLEIF :
      rcs_index  : {rcs_normalisé → [indices de lignes]}
      name_index : {pays_iso → {nom_normalisé → [indices de lignes]}}
      lei_index  : {code_LEI_upper → indice de ligne}  ← nouveau v1.2
    """
    log.info("Construction des index …")
    rcs_index: Dict[str, List[int]] = {}
    lei_index: Dict[str, int] = {}

    for i, (lei, ra_entity) in enumerate(zip(df["lei"], df["ra_entity"])):
        # Index RCS
        key_rcs = normalize_rcs(ra_entity)
        if key_rcs:
            rcs_index.setdefault(key_rcs, []).append(i)
        # Index LEI (lookup direct O(1))
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
        f"  Index RCS : {len(rcs_index):,} entrées | "
        f"Index LEI : {len(lei_index):,} entrées | "
        f"Index noms : {sum(len(v) for v in name_index.values()):,} entrées"
    )
    return rcs_index, name_index, lei_index


def search_by_rcs(
    rcs_norm: str,
    rcs_index: Dict[str, List[int]],
    df: pd.DataFrame,
) -> Optional[pd.Series]:
    if not rcs_norm:
        return None
    indices = rcs_index.get(rcs_norm)
    return df.iloc[indices[0]] if indices else None


def search_by_rcs_fuzzy(
    rcs_norm: str,
    rcs_index: Dict[str, List[int]],
    df: pd.DataFrame,
    threshold: int = 88,
) -> Tuple[Optional[pd.Series], int]:
    """
    Recherche approximative par numéro de registre — critère de contenance.

    Stratégie :
      1. Filtre par longueur : seules les clés GLEIF dont la longueur est dans
         [len(rcs_norm), len(rcs_norm)+2] sont considérées (le RCS client doit
         être plus court ou égal au RCS GLEIF).
      2. Contenance : le RCS client doit apparaître en tant que sous-chaîne
         du RCS GLEIF.
         → Détecte les caractères manquants dans le référentiel client :
           "1513210151" ⊆ "01513210151"  (zéro de tête absent du référentiel)
           "ABCDE123"   ⊆ "XABCDE123"   (préfixe absent)
      3. Score = len(rcs_client) / len(rcs_gleif) × 100  (≥ threshold requis).

    Ce critère est plus strict que fuzz.ratio : deux chaînes de longueurs
    proches mais de contenu différent ne produiront jamais de faux positif.

    Retourne :
      (row, score)  si trouvé,  (None, 0)  sinon.
    """
    if not rcs_norm or len(rcs_norm) < 4:
        return None, 0

    n = len(rcs_norm)
    best_row  = None
    best_score = 0

    for key, idxs in rcs_index.items():
        key_len = len(key)
        # Filtre longueur : la clé GLEIF doit être ≥ RCS client
        # et la différence ne peut excéder 2 caractères
        if key_len < n or (key_len - n) > 2:
            continue

        if rcs_norm in key:
            score = round(n / key_len * 100)
            if score >= threshold and score > best_score:
                best_score = score
                best_row   = df.iloc[idxs[0]]

    return best_row, best_score


def search_by_lei(
    lei_val: str,
    lei_index: Dict[str, int],
    df: pd.DataFrame,
) -> Optional[pd.Series]:
    """Lookup direct par code LEI (O(1))."""
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
    threshold: int = 80,
    client_postal_digits: str = "",
) -> Tuple[Optional[pd.Series], int]:
    """
    Recherche approximative par nom + pays (et code postal optionnel).

    Stratégie :
      1. Filtre les candidats GLEIF dont le nom normalisé est ≥ threshold
         (via fuzz.token_sort_ratio).
      2. Si un code postal client (chiffres uniquement) est fourni :
           • Parmi les candidats retenus, préfère ceux dont le code postal
             GLEIF brut contient les chiffres du client (contenance).
           • Ex: "1338" ⊆ "L-1338"  →  candidat luxembourgeois favorisé.
           • Si aucun candidat n'a de code postal correspondant : retourne
             le meilleur par score nom (comportement dégradé gracieux).
      3. Sans code postal : retourne le meilleur candidat par score nom
         (comportement original).

    Retourne (row, score_nom) ou (None, 0).
    """
    if not name_norm or not iso_country:
        return None, 0
    country_names = name_index.get(iso_country, {})
    if not country_names:
        return None, 0

    if not client_postal_digits:
        # Comportement original : meilleur score nom uniquement
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

    # Avec code postal : récupérer les 10 meilleurs candidats par score nom
    candidates = process.extract(
        name_norm,
        list(country_names.keys()),
        scorer=fuzz.token_sort_ratio,
        score_cutoff=threshold,
        limit=10,
    )
    if not candidates:
        return None, 0

    # Parmi les candidats, préférer celui dont le code postal correspond
    for cand_name, name_score, _ in candidates:
        row = df.iloc[country_names[cand_name][0]]
        gleif_postal = str(row.get("postal_code", "")).strip()
        if gleif_postal and client_postal_digits in gleif_postal:
            return row, int(name_score)

    # Aucun candidat avec code postal correspondant → meilleur par nom
    best_name, score, _ = candidates[0]
    return df.iloc[country_names[best_name][0]], int(score)


# ─────────────────────────────────────────────────────────────────────────────
# Contrôle qualité : écarts entre référentiel client et GLEIF
# ─────────────────────────────────────────────────────────────────────────────

def _check_data_gaps(
    gleif_row: pd.Series,
    client_rcs_raw: str = "",
    client_name_raw: str = "",
    client_iso: str = "",
    client_lei: str = "",
    client_date_raw: str = "",
    client_postal_raw: str = "",
    name_threshold: int = 70,
) -> Dict[str, str]:
    """
    Détecte et décrit tous les écarts entre le référentiel client et GLEIF.

    Contrôles effectués pour tous les types de correspondance :
      1. LEI        — manquant côté client  OU  différent de GLEIF
      2. RCS        — manquant côté client  OU  différent (après normalisation)
      3. Nom        — similarité < name_threshold (seulement si les deux ont une valeur)
      4. Date       — manquante côté client OU  différente de GLEIF (après normalisation)
      5. CodePostal — vrais écarts (chiffres différents) OU écart de format
                      (chiffres identiques mais libellé différent, ex: '1338' vs 'L-1338')

    Retourne un dictionnaire {clé → message} avec les clés :
      "lei", "rcs", "nom", "date", "postal"
    Chaque valeur est vide ("") si aucun écart n'est détecté sur ce champ.
    """
    disc: Dict[str, str] = {"lei": "", "rcs": "", "nom": "", "date": "", "postal": ""}

    # ── 1. LEI ───────────────────────────────────────────────────────────────
    lei_client = str(client_lei).strip().upper() if client_lei else ""
    lei_gleif_raw = str(gleif_row.get("lei", "")).strip()
    lei_gleif = lei_gleif_raw.upper()
    if not lei_client and lei_gleif:
        disc["lei"] = f"LEI manquant → GLEIF: '{lei_gleif_raw}'"
    elif lei_client and lei_gleif and lei_client != lei_gleif:
        disc["lei"] = f"LEI: client='{client_lei.strip()}' ≠ GLEIF='{lei_gleif_raw}'"

    # ── 2. RCS ───────────────────────────────────────────────────────────────
    rcs_client_clean = str(client_rcs_raw).strip() if client_rcs_raw else ""
    rcs_gleif_raw    = str(gleif_row.get("ra_entity", "")).strip()
    rcs_norm_c = normalize_rcs(rcs_client_clean) if rcs_client_clean else ""
    rcs_norm_g = normalize_rcs(rcs_gleif_raw)    if rcs_gleif_raw    else ""
    if not rcs_norm_c and rcs_norm_g:
        disc["rcs"] = f"RCS manquant → GLEIF: '{rcs_gleif_raw}'"
    elif rcs_norm_c and rcs_norm_g and rcs_norm_c != rcs_norm_g:
        disc["rcs"] = f"RCS: client='{rcs_client_clean}' ≠ GLEIF='{rcs_gleif_raw}'"

    # ── 3. Nom légal ─────────────────────────────────────────────────────────
    name_client_clean = str(client_name_raw).strip() if client_name_raw else ""
    name_gleif_clean  = str(gleif_row.get("name", "")).strip()
    if name_client_clean and name_gleif_clean:
        n_c = normalize_name(name_client_clean)
        n_g = normalize_name(name_gleif_clean)
        if n_c and n_g:
            score = fuzz.token_sort_ratio(n_c, n_g)
            if score < name_threshold:
                disc["nom"] = (
                    f"Nom: client='{name_client_clean}' ≠ GLEIF='{name_gleif_clean}' "
                    f"(sim={score}%)"
                )

    # ── 4. Date de validité LEI ───────────────────────────────────────────────
    date_gleif_raw = str(gleif_row.get("renewal_date", "")).strip()
    date_gleif     = normalize_date(date_gleif_raw)
    date_client    = normalize_date(client_date_raw) if client_date_raw else None
    if not date_client and date_gleif:
        disc["date"] = f"Date LEI manquante → GLEIF: '{date_gleif.strftime('%d-%m-%Y')}'"
    elif date_client and date_gleif and date_client != date_gleif:
        disc["date"] = (
            f"Date LEI: client='{date_client.strftime('%d-%m-%Y')}' "
            f"≠ GLEIF='{date_gleif.strftime('%d-%m-%Y')}'"
        )

    # ── 5. Code postal ────────────────────────────────────────────────────────
    # Deux niveaux d'écart détectés :
    #   a) Vrais écarts      : chiffres client ∉ code postal GLEIF
    #   b) Écart de format   : chiffres identiques mais libellé différent
    #      (ex : client='1338', GLEIF='L-1338') → à harmoniser dans le référentiel
    postal_gleif_raw    = str(gleif_row.get("postal_code", "")).strip()
    postal_client_clean = str(client_postal_raw).strip() if client_postal_raw else ""
    postal_client_digits = normalize_postal_code(postal_client_clean) if postal_client_clean else ""
    if postal_client_digits and postal_gleif_raw:
        if postal_client_digits not in postal_gleif_raw:
            # Vrai écart : codes postaux différents
            disc["postal"] = (
                f"CP: client='{postal_client_clean}' ≠ GLEIF='{postal_gleif_raw}'"
            )
        elif postal_client_clean != postal_gleif_raw:
            # Format différent mais chiffres identiques → signaler pour harmonisation DQ
            disc["postal"] = (
                f"Format CP: client='{postal_client_clean}' → GLEIF='{postal_gleif_raw}' "
                f"(à harmoniser)"
            )
    # Si GLEIF n'a pas de code postal : pas d'alerte

    return disc


def _check_lei_discordance(
    gleif_row: pd.Series,
    client_rcs_raw: str,
    client_name_raw: str,
    client_iso: str,
    client_lei: str = "",
    name_threshold: int = 70,
) -> Tuple[str, bool]:
    """
    Compare les données du client avec celles retournées par GLEIF.

    Vérifications effectuées (dans l'ordre) :
      - LEI    : comparaison exacte client vs GLEIF (utile quand l'entité a été
                 retrouvée par RCS/nom après échec du lookup direct par LEI)
      - RCS    : comparaison exacte après normalisation
      - Nom    : similarité fuzzy token_sort_ratio ≥ name_threshold (défaut 70 %)
      - Pays   : comparaison ISO alpha-2

    Retourne :
      (texte_discordance, is_discordant)
      texte_discordance = "" si aucune divergence détectée
    """
    issues: List[str] = []

    # ── Comparaison LEI (client vs GLEIF) ────────────────────────────────────
    # Pertinent quand l'entité a été retrouvée par RCS/nom et non par LEI direct
    lei_client = str(client_lei).strip().upper() if client_lei else ""
    lei_gleif  = str(gleif_row.get("lei", "")).strip().upper()
    if lei_client and lei_gleif and lei_client != lei_gleif:
        issues.append(
            f"LEI: client='{client_lei.strip()}' ≠ GLEIF='{gleif_row.get('lei', '')}'"
        )

    # ── Vérification RCS ─────────────────────────────────────────────────────
    rcs_client = client_rcs_raw.strip() if client_rcs_raw else ""
    if rcs_client:
        rcs_norm_c = normalize_rcs(rcs_client)
        rcs_norm_g = normalize_rcs(str(gleif_row.get("ra_entity", "")))
        if rcs_norm_c and rcs_norm_g and rcs_norm_c != rcs_norm_g:
            issues.append(
                f"RCS: client='{rcs_client}' ≠ GLEIF='{gleif_row.get('ra_entity', '')}'"
            )

    # ── Vérification Nom ─────────────────────────────────────────────────────
    name_client = client_name_raw.strip() if client_name_raw else ""
    if name_client:
        name_norm_c = normalize_name(name_client)
        name_norm_g = normalize_name(str(gleif_row.get("name", "")))
        if name_norm_c and name_norm_g:
            score = fuzz.token_sort_ratio(name_norm_c, name_norm_g)
            if score < name_threshold:
                issues.append(
                    f"Nom: client='{name_client}' ≠ GLEIF='{gleif_row.get('name', '')}' "
                    f"(similarité={score}%)"
                )

    # ── Vérification Pays ────────────────────────────────────────────────────
    if client_iso and client_iso.strip():
        country_g = str(gleif_row.get("country", "")).strip().upper()
        if country_g and client_iso.upper() != country_g:
            issues.append(f"Pays: client={client_iso} ≠ GLEIF={country_g}")

    disc_text = " | ".join(issues)
    return disc_text, bool(issues)


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
    col_lei: Optional[str] = None,          # colonne LEI existant (v1.2)
    col_date: Optional[str] = None,         # colonne date validité LEI (v1.4)
    col_postal: Optional[str] = None,       # colonne code postal client (v1.5)
    fuzzy_threshold: int = 80,
    rcs_fuzzy_threshold: int = 88,
    active_only: bool = True,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    status_cb: Optional[Callable[[str], None]] = None,
) -> pd.DataFrame:
    """
    Pipeline complet de rapprochement.

    Paramètres
    ----------
    col_lei    : colonne LEI existants (optionnel). Si présente et non vide → mode validation.
    col_date   : colonne date validité LEI côté client (optionnel, format dd-mm-yyyy).
                 Comparée avec GLEIF renewal_date ; écart signalé dans LEI_Discordance.
    col_postal : colonne code postal côté client (optionnel).
                 Utilisée pour affiner le matching nom/pays :
                   • Les chiffres du code postal client doivent être contenus dans
                     le code postal GLEIF (ex: "1338" ⊆ "L-1338").
                   • Parmi plusieurs candidats nom/pays, préfère celui dont le
                     code postal correspond.
                   • Écart signalé dans LEI_Discordance si non conforme.

    Note : quand col_lei est fourni, le chargement GLEIF ignore le filtre
    active_only afin de retrouver même les LEI expirés (LAPSED / INACTIVE).
    Le statut réel est reporté dans GLEIF_StatutSociete et GLEIF_StatutLEI.

    La colonne LEI_Discordance signale tous les écarts DQ pour chaque ligne
    ayant une correspondance (LEI manquant, RCS manquant/différent, nom
    différent, date manquante/différente, code postal différent) — applicable
    à tous les types de correspondance, pas uniquement au mode validation LEI.
    """
    def _status(msg):
        log.info(msg)
        if status_cb:
            status_cb(msg)

    _status(f"Lecture du fichier d'entrée : {input_path}")
    df_input = _safe_read_excel(input_path).fillna("")
    _status(f"  {len(df_input):,} lignes chargées")

    # Validation des colonnes obligatoires
    required = [c for c in [col_rcs, col_name, col_pays] if c]
    missing_cols = [c for c in required if c not in df_input.columns]
    if missing_cols:
        raise ValueError(
            f"Colonnes manquantes dans le fichier d'entrée : {missing_cols}\n"
            f"Colonnes disponibles : {list(df_input.columns)}"
        )

    # Détermination du mode LEI
    has_lei_col = bool(col_lei) and col_lei in df_input.columns
    if has_lei_col:
        _status(
            f"  Colonne LEI détectée : '{col_lei}' — mode validation activé.\n"
            "  Chargement de tous les statuts GLEIF pour retrouver les LEI expirés."
        )

    # Chargement GLEIF
    # Si mode validation LEI : charger TOUS les statuts (pour trouver les LAPSED)
    _active_only_load = active_only if not has_lei_col else False
    df_gleif = load_gleif(
        gleif_path,
        active_only=_active_only_load,
        status_cb=status_cb,
    )
    rcs_index, name_index, lei_index = build_indices(df_gleif)

    results = []
    n_total = len(df_input)
    n_exact = n_approx_rcs = n_approx = n_miss = 0
    n_valid = n_discordant = n_lei_unknown = 0

    _status("Rapprochement en cours …")

    has_date_col   = bool(col_date)   and col_date   in df_input.columns
    has_postal_col = bool(col_postal) and col_postal in df_input.columns
    if has_date_col:
        _status(f"  Colonne date LEI détectée : '{col_date}' — contrôle de la date activé.")
    if has_postal_col:
        _status(f"  Colonne code postal détectée : '{col_postal}' — affinage matching nom/pays activé.")

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

        gleif_row   = None
        match_type  = "Non trouvé"
        match_score = ""
        disc: Dict[str, str] = {"lei": "", "rcs": "", "nom": "", "date": "", "postal": ""}

        # ── Mode 1 : validation d'un LEI existant ────────────────────────────
        if lei_exist:
            gleif_row = search_by_lei(lei_exist, lei_index, df_gleif)

            if gleif_row is not None:
                # LEI trouvé directement : match_type basé sur comparaison LEI
                lei_g = str(gleif_row.get("lei", "")).strip().upper()
                lei_c = str(lei_exist).strip().upper()
                if lei_c and lei_g and lei_c != lei_g:
                    match_type = "LEI Discordant"
                    n_discordant += 1
                else:
                    match_type = "LEI Valide"
                    n_valid += 1
                # Tous les écarts DQ pour cette ligne (LEI + RCS + Nom + Date + CP)
                disc = _check_data_gaps(
                    gleif_row, rcs_raw, name_raw, iso,
                    client_lei=lei_exist, client_date_raw=date_raw,
                    client_postal_raw=postal_raw,
                )

            else:
                # LEI introuvable dans GLEIF → fallback par RCS/nom pour
                # retrouver l'entité et comparer le bon LEI avec celui du client
                fallback_row = None

                if rcs_norm:
                    fallback_row = search_by_rcs(rcs_norm, rcs_index, df_gleif)
                    if fallback_row is None and rcs_fuzzy_threshold > 0:
                        fallback_row, _fb_rcs_score = search_by_rcs_fuzzy(
                            rcs_norm, rcs_index, df_gleif, rcs_fuzzy_threshold
                        )

                if fallback_row is None and name_norm:
                    fallback_row, _score = search_by_name_country(
                        name_norm, iso, name_index, df_gleif, fuzzy_threshold,
                        client_postal_digits=postal_digits,
                    )

                if fallback_row is not None:
                    gleif_row  = fallback_row
                    match_type = "LEI Discordant"
                    n_discordant += 1
                    # Tous les écarts DQ (_check_data_gaps inclut la comparaison LEI)
                    disc = _check_data_gaps(
                        gleif_row, rcs_raw, name_raw, iso,
                        client_lei=lei_exist, client_date_raw=date_raw,
                        client_postal_raw=postal_raw,
                    )
                else:
                    match_type = "Non trouvé (LEI invalide)"
                    n_lei_unknown += 1

        # ── Mode 2 : recherche d'un LEI manquant ─────────────────────────────
        else:
            # ── 2a. Correspondance RCS exacte ────────────────────────────────
            if rcs_norm:
                gleif_row = search_by_rcs(rcs_norm, rcs_index, df_gleif)
                if gleif_row is not None:
                    if active_only:
                        es = str(gleif_row.get("entity_status", "")).upper()
                        ls = str(gleif_row.get("lei_status", "")).upper()
                        if es != "ACTIVE" or ls != "ISSUED":
                            gleif_row = None
                    if gleif_row is not None:
                        match_type  = "Exact – RCS"
                        match_score = 100
                        n_exact    += 1

            # ── 2b. Correspondance RCS approchée ─────────────────────────────
            # Intercalée entre exact RCS et fuzzy nom, elle gère les cas :
            #   • zéro(s) de tête différents : "1513210151" vs "01513210151"
            #     (résolu aussi par normalize_rcs, cette étape est le filet)
            #   • faute de frappe mineure, formatage légèrement différent
            if gleif_row is None and rcs_norm and rcs_fuzzy_threshold > 0:
                approx_row, rcs_score = search_by_rcs_fuzzy(
                    rcs_norm, rcs_index, df_gleif, rcs_fuzzy_threshold
                )
                if approx_row is not None:
                    if active_only:
                        es = str(approx_row.get("entity_status", "")).upper()
                        ls = str(approx_row.get("lei_status", "")).upper()
                        if es != "ACTIVE" or ls != "ISSUED":
                            approx_row = None
                    if approx_row is not None:
                        # Validation secondaire : similarité nom pour s'assurer
                        # qu'il ne s'agit pas d'une coïncidence de numéro
                        gl_name_norm = normalize_name(str(approx_row.get("name", "")))
                        name_sim = (
                            fuzz.token_sort_ratio(name_norm, gl_name_norm)
                            if name_norm and gl_name_norm else ""
                        )
                        match_score = (
                            f"RCS:{rcs_score}% / Nom:{name_sim}%"
                            if name_sim != "" else f"RCS:{rcs_score}%"
                        )
                        match_type = "Approx – RCS"
                        gleif_row  = approx_row
                        n_approx_rcs += 1

            # ── 2c. Correspondance approximative nom + pays (+ code postal) ───
            if gleif_row is None and name_norm:
                gleif_row, score = search_by_name_country(
                    name_norm, iso, name_index, df_gleif, fuzzy_threshold,
                    client_postal_digits=postal_digits,
                )
                if gleif_row is not None:
                    if active_only:
                        es = str(gleif_row.get("entity_status", "")).upper()
                        ls = str(gleif_row.get("lei_status", "")).upper()
                        if es != "ACTIVE" or ls != "ISSUED":
                            gleif_row = None
                            score = 0
                    if gleif_row is not None:
                        match_type = "Approx – Nom/Pays"
                        # Indicateur code postal dans le score si fourni
                        if postal_digits:
                            gleif_postal = str(gleif_row.get("postal_code", "")).strip()
                            cp_ok = bool(gleif_postal and postal_digits in gleif_postal)
                            match_score = f"Nom:{score}% / CP:{'✓' if cp_ok else '✗'}"
                        else:
                            match_score = score
                        n_approx += 1

            if gleif_row is None:
                n_miss += 1
            else:
                # Écarts DQ pour tous les types de correspondance Mode 2
                disc = _check_data_gaps(
                    gleif_row, rcs_raw, name_raw, iso,
                    client_lei=lei_exist, client_date_raw=date_raw,
                    client_postal_raw=postal_raw,
                )

        # ── Construction de la ligne de résultat ─────────────────────────────
        if gleif_row is not None:
            results.append({
                "LEI_GLEIF":                gleif_row["lei"],
                "GLEIF_NomLegal":           gleif_row["name"],
                "GLEIF_Pays":               gleif_row["country"],
                "GLEIF_StatutSociete":      gleif_row["entity_status"],
                "GLEIF_StatutLEI":          gleif_row["lei_status"],
                "GLEIF_AutoriteRegistre":   gleif_row["ra_id"],
                "GLEIF_NumRegistre":        gleif_row["ra_entity"],
                "GLEIF_DateRenouvellement": gleif_row["renewal_date"],
                "GLEIF_CodePostal":         gleif_row.get("postal_code", ""),
                "TypeCorrespondance":       match_type,
                "ScoreCorrespondance":      match_score,
                "Disc_LEI":                 disc["lei"],
                "Disc_RCS":                 disc["rcs"],
                "Disc_Nom":                 disc["nom"],
                "Disc_Date":                disc["date"],
                "Disc_CodePostal":          disc["postal"],
            })
        else:
            results.append({
                "LEI_GLEIF": "", "GLEIF_NomLegal": "", "GLEIF_Pays": "",
                "GLEIF_StatutSociete": "", "GLEIF_StatutLEI": "",
                "GLEIF_AutoriteRegistre": "", "GLEIF_NumRegistre": "",
                "GLEIF_DateRenouvellement": "", "GLEIF_CodePostal": "",
                "TypeCorrespondance":  match_type,
                "ScoreCorrespondance": "",
                "Disc_LEI": "", "Disc_RCS": "", "Disc_Nom": "",
                "Disc_Date": "", "Disc_CodePostal": "",
            })

        if progress_cb and ((idx + 1) % 10 == 0 or (idx + 1) == n_total):
            progress_cb(idx + 1, n_total)

    df_results = pd.DataFrame(results)
    df_output  = pd.concat([df_input.reset_index(drop=True), df_results], axis=1)

    _status(f"Export vers : {output_path}")
    _export_excel(df_output, output_path, fuzzy_threshold)

    log.info(
        f"\n{'='*55}\n"
        f"  Total             : {n_total:>6,}\n"
        f"  LEI Valide        : {n_valid:>6,}  ({n_valid/n_total*100:.1f}%)\n"
        f"  LEI Discordant    : {n_discordant:>6,}  ({n_discordant/n_total*100:.1f}%)\n"
        f"  LEI Invalide      : {n_lei_unknown:>6,}  ({n_lei_unknown/n_total*100:.1f}%)\n"
        f"  Exact RCS         : {n_exact:>6,}  ({n_exact/n_total*100:.1f}%)\n"
        f"  Approx RCS        : {n_approx_rcs:>6,}  ({n_approx_rcs/n_total*100:.1f}%)\n"
        f"  Approx Nom/Pays   : {n_approx:>6,}  ({n_approx/n_total*100:.1f}%)\n"
        f"  Non trouvé        : {n_miss:>6,}  ({n_miss/n_total*100:.1f}%)\n"
        f"{'='*55}"
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

    HEADER_FILL   = PatternFill("solid", fgColor="1F4E79")
    GLEIF_FILL    = PatternFill("solid", fgColor="D6E4F0")
    EXACT_FILL    = PatternFill("solid", fgColor="D9EAD3")   # vert foncé → Exact RCS / LEI Valide
    APPROX_RCS_FILL = PatternFill("solid", fgColor="EAF4E4") # vert clair → Approx RCS
    APPROX_FILL   = PatternFill("solid", fgColor="FFF2CC")   # jaune → Approx Nom/Pays
    DISCORD_FILL  = PatternFill("solid", fgColor="FCE8D0")   # orange → LEI Discordant
    UNKNOWN_FILL  = PatternFill("solid", fgColor="DAE8FC")   # bleu clair → LEI Invalide
    MISS_FILL     = PatternFill("solid", fgColor="FCE4D6")   # rouge → Non trouvé

    thin   = Side(style="thin", color="AAAAAA")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    hfont  = Font(name="Arial", bold=True, color="FFFFFF", size=10)
    dfont  = Font(name="Arial", size=10)

    gleif_cols = [
        "LEI_GLEIF", "GLEIF_NomLegal", "GLEIF_Pays",
        "GLEIF_StatutSociete", "GLEIF_StatutLEI",
        "GLEIF_AutoriteRegistre", "GLEIF_NumRegistre",
        "GLEIF_DateRenouvellement", "GLEIF_CodePostal",
        "TypeCorrespondance", "ScoreCorrespondance",
        "Disc_LEI", "Disc_RCS", "Disc_Nom", "Disc_Date", "Disc_CodePostal",
    ]
    disc_cols = {"Disc_LEI", "Disc_RCS", "Disc_Nom", "Disc_Date", "Disc_CodePostal"}
    columns = list(df.columns)

    for ci, cn in enumerate(columns, 1):
        cell = ws.cell(row=1, column=ci, value=cn)
        cell.font      = hfont
        cell.fill      = HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = border

    for ri, row in enumerate(df.itertuples(index=False), 2):
        mt = getattr(row, "TypeCorrespondance", "")
        if mt in ("Exact – RCS", "LEI Valide"):
            rf = EXACT_FILL
        elif mt == "Approx – RCS":
            rf = APPROX_RCS_FILL
        elif mt == "Approx – Nom/Pays":
            rf = APPROX_FILL
        elif mt == "LEI Discordant":
            rf = DISCORD_FILL
        elif mt in ("LEI Inconnu – GLEIF", "Non trouvé (LEI invalide)"):
            rf = UNKNOWN_FILL
        else:
            rf = MISS_FILL

        for ci, (cn, val) in enumerate(zip(columns, row), 1):
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.font      = dfont
            cell.border    = border
            cell.alignment = Alignment(vertical="center")
            if cn in gleif_cols:
                cell.fill = rf
            # Colonnes Disc_* : rouge gras si non vide
            if cn in disc_cols and val:
                cell.font = Font(name="Arial", size=10, color="C00000", bold=True)

    col_widths = {
        "LEI_GLEIF": 25, "GLEIF_NomLegal": 35, "GLEIF_Pays": 10,
        "GLEIF_StatutSociete": 16, "GLEIF_StatutLEI": 14,
        "GLEIF_AutoriteRegistre": 18, "GLEIF_NumRegistre": 20,
        "GLEIF_DateRenouvellement": 22, "GLEIF_CodePostal": 18,
        "TypeCorrespondance": 22, "ScoreCorrespondance": 12,
        "Disc_LEI": 42, "Disc_RCS": 42, "Disc_Nom": 55,
        "Disc_Date": 48, "Disc_CodePostal": 45,
    }
    for ci, cn in enumerate(columns, 1):
        ws.column_dimensions[get_column_letter(ci)].width = col_widths.get(cn, 22)

    ws.row_dimensions[1].height = 30
    ws.freeze_panes = "A2"

    ws_legend = wb.create_sheet("Légende")
    legend_rows = [
        ("Colonne",             "Description"),
        ("LEI_Existant",        "LEI présent dans votre base (issu du fichier d'entrée)"),
        ("LEI_GLEIF",           "LEI retourné par la base GLEIF (validé ou trouvé)"),
        ("GLEIF_DateRenouvellement", "Date de prochaine échéance du LEI selon GLEIF"),
        ("GLEIF_CodePostal",     "Code postal de l'entité tel qu'enregistré dans GLEIF"),
        ("ScoreCorrespondance", "Score de correspondance : 'Nom:xx% / CP:✓' ou 'Nom:xx% / CP:✗' pour Approx Nom/Pays"),
        ("", ""),
        ("Colonnes Disc_* (rouge gras si anomalie détectée)", ""),
        ("Disc_LEI",       "Anomalie sur le LEI : manquant côté client OU différent de GLEIF"),
        ("Disc_RCS",       "Anomalie sur le RCS : manquant côté client OU différent (après normalisation)"),
        ("Disc_Nom",       "Anomalie sur le nom : similarité fuzzy < seuil DQ (70 %)"),
        ("Disc_Date",      "Anomalie sur la date LEI : manquante côté client OU différente de GLEIF"),
        ("Disc_CodePostal","Anomalie sur le code postal :\n"
         "  • Vrai écart : chiffres différents  ex: client='75001' ≠ GLEIF='1000'\n"
         "  • Écart de format : chiffres identiques, libellé différent\n"
         "    ex: client='1338' → GLEIF='L-1338' (à harmoniser dans votre référentiel)"),
        ("", ""),
        ("Couleur",             "Signification du type de correspondance"),
        ("Vert foncé",          "LEI validé (données cohérentes) ou correspondance exacte par RCS"),
        ("Vert clair",          "Correspondance RCS approchée — le RCS client est contenu dans le RCS GLEIF (ex: zéro de tête manquant). ScoreCorrespondance = 'RCS:xx% / Nom:yy%'"),
        ("Jaune",               f"Correspondance approximative nom/pays (score ≥ {threshold} %). ScoreCorrespondance inclut l'indicateur code postal si fourni."),
        ("Orange",              "LEI Discordant — divergence détectée (LEI erroné, RCS/nom/CP différent)"),
        ("Bleu clair",          "Non trouvé (LEI invalide) — introuvable même par RCS/nom"),
        ("Rouge",               "Aucune correspondance trouvée (pas de LEI dans la base d'entrée)"),
        ("", ""),
        ("Logique de détection",""),
        ("1. LEI_Existant fourni + trouvé dans GLEIF",
         "→ comparaison RCS / Nom / Date / Code Postal — Valide ou Discordant"),
        ("2. LEI_Existant fourni + NON trouvé dans GLEIF",
         "→ fallback par RCS puis nom/pays (avec code postal) pour retrouver l'entité\n"
         "   Si trouvé : LEI Discordant (avec comparaison LEI_client vs LEI_GLEIF)\n"
         "   Si introuvable : Non trouvé (LEI invalide)"),
        ("3. Pas de LEI_Existant",
         "→ RCS exact, puis RCS approché (contenance : RCS client ⊆ RCS GLEIF,\n"
         "   ex: zéro de tête manquant), puis approximation nom+pays+code postal"),
        ("", ""),
        ("Code postal",         ""),
        ("Règle de contenance", "Les chiffres du code postal client doivent être contenus\n"
         "dans le code postal GLEIF brut (ex: '1338' ⊆ 'L-1338').\n"
         "CP:✓ = correspondance, CP:✗ = écart (signalé dans Disc_CodePostal)"),
    ]
    for r, (a, b) in enumerate(legend_rows, 1):
        ws_legend.cell(r, 1, a).font = Font(bold=(r == 1))
        ws_legend.cell(r, 2, b).font = Font(bold=(r == 1))
    ws_legend.column_dimensions["A"].width = 25
    ws_legend.column_dimensions["B"].width = 65

    wb.save(output_path)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser(description="GLEIF LEI Matcher v1.2")
    p.add_argument("--input",             required=True)
    p.add_argument("--gleif",             required=True)
    p.add_argument("--output",            required=True)
    p.add_argument("--col-rcs",           default="RCS")
    p.add_argument("--col-name",          default="NomEntreprise")
    p.add_argument("--col-pays",          default="Pays")
    p.add_argument("--col-lei",           default=None,
                   help="Colonne LEI existant dans le fichier d'entrée (ex: LEI_Existant)")
    p.add_argument("--col-date",          default=None,
                   help="Colonne date validité LEI côté client (format dd-mm-yyyy, ex: LEI_DateValidite)")
    p.add_argument("--col-postal",        default=None,
                   help="Colonne code postal côté client (ex: CodePostal)")
    p.add_argument("--fuzzy-threshold",     type=int, default=80,
                   help="Seuil similarité nom/pays (défaut: 80)")
    p.add_argument("--rcs-fuzzy-threshold", type=int, default=88,
                   help="Seuil similarité RCS approché (défaut: 88, 0=désactivé)")
    p.add_argument("--active-only",         action="store_true", default=True)
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
