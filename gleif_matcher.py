"""
gleif_matcher.py
================
Module de rapprochement LEI GLEIF pour fichiers Excel.

Workflow :
  1. Recherche exacte par numéro RCS (normalisé)
  2. Si non trouvé → fuzzy matching par nom d'entreprise + pays
  3. Export Excel enrichi avec colonnes LEI, statut, type de correspondance

Usage :
  python gleif_matcher.py \
      --input     societes.xlsx \
      --gleif     gleif_golden_copy.csv \
      --output    resultats_LEI.xlsx \
      [--col-rcs  RCS] \
      [--col-name NomEntreprise] \
      [--col-pays Pays] \
      [--fuzzy-threshold 80] \
      [--active-only]

Colonnes GLEIF attendues (Golden Copy CSV) :
  LEI, Entity.LegalName, Entity.LegalAddress.Country,
  Entity.EntityStatus, Registration.RegistrationStatus,
  Registration.RegistrationAuthorityID,
  Registration.RegistrationAuthorityEntityID

Statuts importants :
  Entity.EntityStatus          → statut de la SOCIÉTÉ  : ACTIVE / INACTIVE / MERGED
  Registration.RegistrationStatus → statut du LEI        : ISSUED / LAPSED / RETIRED / …
  Le filtre --active-only applique les deux : EntityStatus=ACTIVE ET RegistrationStatus=ISSUED
"""

import argparse
import logging
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from rapidfuzz import fuzz, process

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Mapping pays → code ISO 3166-1 alpha-2
# (noms français + anglais les plus courants)
# ---------------------------------------------------------------------------
COUNTRY_MAP: Dict[str, str] = {
    # France
    "france": "FR", "fr": "FR",
    # Allemagne
    "allemagne": "DE", "germany": "DE", "de": "DE",
    # Italie
    "italie": "IT", "italy": "IT", "it": "IT",
    # Espagne
    "espagne": "ES", "spain": "ES", "es": "ES",
    # Belgique
    "belgique": "BE", "belgium": "BE", "be": "BE",
    # Suisse
    "suisse": "CH", "switzerland": "CH", "ch": "CH",
    # Luxembourg
    "luxembourg": "LU", "lu": "LU",
    # Pays-Bas
    "pays-bas": "NL", "netherlands": "NL", "nl": "NL", "hollande": "NL",
    # Royaume-Uni
    "royaume-uni": "GB", "united kingdom": "GB", "uk": "GB", "gb": "GB",
    "angleterre": "GB", "england": "GB",
    # États-Unis
    "etats-unis": "US", "états-unis": "US", "united states": "US",
    "usa": "US", "us": "US",
    # Portugal
    "portugal": "PT", "pt": "PT",
    # Autriche
    "autriche": "AT", "austria": "AT", "at": "AT",
    # Suède
    "suede": "SE", "suède": "SE", "sweden": "SE", "se": "SE",
    # Danemark
    "danemark": "DK", "denmark": "DK", "dk": "DK",
    # Norvège
    "norvege": "NO", "norvège": "NO", "norway": "NO", "no": "NO",
    # Finlande
    "finlande": "FI", "finland": "FI", "fi": "FI",
    # Pologne
    "pologne": "PL", "poland": "PL", "pl": "PL",
    # République tchèque
    "republique tcheque": "CZ", "czech republic": "CZ",
    "czechia": "CZ", "cz": "CZ",
    # Irlande
    "irlande": "IE", "ireland": "IE", "ie": "IE",
    # Grèce
    "grece": "GR", "grèce": "GR", "greece": "GR", "gr": "GR",
    # Roumanie
    "roumanie": "RO", "romania": "RO", "ro": "RO",
    # Hongrie
    "hongrie": "HU", "hungary": "HU", "hu": "HU",
    # Japon
    "japon": "JP", "japan": "JP", "jp": "JP",
    # Chine
    "chine": "CN", "china": "CN", "cn": "CN",
    # Canada
    "canada": "CA", "ca": "CA",
    # Australie
    "australie": "AU", "australia": "AU", "au": "AU",
    # Singapore
    "singapour": "SG", "singapore": "SG", "sg": "SG",
    # Emirats arabes unis
    "emirats arabes unis": "AE", "uae": "AE", "ae": "AE",
    # Monaco
    "monaco": "MC", "mc": "MC",
    # Liechtenstein
    "liechtenstein": "LI", "li": "LI",
    # Andorre
    "andorre": "AD", "andorra": "AD", "ad": "AD",
    # Ile Maurice
    "ile maurice": "MU", "mauritius": "MU", "mu": "MU",
    # Maroc
    "maroc": "MA", "morocco": "MA", "ma": "MA",
}

# Codes ISO déjà valides (2 lettres maj)
_ISO_PATTERN = re.compile(r"^[A-Z]{2}$")

# Formes juridiques à retirer lors de la normalisation des noms
_LEGAL_FORMS = (
    r"\bS\.?A\.?S\.?U?\b", r"\bS\.?A\.?R\.?L\.?\b", r"\bS\.?A\.?\b",
    r"\bS\.?N\.?C\.?\b", r"\bS\.?C\.?I\.?\b", r"\bE\.?U\.?R\.?L\.?\b",
    r"\bG\.?I\.?E\.?\b", r"\bS\.?C\.?M\.?\b", r"\bS\.?C\.?P\.?\b",
    r"\bS\.?C\.?S\.?\b", r"\bS\.?C\.?\b", r"\bG\.?M\.?B\.?H\.?\b",
    r"\bA\.?G\.?\b", r"\bL\.?T\.?D\.?\b", r"\bP\.?L\.?C\.?\b",
    r"\bI\.?N\.?C\.?\b", r"\bL\.?L\.?C\.?\b", r"\bB\.?V\.?\b",
    r"\bN\.?V\.?\b", r"\bS\.?P\.?A\.?\b", r"\bS\.?R\.?L\.?\b",
)
_LEGAL_FORMS_RE = re.compile("|".join(_LEGAL_FORMS), re.IGNORECASE)


# ---------------------------------------------------------------------------
# Fonctions utilitaires
# ---------------------------------------------------------------------------

def normalize_rcs(value) -> str:
    """
    Normalise un numéro RCS/SIREN en conservant uniquement les chiffres
    et lettres (supprime espaces, points, tirets, slash, etc.).

    Ex : "RCS Paris 123 456 789" → "123456789"
         "123.456.789 B"         → "123456789B"
    """
    if pd.isna(value) or str(value).strip() == "":
        return ""
    raw = str(value).upper()
    # Supprimer le préfixe "RCS <ville>" éventuel
    raw = re.sub(r"^RCS\s+[A-ZÉÈÀÂÊÎÔÙÛÇ\s]+\s+", "", raw).strip()
    # Garder uniquement alphanumériques
    return re.sub(r"[^0-9A-Z]", "", raw)


def normalize_name(value) -> str:
    """
    Normalise un nom d'entreprise pour la comparaison :
    - Majuscules
    - Suppression des accents
    - Suppression des formes juridiques courantes
    - Suppression des caractères spéciaux et espaces multiples
    """
    if pd.isna(value) or str(value).strip() == "":
        return ""
    name = str(value).upper()
    # Supprimer les accents
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    # Supprimer les formes juridiques
    name = _LEGAL_FORMS_RE.sub(" ", name)
    # Supprimer la ponctuation sauf espaces
    name = re.sub(r"[^A-Z0-9\s]", " ", name)
    # Compresser les espaces
    return re.sub(r"\s+", " ", name).strip()


def country_to_iso(value) -> str:
    """
    Convertit un nom de pays (français ou anglais) ou un code ISO
    en code ISO 3166-1 alpha-2 en majuscules.
    Retourne '' si non reconnu.
    """
    if pd.isna(value) or str(value).strip() == "":
        return ""
    raw = str(value).strip().upper()
    # Déjà un code ISO à 2 lettres
    if _ISO_PATTERN.match(raw):
        return raw
    # Chercher dans le mapping (clé en minuscules)
    key = raw.lower()
    # Supprimer les accents pour la recherche
    key_no_accent = "".join(
        c for c in unicodedata.normalize("NFD", key)
        if unicodedata.category(c) != "Mn"
    )
    return COUNTRY_MAP.get(key_no_accent, COUNTRY_MAP.get(key, ""))


# ---------------------------------------------------------------------------
# Chargement de la base GLEIF
# ---------------------------------------------------------------------------

GLEIF_COLS = {
    "lei":          "LEI",
    "name":         "Entity.LegalName",
    "country":      "Entity.LegalAddress.Country",
    "entity_status":"Entity.EntityStatus",          # statut de la SOCIÉTÉ
    "lei_status":   "Registration.RegistrationStatus",  # statut du LEI
    "ra_id":        "Registration.RegistrationAuthorityID",
    "ra_entity":    "Registration.RegistrationAuthorityEntityID",
}


def load_gleif(gleif_path: str, active_only: bool = True) -> pd.DataFrame:
    """
    Charge le fichier Golden Copy GLEIF (CSV ou JSON).

    Paramètres
    ----------
    gleif_path  : chemin vers le fichier GLEIF téléchargé
    active_only : si True, ne charge que les entités avec EntityStatus=ACTIVE

    Retourne un DataFrame avec les colonnes normalisées.
    """
    path = Path(gleif_path)
    log.info(f"Chargement GLEIF depuis : {path.name} …")

    suffix = path.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(
            gleif_path,
            usecols=list(GLEIF_COLS.values()),
            dtype=str,
            low_memory=False,
        )
    elif suffix == ".json":
        raw = pd.read_json(gleif_path, dtype=str)
        # Le JSON GLEIF peut avoir une structure imbriquée – on tente un flatten
        if "LEI" not in raw.columns:
            raw = pd.json_normalize(raw.to_dict(orient="records"))
        df = raw[[c for c in GLEIF_COLS.values() if c in raw.columns]]
    else:
        raise ValueError(f"Format non supporté : {suffix}. Utilisez CSV ou JSON.")

    # Renommer pour faciliter l'usage interne
    df = df.rename(columns={v: k for k, v in GLEIF_COLS.items()})

    # S'assurer que toutes les colonnes existent
    for col in GLEIF_COLS:
        if col not in df.columns:
            df[col] = ""

    df = df.fillna("")

    if active_only:
        before = len(df)
        # Double filtre :
        #   Entity.EntityStatus          == ACTIVE  → société encore existante
        #   Registration.RegistrationStatus == ISSUED  → LEI valide et à jour
        mask = (
            (df["entity_status"].str.upper() == "ACTIVE") &
            (df["lei_status"].str.upper() == "ISSUED")
        )
        df = df[mask].copy()
        log.info(
            f"  Filtre Entity=ACTIVE + LEI=ISSUED : {before:,} → {len(df):,} entités"
        )
    else:
        log.info(f"  Entités chargées (tous statuts) : {len(df):,}")

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Construction des index
# ---------------------------------------------------------------------------

def build_indices(
    df: pd.DataFrame,
) -> Tuple[Dict[str, List[int]], Dict[str, Dict[str, List[int]]]]:
    """
    Construit deux index pour accélérer la recherche :

    rcs_index   : { rcs_normalisé → [indices dans df] }
    name_index  : { code_pays → { nom_normalisé → [indices dans df] } }
    """
    log.info("Construction des index …")

    # --- Index RCS ---
    rcs_index: Dict[str, List[int]] = {}
    for i, row in enumerate(df["ra_entity"]):
        key = normalize_rcs(row)
        if key:
            rcs_index.setdefault(key, []).append(i)

    # --- Index nom/pays ---
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


# ---------------------------------------------------------------------------
# Fonctions de recherche
# ---------------------------------------------------------------------------

def search_by_rcs(
    rcs_norm: str, rcs_index: Dict[str, List[int]], df: pd.DataFrame
) -> Optional[pd.Series]:
    """Recherche exacte par RCS normalisé. Retourne la première ligne trouvée."""
    if not rcs_norm:
        return None
    indices = rcs_index.get(rcs_norm)
    if indices:
        return df.iloc[indices[0]]
    return None


def search_by_name_country(
    name_norm: str,
    iso_country: str,
    name_index: Dict[str, Dict[str, List[int]]],
    df: pd.DataFrame,
    threshold: int = 80,
) -> Tuple[Optional[pd.Series], int]:
    """
    Recherche fuzzy par nom normalisé + pays ISO.

    Retourne (ligne_gleif, score) ou (None, 0) si aucune correspondance
    n'atteint le seuil.
    """
    if not name_norm or not iso_country:
        return None, 0

    country_names = name_index.get(iso_country, {})
    if not country_names:
        return None, 0

    candidates = list(country_names.keys())
    result = process.extractOne(
        name_norm,
        candidates,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=threshold,
    )
    if result is None:
        return None, 0

    best_name, score, _ = result
    indices = country_names[best_name]
    return df.iloc[indices[0]], int(score)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def match_companies(
    input_path: str,
    gleif_path: str,
    output_path: str,
    col_rcs: str = "RCS",
    col_name: str = "NomEntreprise",
    col_pays: str = "Pays",
    fuzzy_threshold: int = 80,
    active_only: bool = True,
) -> pd.DataFrame:
    """
    Charge le fichier d'entrée, effectue le rapprochement GLEIF,
    et exporte le résultat enrichi en Excel.

    Retourne le DataFrame résultat.
    """
    # 1. Charger le fichier d'entrée
    log.info(f"Lecture du fichier d'entrée : {input_path}")
    df_input = pd.read_excel(input_path, dtype=str)
    df_input = df_input.fillna("")
    log.info(f"  {len(df_input):,} lignes chargées")

    # Vérifier les colonnes
    missing = [c for c in [col_rcs, col_name, col_pays] if c not in df_input.columns]
    if missing:
        raise ValueError(
            f"Colonnes manquantes dans le fichier d'entrée : {missing}\n"
            f"Colonnes disponibles : {list(df_input.columns)}"
        )

    # 2. Charger GLEIF
    df_gleif = load_gleif(gleif_path, active_only=active_only)

    # 3. Construire les index
    rcs_index, name_index = build_indices(df_gleif)

    # 4. Rapprochement ligne par ligne
    results = []
    log.info("Rapprochement en cours …")

    for idx, row in df_input.iterrows():
        rcs_raw   = str(row[col_rcs]).strip()
        name_raw  = str(row[col_name]).strip()
        pays_raw  = str(row[col_pays]).strip()

        rcs_norm   = normalize_rcs(rcs_raw)
        name_norm  = normalize_name(name_raw)
        iso        = country_to_iso(pays_raw) if pays_raw else ""

        gleif_row  = None
        match_type = "Non trouvé"
        match_score = ""

        # -- Étape 1 : correspondance exacte par RCS --
        if rcs_norm:
            gleif_row = search_by_rcs(rcs_norm, rcs_index, df_gleif)
            if gleif_row is not None:
                match_type  = "Exact – RCS"
                match_score = 100

        # -- Étape 2 : fuzzy matching par nom + pays --
        if gleif_row is None and name_norm:
            gleif_row, score = search_by_name_country(
                name_norm, iso, name_index, df_gleif, fuzzy_threshold
            )
            if gleif_row is not None:
                match_type  = "Approx – Nom/Pays"
                match_score = score

        # Construire la ligne de résultat
        if gleif_row is not None:
            results.append(
                {
                    "LEI":                      gleif_row["lei"],
                    "GLEIF_NomLegal":           gleif_row["name"],
                    "GLEIF_Pays":               gleif_row["country"],
                    "GLEIF_StatutSociete":      gleif_row["entity_status"],   # ACTIVE / INACTIVE
                    "GLEIF_StatutLEI":          gleif_row["lei_status"],       # ISSUED / LAPSED
                    "GLEIF_AutoriteRegistre":   gleif_row["ra_id"],
                    "GLEIF_NumRegistre":        gleif_row["ra_entity"],
                    "TypeCorrespondance":       match_type,
                    "ScoreCorrespondance":      match_score,
                }
            )
        else:
            results.append(
                {
                    "LEI":                     "",
                    "GLEIF_NomLegal":          "",
                    "GLEIF_Pays":             "",
                    "GLEIF_StatutSociete":    "",
                    "GLEIF_StatutLEI":        "",
                    "GLEIF_AutoriteRegistre": "",
                    "GLEIF_NumRegistre":      "",
                    "TypeCorrespondance":     match_type,
                    "ScoreCorrespondance":    "",
                }
            )

        if (idx + 1) % 100 == 0:
            log.info(f"  {idx + 1}/{len(df_input)} lignes traitées …")

    # 5. Assembler le DataFrame résultat
    df_results = pd.DataFrame(results)
    df_output  = pd.concat([df_input.reset_index(drop=True), df_results], axis=1)

    # 6. Export Excel avec mise en forme
    log.info(f"Export vers : {output_path}")
    _export_excel(df_output, output_path, fuzzy_threshold)

    # Statistiques
    n_exact  = (df_results["TypeCorrespondance"] == "Exact – RCS").sum()
    n_approx = (df_results["TypeCorrespondance"] == "Approx – Nom/Pays").sum()
    n_miss   = (df_results["TypeCorrespondance"] == "Non trouvé").sum()
    log.info(
        f"\n{'='*50}\n"
        f"  Total lignes          : {len(df_input):>6,}\n"
        f"  Exact – RCS           : {n_exact:>6,}  ({n_exact/len(df_input)*100:.1f}%)\n"
        f"  Approx – Nom/Pays     : {n_approx:>6,}  ({n_approx/len(df_input)*100:.1f}%)\n"
        f"  Non trouvé            : {n_miss:>6,}  ({n_miss/len(df_input)*100:.1f}%)\n"
        f"{'='*50}"
    )

    return df_output


# ---------------------------------------------------------------------------
# Export Excel avec couleurs
# ---------------------------------------------------------------------------

def _export_excel(df: pd.DataFrame, output_path: str, threshold: int) -> None:
    """Exporte le DataFrame en Excel avec mise en forme conditionnelle."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "Résultats LEI"

    # Couleurs
    HEADER_FILL   = PatternFill("solid", fgColor="1F4E79")   # bleu foncé
    GLEIF_FILL    = PatternFill("solid", fgColor="D6E4F0")   # bleu clair
    EXACT_FILL    = PatternFill("solid", fgColor="D9EAD3")   # vert clair
    APPROX_FILL   = PatternFill("solid", fgColor="FFF2CC")   # jaune
    MISS_FILL     = PatternFill("solid", fgColor="FCE4D6")   # rouge clair

    thin = Side(style="thin", color="AAAAAA")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    header_font = Font(name="Arial", bold=True, color="FFFFFF", size=10)
    data_font   = Font(name="Arial", size=10)

    # Identifier les colonnes enrichies GLEIF
    gleif_cols = [
        "LEI", "GLEIF_NomLegal", "GLEIF_Pays",
        "GLEIF_StatutSociete", "GLEIF_StatutLEI",
        "GLEIF_AutoriteRegistre", "GLEIF_NumRegistre",
        "TypeCorrespondance", "ScoreCorrespondance",
    ]

    columns = list(df.columns)

    # En-têtes
    for col_idx, col_name in enumerate(columns, start=1):
        cell = ws.cell(row=1, column=col_idx, value=col_name)
        cell.font      = header_font
        cell.fill      = HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = border

    # Données
    for row_idx, row in enumerate(df.itertuples(index=False), start=2):
        match_type = getattr(row, "TypeCorrespondance", "")
        if match_type == "Exact – RCS":
            row_fill = EXACT_FILL
        elif match_type == "Approx – Nom/Pays":
            row_fill = APPROX_FILL
        elif match_type == "Non trouvé":
            row_fill = MISS_FILL
        else:
            row_fill = None

        for col_idx, (col_name, value) in enumerate(zip(columns, row), start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.font   = data_font
            cell.border = border
            cell.alignment = Alignment(vertical="center")

            if col_name in gleif_cols and row_fill:
                cell.fill = row_fill
            elif col_name in gleif_cols:
                cell.fill = GLEIF_FILL

    # Largeurs de colonnes auto
    col_widths = {
        "LEI":                    25,
        "GLEIF_NomLegal":         35,
        "GLEIF_Pays":             10,
        "GLEIF_StatutSociete":    16,
        "GLEIF_StatutLEI":        14,
        "GLEIF_AutoriteRegistre": 18,
        "GLEIF_NumRegistre":      20,
        "TypeCorrespondance":     20,
        "ScoreCorrespondance":    12,
    }
    for col_idx, col_name in enumerate(columns, start=1):
        col_letter = get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = col_widths.get(col_name, 22)

    ws.row_dimensions[1].height = 30
    ws.freeze_panes = "A2"

    # Onglet légende
    ws_legend = wb.create_sheet("Légende")
    legend_data = [
        ("Couleur", "Signification"),
        ("Vert",    "Correspondance exacte par numéro RCS"),
        ("Jaune",   f"Correspondance approximative par nom/pays (score ≥ {threshold})"),
        ("Rouge",   "Aucune correspondance trouvée"),
        ("Bleu",    "Colonnes issues de la base GLEIF"),
    ]
    for r, (col_a, col_b) in enumerate(legend_data, start=1):
        ws_legend.cell(row=r, column=1, value=col_a).font = Font(bold=(r == 1))
        ws_legend.cell(row=r, column=2, value=col_b).font = Font(bold=(r == 1))
    ws_legend.column_dimensions["A"].width = 15
    ws_legend.column_dimensions["B"].width = 55

    wb.save(output_path)
    log.info("  Fichier Excel sauvegardé avec mise en forme.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Rapprochement LEI GLEIF à partir d'un fichier Excel"
    )
    parser.add_argument("--input",   required=True, help="Fichier Excel d'entrée (.xlsx)")
    parser.add_argument("--gleif",   required=True, help="Fichier GLEIF Golden Copy (CSV ou JSON)")
    parser.add_argument("--output",  required=True, help="Fichier Excel de sortie (.xlsx)")
    parser.add_argument("--col-rcs",  default="RCS",            help="Nom de la colonne RCS  (défaut: RCS)")
    parser.add_argument("--col-name", default="NomEntreprise",  help="Nom de la colonne nom  (défaut: NomEntreprise)")
    parser.add_argument("--col-pays", default="Pays",           help="Nom de la colonne pays (défaut: Pays)")
    parser.add_argument("--fuzzy-threshold", type=int, default=80,
                        help="Seuil de similarité pour le fuzzy matching (0-100, défaut: 80)")
    parser.add_argument("--active-only", action="store_true", default=True,
                        help="Ne traiter que les entités GLEIF actives (défaut: oui)")
    parser.add_argument("--all-statuses", dest="active_only", action="store_false",
                        help="Inclure aussi les entités non-actives")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    match_companies(
        input_path      = args.input,
        gleif_path      = args.gleif,
        output_path     = args.output,
        col_rcs         = args.col_rcs,
        col_name        = args.col_name,
        col_pays        = args.col_pays,
        fuzzy_threshold = args.fuzzy_threshold,
        active_only     = args.active_only,
    )
