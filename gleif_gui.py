"""
gleif_gui.py
============
Interface graphique GLEIF LEI Matcher v2.1 — Bento Edition.

Refonte UX/UI complète :
  • customtkinter avec coins arrondis et thème clair Société Générale.
  • Layout Bento : grille de cartes blanches sur fond gris très clair.
  • Charte SG : Rouge #E60028 / Noir / Anthracite #333333.
  • Mode Focus : paramètres techniques masqués dans une modale "⚙️ Paramètres
    avancés" pour ne montrer que l'essentiel à l'écran principal.
  • Dashboard dynamique avec compteurs live (✅ ⚠️ ❌).
  • Footer disclaimer élégant + zone de logs rétractable.

Le module conserve l'intégralité de la logique métier de gleif_matcher.py.

Dépendances : customtkinter, pandas, openpyxl, rapidfuzz.
"""

import os
import sys
import json
import threading
import subprocess
import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, StringVar, IntVar, BooleanVar, DoubleVar

try:
    import customtkinter as ctk
except ImportError:
    import tkinter.messagebox as _mb
    _mb.showerror(
        "Dépendance manquante",
        "Le module 'customtkinter' est requis.\n\n"
        "Installation :\n  pip install customtkinter\n\n"
        "Puis relancez l'application.",
    )
    sys.exit(1)

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

TEAM_CONFIG_PATH = BASE_DIR / "gleif_config.json"
USER_PREFS_PATH  = Path.home() / ".gleif_matcher_prefs.json"
LOGO_PATH        = BASE_DIR / "assets" / "logo_sg.png"

APP_VERSION = "v2.2"

# ─────────────────────────────────────────────────────────────────────────────
# Charte Société Générale — Bento Edition
# ─────────────────────────────────────────────────────────────────────────────
SG_RED            = "#E60028"
SG_RED_HOVER      = "#B3001F"
SG_BLACK          = "#000000"
SG_ANTHRACITE     = "#333333"
SG_GREY           = "#6B7280"
SG_GREY_LIGHT     = "#F2F2F2"      # Fond de fenêtre
SG_GREY_BORDER    = "#E5E7EB"      # Bordure douce des cartes
SG_WHITE          = "#FFFFFF"

# Sémantique
C_OK              = "#16A34A"      # Vert émeraude
C_OK_BG           = "#DCFCE7"
C_WARN            = "#F59E0B"      # Orange
C_WARN_BG         = "#FEF3C7"
C_ERR             = "#DC2626"
C_ERR_BG          = "#FEE2E2"

CARD_RADIUS       = 15
CARD_PADDING      = 18
CARD_BORDER       = 1


# ─────────────────────────────────────────────────────────────────────────────
# Persistance
# ─────────────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    defaults = {
        "gleif_path": "", "col_rcs": "RCS", "col_name": "NomEntreprise",
        "col_pays": "Pays", "col_lei": "LEI_Existant",
        "col_date": "LEI_DateValidite", "col_postal": "CodePostal",
        "fuzzy_threshold": 90, "rcs_fuzzy_threshold": 88,
        "active_only": True,
        "last_input": "", "last_output": "", "use_slim": False,
    }
    if TEAM_CONFIG_PATH.exists():
        try:
            with open(TEAM_CONFIG_PATH, encoding="utf-8") as f:
                team = {k: v for k, v in json.load(f).items() if not k.startswith("_")}
            defaults.update(team)
        except Exception:
            pass
    if USER_PREFS_PATH.exists():
        try:
            with open(USER_PREFS_PATH, encoding="utf-8") as f:
                defaults.update(json.load(f))
        except Exception:
            pass
    return defaults


def save_user_prefs(prefs: dict) -> None:
    try:
        with open(USER_PREFS_PATH, "w", encoding="utf-8") as f:
            json.dump(prefs, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _browse_file(var: StringVar, title: str, filetypes, save: bool = False):
    if save:
        path = filedialog.asksaveasfilename(title=title, filetypes=filetypes, defaultextension=".xlsx")
    else:
        path = filedialog.askopenfilename(title=title, filetypes=filetypes)
    if path:
        var.set(path)


def _is_onedrive_path(path: str) -> bool:
    return "onedrive" in (path or "").lower()


def _translate_error(exc: BaseException) -> str:
    msg = str(exc)
    low = msg.lower()
    name = type(exc).__name__
    if "407" in msg or "proxy authentication" in low:
        return ("Action requise : authentification proxy refusée.\n\n"
                "• Videz le champ Proxy dans Paramètres avancés.\n"
                "• Si le problème persiste, contactez votre support IT.")
    if "errno 13" in low or "permission denied" in low or name == "PermissionError":
        return ("Action requise : fichier verrouillé.\n\n"
                "• Fermez le fichier dans Excel s'il est ouvert.\n"
                "• Si c'est un fichier OneDrive, attendez la synchronisation.")
    if name == "FileNotFoundError" or "no such file" in low:
        return f"Action requise : fichier introuvable.\n\nVérifiez le chemin :\n{msg}"
    if name == "MemoryError" or ("memory" in low and "error" in low):
        return ("Action requise : mémoire insuffisante.\n\n"
                "• Activez « Utiliser la base slim » dans Paramètres avancés.\n"
                "• Ou utilisez Python 64 bits.")
    if "11001" in msg or "getaddrinfo" in low:
        return "Action requise : impossible de joindre le serveur GLEIF.\n\nVérifiez votre connexion réseau."
    if "timeout" in low:
        return "Action requise : délai d'attente dépassé. Réessayez dans quelques minutes."
    if "ssl" in low or "certificate" in low:
        return "Action requise : problème de certificat SSL. Contactez votre support IT."
    if "colonnes manquantes" in low or "colonnes introuvables" in low:
        return f"Action requise : colonnes du fichier source incorrectes.\n\n{msg}"
    return f"Erreur inattendue ({name}) :\n\n{msg}"


def _file_age_status(path: str) -> tuple:
    """Retourne (color, label) selon la fraîcheur du fichier GLEIF."""
    if not path or not Path(path).exists():
        return C_ERR, "Aucune base sélectionnée"
    try:
        is_turbo = path.lower().endswith(".db")
        mtime = datetime.datetime.fromtimestamp(Path(path).stat().st_mtime)
        age = (datetime.datetime.now() - mtime).days
        prefix = "⚡ Cache SQLite • " if is_turbo else ""
        if age < 7:
            return C_OK, f"{prefix}Base à jour ({age} j)"
        if age < 30:
            return C_WARN, f"{prefix}Base de {age} j"
        return C_ERR, f"{prefix}Base de {age} j — mise à jour recommandée"
    except Exception:
        return SG_GREY, "Statut inconnu"


# ─────────────────────────────────────────────────────────────────────────────
# Carte Bento — composant réutilisable
# ─────────────────────────────────────────────────────────────────────────────

class BentoCard(ctk.CTkFrame):
    """Carte blanche à coins arrondis pour le layout Bento."""

    def __init__(self, master, **kwargs):
        kwargs.setdefault("corner_radius", CARD_RADIUS)
        kwargs.setdefault("fg_color", SG_WHITE)
        kwargs.setdefault("border_width", CARD_BORDER)
        kwargs.setdefault("border_color", SG_GREY_BORDER)
        super().__init__(master, **kwargs)

    def title(self, text: str, icon: str = "") -> ctk.CTkLabel:
        label = ctk.CTkLabel(
            self, text=f"{icon}  {text}".strip(),
            font=ctk.CTkFont(size=13, weight="bold"),
            text_color=SG_ANTHRACITE, anchor="w",
        )
        return label


# ─────────────────────────────────────────────────────────────────────────────
# Application principale
# ─────────────────────────────────────────────────────────────────────────────

class GleifApp(ctk.CTk):

    def __init__(self):
        super().__init__()
        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")  # surchargé par nos couleurs SG

        self.title("LEI Matcher — Société Générale")
        self.configure(fg_color=SG_GREY_LIGHT)
        self.minsize(960, 720)
        self._center_window(1040, 780)

        cfg = load_config()
        self.v_input      = StringVar(value=cfg.get("last_input", ""))
        self.v_gleif      = StringVar(value=cfg.get("gleif_path", ""))
        self.v_output     = StringVar(value=cfg.get("last_output", ""))
        self.v_col_rcs    = StringVar(value=cfg.get("col_rcs",    "RCS"))
        self.v_col_name   = StringVar(value=cfg.get("col_name",   "NomEntreprise"))
        self.v_col_pays   = StringVar(value=cfg.get("col_pays",   "Pays"))
        self.v_col_lei    = StringVar(value=cfg.get("col_lei",    "LEI_Existant"))
        self.v_col_date   = StringVar(value=cfg.get("col_date",   "LEI_DateValidite"))
        self.v_col_postal = StringVar(value=cfg.get("col_postal", "CodePostal"))
        self.v_threshold     = IntVar(value=int(cfg.get("fuzzy_threshold", 90)))
        self.v_rcs_threshold = IntVar(value=int(cfg.get("rcs_fuzzy_threshold", 88)))
        self.v_active   = BooleanVar(value=bool(cfg.get("active_only", True)))
        self.v_use_slim = BooleanVar(value=bool(cfg.get("use_slim", False)))
        self.v_progress = DoubleVar(value=0)
        self.v_status   = StringVar(value="Prêt — sélectionnez vos fichiers et lancez le rapprochement.")

        # Compteurs live du dashboard
        self.v_count_total = StringVar(value="—")
        self.v_count_ok    = StringVar(value="0")
        self.v_count_warn  = StringVar(value="0")
        self.v_count_ko    = StringVar(value="0")

        # Proxy
        _proxy = cfg.get("proxy", None)
        if _proxy is None:
            try:
                from gleif_updater import detect_system_proxy
                _proxy = detect_system_proxy() or ""
            except Exception:
                _proxy = ""
        self.v_proxy = StringVar(value=_proxy)

        self._logs_visible = False
        self._welcome_shown = False
        self._build_ui()
        # Validation initiale + écoute des changements pour rester "incassable"
        self._refresh_data_validity()
        self.v_gleif.trace_add("write", lambda *_: self._refresh_data_validity())
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(500, self._check_python_arch)
        self.after(700, self._maybe_show_welcome)

    # ── Layout principal ────────────────────────────────────────────────────

    def _build_ui(self):
        self._build_header()
        self._build_grid()
        self._build_footer()

    def _build_header(self):
        header = ctk.CTkFrame(
            self, height=58, corner_radius=0,
            fg_color=SG_WHITE, border_width=0,
        )
        header.pack(fill="x", side="top")
        header.pack_propagate(False)

        # ── Logo + nom banque ─────────────────────────────────────────────
        left = ctk.CTkFrame(header, fg_color="transparent")
        left.pack(side="left", padx=24)

        # Barre rouge SG
        ctk.CTkFrame(left, fg_color=SG_RED, width=4, height=34, corner_radius=2
                     ).pack(side="left", padx=(0, 12), pady=12)

        ctk.CTkLabel(
            left, text="SOCIÉTÉ GÉNÉRALE",
            font=ctk.CTkFont(family="Arial", size=15, weight="bold"),
            text_color=SG_RED,
        ).pack(side="left", pady=18)

        # ── Nom outil + statut base GLEIF ─────────────────────────────────
        right = ctk.CTkFrame(header, fg_color="transparent")
        right.pack(side="right", padx=24)

        self.lbl_status_dot = ctk.CTkLabel(
            right, text="●",
            font=ctk.CTkFont(size=16, weight="bold"),
            text_color=SG_GREY,
        )
        self.lbl_status_dot.pack(side="right", padx=(8, 0), pady=18)

        self.lbl_status_text = ctk.CTkLabel(
            right, text="Statut base : —",
            font=ctk.CTkFont(size=11),
            text_color=SG_ANTHRACITE,
        )
        self.lbl_status_text.pack(side="right", pady=18)

        ctk.CTkFrame(right, fg_color=SG_GREY_BORDER, width=1, height=24
                     ).pack(side="right", padx=14, pady=18)

        ctk.CTkLabel(
            right, text=f"LEI Matcher  {APP_VERSION}",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color=SG_ANTHRACITE,
        ).pack(side="right", pady=18)

        # Liseré rouge sous le header
        ctk.CTkFrame(self, fg_color=SG_RED, height=3, corner_radius=0
                     ).pack(fill="x", side="top")

    def _build_grid(self):
        # Container principal scrollable-ready (mais on garde fixe pour Bento clean)
        main = ctk.CTkFrame(self, fg_color=SG_GREY_LIGHT)
        main.pack(fill="both", expand=True, padx=24, pady=20)

        # Grille responsive : 2 colonnes égales pour la rangée du haut,
        # rangées suivantes en pleine largeur.
        main.grid_columnconfigure(0, weight=1, uniform="bento")
        main.grid_columnconfigure(1, weight=1, uniform="bento")
        main.grid_rowconfigure(2, weight=1)  # dashboard prend l'espace vertical

        # ── Carte Source (haut gauche) ────────────────────────────────────
        card_src = BentoCard(main)
        card_src.grid(row=0, column=0, sticky="nsew", padx=(0, 10), pady=(0, 10))
        self._build_source_card(card_src)

        # ── Carte GLEIF (haut droite) ─────────────────────────────────────
        card_gleif = BentoCard(main)
        card_gleif.grid(row=0, column=1, sticky="nsew", padx=(10, 0), pady=(0, 10))
        self._build_gleif_card(card_gleif)

        # ── Carte Dashboard (centre, full width) ──────────────────────────
        card_dash = BentoCard(main)
        card_dash.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(10, 10))
        self._build_dashboard_card(card_dash)

        # ── Carte Action (bas, full width) ────────────────────────────────
        card_action = BentoCard(main)
        card_action.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(10, 0))
        self._build_action_card(card_action)

    def _build_source_card(self, card: BentoCard):
        card.grid_columnconfigure(0, weight=1)
        card.title("Fichier source", "📄").grid(
            row=0, column=0, sticky="w", padx=CARD_PADDING, pady=(CARD_PADDING, 8)
        )

        self._file_field(card, row=1, label="Sociétés (.xlsx)",
                         var=self.v_input, save=False,
                         filetypes=[("Excel", "*.xlsx")])

        self._file_field(card, row=3, label="Fichier de sortie (.xlsx)",
                         var=self.v_output, save=True,
                         filetypes=[("Excel", "*.xlsx")])

    def _build_gleif_card(self, card: BentoCard):
        card.grid_columnconfigure(0, weight=1)
        card.title("Base GLEIF", "🗄").grid(
            row=0, column=0, sticky="w", padx=CARD_PADDING, pady=(CARD_PADDING, 8)
        )

        # Champ + bouton parcourir
        row = ctk.CTkFrame(card, fg_color="transparent")
        row.grid(row=1, column=0, sticky="ew", padx=CARD_PADDING, pady=(0, 4))
        row.grid_columnconfigure(0, weight=1)
        ctk.CTkEntry(
            row, textvariable=self.v_gleif, height=34,
            border_color=SG_GREY_BORDER, border_width=1,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ctk.CTkButton(
            row, text="Parcourir", width=90, height=34,
            fg_color=SG_ANTHRACITE, hover_color=SG_BLACK, text_color=SG_WHITE,
            corner_radius=8, font=ctk.CTkFont(size=11),
            command=lambda: _browse_file(
                self.v_gleif, "Base GLEIF",
                [("CSV", "*.csv"), ("Tous", "*.*")],
            ),
        ).grid(row=0, column=1)

        # Bouton mise à jour intégré
        ctk.CTkButton(
            card, text="🔄  Mettre à jour la base GLEIF",
            height=38,
            fg_color=SG_RED, hover_color=SG_RED_HOVER, text_color=SG_WHITE,
            corner_radius=8, font=ctk.CTkFont(size=12, weight="bold"),
            command=self._open_update_dialog,
        ).grid(row=2, column=0, sticky="ew", padx=CARD_PADDING, pady=(8, 4))

        # Indicateur de fraîcheur
        self.lbl_gleif_age = ctk.CTkLabel(
            card, text="—",
            font=ctk.CTkFont(size=10),
            text_color=SG_GREY,
        )
        self.lbl_gleif_age.grid(row=3, column=0, sticky="w",
                                padx=CARD_PADDING, pady=(4, CARD_PADDING))

    def _build_dashboard_card(self, card: BentoCard):
        card.grid_columnconfigure(0, weight=1)

        # Titre dashboard
        header = ctk.CTkFrame(card, fg_color="transparent")
        header.grid(row=0, column=0, sticky="ew",
                    padx=CARD_PADDING, pady=(CARD_PADDING, 12))
        header.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            header, text="📊  Tableau de bord",
            font=ctk.CTkFont(size=13, weight="bold"),
            text_color=SG_ANTHRACITE, anchor="w",
        ).grid(row=0, column=0, sticky="w")

        ctk.CTkLabel(
            header, textvariable=self.v_count_total,
            font=ctk.CTkFont(size=11),
            text_color=SG_GREY,
        ).grid(row=0, column=1, sticky="e")

        # Compteurs en gros caractères
        counters = ctk.CTkFrame(card, fg_color="transparent")
        counters.grid(row=1, column=0, sticky="ew", padx=CARD_PADDING)
        counters.grid_columnconfigure((0, 1, 2), weight=1, uniform="cnt")

        self._counter_box(counters, col=0,
                          icon="✅", label="OK",
                          var=self.v_count_ok, fg=C_OK, bg=C_OK_BG)
        self._counter_box(counters, col=1,
                          icon="⚠️", label="À vérifier",
                          var=self.v_count_warn, fg=C_WARN, bg=C_WARN_BG)
        self._counter_box(counters, col=2,
                          icon="❌", label="KO",
                          var=self.v_count_ko, fg=C_ERR, bg=C_ERR_BG)

        # Barre de progression
        self.progress = ctk.CTkProgressBar(
            card, variable=self.v_progress, height=10,
            corner_radius=6, progress_color=SG_RED,
        )
        self.progress.grid(row=2, column=0, sticky="ew",
                           padx=CARD_PADDING, pady=(18, 6))
        self.progress.set(0)

        # Status texte
        self.lbl_status_msg = ctk.CTkLabel(
            card, textvariable=self.v_status,
            font=ctk.CTkFont(size=11),
            text_color=SG_ANTHRACITE, anchor="w", justify="left",
            wraplength=900,
        )
        self.lbl_status_msg.grid(row=3, column=0, sticky="ew",
                                 padx=CARD_PADDING, pady=(4, CARD_PADDING))

    def _counter_box(self, parent, col: int, icon: str, label: str,
                     var: StringVar, fg: str, bg: str):
        box = ctk.CTkFrame(parent, fg_color=bg, corner_radius=12,
                           border_width=0)
        box.grid(row=0, column=col, sticky="nsew", padx=4, pady=4)
        box.grid_columnconfigure(0, weight=1)

        top = ctk.CTkFrame(box, fg_color="transparent")
        top.grid(row=0, column=0, sticky="ew", padx=14, pady=(14, 0))
        top.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(
            top, text=icon, font=ctk.CTkFont(size=18),
            text_color=fg,
        ).grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(
            top, text=label, font=ctk.CTkFont(size=11, weight="bold"),
            text_color=fg, anchor="e",
        ).grid(row=0, column=1, sticky="e")

        ctk.CTkLabel(
            box, textvariable=var,
            font=ctk.CTkFont(size=32, weight="bold"),
            text_color=fg,
        ).grid(row=1, column=0, sticky="w", padx=14, pady=(2, 14))

    def _build_action_card(self, card: BentoCard):
        card.grid_columnconfigure(0, weight=1)
        card.grid_columnconfigure(1, weight=0)
        card.grid_columnconfigure(2, weight=1)

        # Bouton principal centré
        self.btn_run = ctk.CTkButton(
            card, text="▶  LANCER LE RAPPROCHEMENT",
            height=52, width=400,
            fg_color=SG_RED, hover_color=SG_RED_HOVER, text_color=SG_WHITE,
            corner_radius=12,
            font=ctk.CTkFont(size=14, weight="bold"),
            command=self._start_matching,
        )
        self.btn_run.grid(row=0, column=1, padx=12, pady=CARD_PADDING)

        # Boutons secondaires (alignés à droite)
        side = ctk.CTkFrame(card, fg_color="transparent")
        side.grid(row=0, column=2, sticky="e", padx=CARD_PADDING, pady=CARD_PADDING)
        ctk.CTkButton(
            side, text="⚙️  Paramètres avancés",
            height=36, width=170,
            fg_color=SG_WHITE, hover_color=SG_GREY_LIGHT,
            text_color=SG_ANTHRACITE, border_color=SG_GREY_BORDER, border_width=1,
            corner_radius=10, font=ctk.CTkFont(size=11),
            command=self._open_advanced_settings,
        ).pack(side="left", padx=(0, 6))

        self.btn_open_result = ctk.CTkButton(
            side, text="📂  Ouvrir résultats",
            height=36, width=150,
            fg_color=SG_WHITE, hover_color=SG_GREY_LIGHT,
            text_color=SG_ANTHRACITE, border_color=SG_GREY_BORDER, border_width=1,
            corner_radius=10, font=ctk.CTkFont(size=11),
            command=self._open_result, state="disabled",
        )
        self.btn_open_result.pack(side="left")

    def _build_footer(self):
        # Zone logs (cachée par défaut)
        self.frame_logs = ctk.CTkFrame(self, fg_color=SG_WHITE,
                                       corner_radius=0, height=140)
        self.frame_logs.pack_propagate(False)
        self.txt_logs = ctk.CTkTextbox(
            self.frame_logs, fg_color=SG_GREY_LIGHT,
            text_color=SG_ANTHRACITE, corner_radius=0,
            font=ctk.CTkFont(family="Courier", size=10),
            wrap="word",
        )
        self.txt_logs.pack(fill="both", expand=True, padx=24, pady=8)
        self.txt_logs.configure(state="disabled")

        # Footer
        footer = ctk.CTkFrame(self, fg_color=SG_WHITE,
                              corner_radius=0, height=42)
        footer.pack(fill="x", side="bottom")
        footer.pack_propagate(False)

        # Liseré rouge fin au-dessus du footer
        ctk.CTkFrame(self, fg_color=SG_GREY_BORDER, height=1, corner_radius=0
                     ).pack(fill="x", side="bottom")

        ctk.CTkLabel(
            footer,
            text="⚠  Outil d'aide à la décision — toute correspondance « À vérifier » "
                 "ou « KO » nécessite une validation manuelle.",
            font=ctk.CTkFont(size=10),
            text_color=SG_GREY, anchor="w",
        ).pack(side="left", padx=24)

        self.btn_logs = ctk.CTkButton(
            footer, text="▾  Logs", width=70, height=24,
            fg_color="transparent", hover_color=SG_GREY_LIGHT,
            text_color=SG_GREY, border_width=0,
            corner_radius=6, font=ctk.CTkFont(size=10),
            command=self._toggle_logs,
        )
        self.btn_logs.pack(side="right", padx=18)

    def _toggle_logs(self):
        if self._logs_visible:
            self.frame_logs.pack_forget()
            self.btn_logs.configure(text="▾  Logs")
            self._logs_visible = False
        else:
            # side="bottom" empile au-dessus des éléments déjà en bas (footer)
            self.frame_logs.pack(fill="x", side="bottom")
            self.btn_logs.configure(text="▴  Logs")
            self._logs_visible = True

    def _append_log(self, msg: str):
        try:
            self.txt_logs.configure(state="normal")
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            self.txt_logs.insert("end", f"[{ts}] {msg}\n")
            self.txt_logs.see("end")
            self.txt_logs.configure(state="disabled")
        except Exception:
            pass

    # ── Champ fichier réutilisable ──────────────────────────────────────────

    def _file_field(self, parent, row: int, label: str, var: StringVar,
                    save: bool, filetypes):
        ctk.CTkLabel(
            parent, text=label, font=ctk.CTkFont(size=11),
            text_color=SG_GREY, anchor="w",
        ).grid(row=row, column=0, sticky="w",
               padx=CARD_PADDING, pady=(4, 2))

        rowf = ctk.CTkFrame(parent, fg_color="transparent")
        rowf.grid(row=row + 1, column=0, sticky="ew",
                  padx=CARD_PADDING, pady=(0, 6))
        rowf.grid_columnconfigure(0, weight=1)
        ctk.CTkEntry(
            rowf, textvariable=var, height=34,
            border_color=SG_GREY_BORDER, border_width=1,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ctk.CTkButton(
            rowf, text="Parcourir", width=90, height=34,
            fg_color=SG_ANTHRACITE, hover_color=SG_BLACK, text_color=SG_WHITE,
            corner_radius=8, font=ctk.CTkFont(size=11),
            command=lambda: _browse_file(var, label, filetypes, save=save),
        ).grid(row=0, column=1)

    # ── Statut & validation base GLEIF (incassable) ─────────────────────────

    def _refresh_data_validity(self):
        """
        Vérifie la présence physique du fichier GLEIF référencé dans les prefs.
        Si absent : tentative de fallback (.db → .csv frère), sinon désactive
        le bouton Lancer et bascule en état "Aucune base".
        Appelée au démarrage et à chaque changement de v_gleif.
        """
        path = (self.v_gleif.get() or "").strip()

        if not path:
            self._set_no_base_state("Aucune base sélectionnée. Cliquez sur 🔄 pour télécharger.")
            return

        if not os.path.exists(path):
            # Cache .db disparu → tentative de fallback automatique sur le CSV
            if path.lower().endswith(".db"):
                parent = Path(path).parent
                fallback = next(
                    (p for p in (
                        parent / "gleif_golden_copy.csv",
                        parent / "gleif_slim.csv",
                    ) if p.exists()),
                    None,
                )
                if fallback is not None:
                    if messagebox.askyesno(
                        "Cache SQLite manquant",
                        f"Le cache « {Path(path).name} » référencé dans vos préférences "
                        f"est introuvable.\n\n"
                        f"Un fichier source est disponible :\n   {fallback.name}\n\n"
                        f"Voulez-vous basculer dessus (mode standard, plus lent) ?\n\n"
                        f"Vous pourrez régénérer le cache via 🔄 « Mettre à jour »."
                    ):
                        self.v_gleif.set(str(fallback))
                        return  # le trace re-déclenchera _refresh_data_validity()

            # Aucun fallback possible → réinitialisation
            self._set_no_base_state(
                f"⚠️ Base non trouvée — Mise à jour requise\n"
                f"   Chemin précédent : {path}"
            )
            # On vide la préférence pour ne pas re-prompter à chaque sortie
            self.v_gleif.set("")
            self._save_prefs()
            return

        # ── Fichier présent : état OK ────────────────────────────────────────
        self._set_base_ok_state()

    def _set_no_base_state(self, reason: str):
        """Désactive le bouton Lancer et affiche un message en rouge."""
        self.v_status.set(reason)
        try:
            self.lbl_status_msg.configure(text_color=C_ERR)
        except Exception:
            pass
        try:
            self.btn_run.configure(
                state="disabled", text="⚠️  Base GLEIF requise",
                fg_color=SG_GREY, hover_color=SG_GREY,
            )
        except AttributeError:
            pass  # btn_run pas encore créé pendant l'init
        try:
            self.lbl_status_dot.configure(text_color=C_ERR)
            self.lbl_status_text.configure(text="Aucune base GLEIF")
            self.lbl_gleif_age.configure(text="Base introuvable", text_color=C_ERR)
        except (AttributeError, Exception):
            pass

    def _set_base_ok_state(self):
        """Réactive le bouton Lancer et affiche le statut de fraîcheur."""
        color, label = _file_age_status(self.v_gleif.get())
        try:
            self.lbl_status_dot.configure(text_color=color)
            self.lbl_status_text.configure(text=label)
            self.lbl_gleif_age.configure(text=label, text_color=color)
        except (AttributeError, Exception):
            pass
        try:
            # Ne ré-active que si l'utilisateur n'a pas déclenché un run
            if self.btn_run.cget("text") in ("⚠️  Base GLEIF requise", "▶  LANCER LE RAPPROCHEMENT"):
                self.btn_run.configure(
                    state="normal", text="▶  LANCER LE RAPPROCHEMENT",
                    fg_color=SG_RED, hover_color=SG_RED_HOVER,
                )
            self.lbl_status_msg.configure(text_color=SG_ANTHRACITE)
            if "Base introuvable" in self.v_status.get() or "Aucune base" in self.v_status.get() \
                    or "Base non trouvée" in self.v_status.get():
                self.v_status.set("Prêt — sélectionnez vos fichiers et lancez le rapprochement.")
        except (AttributeError, Exception):
            pass

    def _maybe_show_welcome(self):
        """
        Affiche un message de bienvenue lors du tout premier démarrage
        (aucune base, aucune préférence). Une seule fois par session.
        """
        if self._welcome_shown:
            return
        path = (self.v_gleif.get() or "").strip()
        if path and os.path.exists(path):
            return  # base OK, pas besoin de bienvenue

        self._welcome_shown = True
        messagebox.showinfo(
            "Bienvenue dans LEI Matcher",
            "👋  Bienvenue dans LEI Matcher — Société Générale Middle Office.\n\n"
            "Aucune base GLEIF n'est encore configurée sur ce poste.\n\n"
            "Pour commencer, cliquez sur le bouton 🔄  « Mettre à jour la base GLEIF » "
            "afin de télécharger la base mondiale (≈ 450 Mo) et de générer "
            "automatiquement le cache SQLite (mode Turbo, recommandé).\n\n"
            "Cette opération n'est nécessaire qu'une seule fois — les mises à "
            "jour ultérieures sont incrémentales."
        )

    # ── Actions ─────────────────────────────────────────────────────────────

    def _check_python_arch(self):
        if sys.maxsize <= 2**31:
            messagebox.showwarning(
                "Python 32 bits détecté",
                "Vous utilisez Python en 32 bits.\n\n"
                "Le chargement du GLEIF (~450 Mo) risque de provoquer une "
                "erreur mémoire.\n\nActivez la base SLIM dans Paramètres avancés "
                "ou installez Python 64 bits.",
            )

    def _validate(self) -> bool:
        errors = []
        if not self.v_input.get().strip():
            errors.append("Fichier sociétés non sélectionné")
        elif not Path(self.v_input.get()).exists():
            errors.append("Fichier sociétés introuvable")
        if not self.v_gleif.get().strip():
            errors.append("Base GLEIF non sélectionnée")
        elif not Path(self.v_gleif.get()).exists():
            errors.append("Base GLEIF introuvable")
        if not self.v_output.get().strip():
            errors.append("Fichier de sortie non défini")
        if errors:
            messagebox.showerror(
                "Champs manquants",
                "Action requise — corrigez les points suivants :\n\n• " +
                "\n• ".join(errors),
            )
            return False
        return True

    def _start_matching(self):
        if not self._validate():
            return
        if _is_onedrive_path(self.v_input.get()):
            if not messagebox.askyesno(
                "Fichier OneDrive détecté",
                "Le fichier sociétés est dans OneDrive Entreprise.\n\n"
                "Une copie temporaire sera créée si nécessaire.\n\nContinuer ?",
            ):
                return

        # Reset des compteurs
        self.v_count_total.set("Initialisation…")
        self.v_count_ok.set("0")
        self.v_count_warn.set("0")
        self.v_count_ko.set("0")
        self.v_progress.set(0)
        self.v_status.set("Démarrage du rapprochement…")
        self.btn_run.configure(state="disabled", text="⏳  Traitement en cours…")
        self.btn_open_result.configure(state="disabled")
        self._append_log("=" * 50)
        self._append_log("Lancement du rapprochement")

        threading.Thread(target=self._run_matching, daemon=True).start()

    def _run_matching(self):
        try:
            from gleif_matcher import match_companies

            col_lei    = self.v_col_lei.get().strip()    or None
            col_date   = self.v_col_date.get().strip()   or None
            col_postal = self.v_col_postal.get().strip() or None

            _, stats = match_companies(
                input_path  = self.v_input.get(),
                gleif_path  = self.v_gleif.get(),
                output_path = self.v_output.get(),
                col_rcs     = self.v_col_rcs.get().strip(),
                col_name    = self.v_col_name.get().strip(),
                col_pays    = self.v_col_pays.get().strip(),
                col_lei     = col_lei,
                col_date    = col_date,
                col_postal  = col_postal,
                fuzzy_threshold     = int(self.v_threshold.get()),
                rcs_fuzzy_threshold = int(self.v_rcs_threshold.get()),
                active_only         = bool(self.v_active.get()),
                progress_cb = self._on_progress,
                status_cb   = self._on_status,
            )
            self.after(0, lambda: self._on_finished(stats))

        except Exception as exc:
            friendly = _translate_error(exc)
            self.after(0, lambda: self._on_error(friendly))

    def _on_progress(self, done: int, total: int):
        if total <= 0:
            return
        pct = min(done / total, 1.0)
        def _u():
            self.v_progress.set(pct)
            self.v_count_total.set(f"{done:,} / {total:,} lignes")
            self.v_status.set(f"Rapprochement en cours… ({pct*100:.0f} %)")
        self.after(0, _u)

    def _on_status(self, msg: str):
        self.after(0, lambda: (self.v_status.set(msg), self._append_log(msg)))

    def _on_finished(self, stats: dict):
        total = stats.get("total", 0)
        ok    = stats.get("ok", 0)
        warn  = stats.get("a_verifier", 0)
        ko    = stats.get("ko", 0)
        pct_ok = (ok / total * 100) if total else 0

        self.v_count_total.set(f"Total : {total:,}")
        self.v_count_ok.set(f"{ok:,}")
        self.v_count_warn.set(f"{warn:,}")
        self.v_count_ko.set(f"{ko:,}")
        self.v_progress.set(1.0)
        self.v_status.set(
            f"✅  Traitement terminé — {pct_ok:.0f} % OK, "
            f"{warn:,} à vérifier, {ko:,} KO. Fichier généré : "
            f"{Path(self.v_output.get()).name}"
        )
        self.btn_run.configure(state="normal", text="▶  LANCER LE RAPPROCHEMENT")
        self.btn_open_result.configure(state="normal")
        self._save_prefs()
        self._append_log(
            f"Stats : OK={ok}  À vérifier={warn}  KO={ko}  "
            f"(Exact RCS={stats.get('exact_rcs', 0)}, "
            f"Approx RCS={stats.get('approx_rcs', 0)}, "
            f"Approx Nom={stats.get('approx_nom', 0)}, "
            f"LEI Valide={stats.get('lei_valide', 0)}, "
            f"LEI Discordant={stats.get('lei_discordant', 0)})"
        )

        # Avertissement modal si la qualité du lot est dégradée
        if total and (warn + ko) / total >= 0.3:
            self.after(300, lambda: messagebox.showwarning(
                "Vérification manuelle nécessaire",
                f"⚠  {warn + ko:,} ligne(s) sur {total:,} "
                f"({(warn + ko) / total * 100:.0f} %) sont marquées « À vérifier » "
                f"ou « KO ».\n\nConsultez la colonne ActionRequise dans le "
                f"fichier de sortie pour le détail.",
            ))

    def _on_error(self, msg: str):
        messagebox.showerror("Erreur", msg)
        self.btn_run.configure(state="normal", text="▶  LANCER LE RAPPROCHEMENT")
        self.v_status.set("❌  Erreur — voir le détail ci-dessus.")
        self._append_log(f"ERREUR : {msg}")

    def _open_result(self):
        path = self.v_output.get()
        if path and Path(path).exists():
            if sys.platform == "win32":
                os.startfile(path)
            elif sys.platform == "darwin":
                subprocess.run(["open", path])
            else:
                subprocess.run(["xdg-open", path])
        else:
            messagebox.showwarning("Introuvable", "Le fichier de sortie n'existe pas encore.")

    def _open_update_dialog(self):
        UpdateDialog(self, self.v_gleif, self.v_proxy)

    def _open_advanced_settings(self):
        AdvancedSettingsDialog(self)

    # ── Persistance ─────────────────────────────────────────────────────────

    def _save_prefs(self):
        save_user_prefs({
            "last_input":  self.v_input.get(),
            "gleif_path":  self.v_gleif.get(),
            "last_output": self.v_output.get(),
            "col_rcs":     self.v_col_rcs.get(),
            "col_name":    self.v_col_name.get(),
            "col_pays":    self.v_col_pays.get(),
            "col_lei":     self.v_col_lei.get(),
            "col_date":    self.v_col_date.get(),
            "col_postal":  self.v_col_postal.get(),
            "fuzzy_threshold":     int(self.v_threshold.get()),
            "rcs_fuzzy_threshold": int(self.v_rcs_threshold.get()),
            "active_only": bool(self.v_active.get()),
            "use_slim":    bool(self.v_use_slim.get()),
            "proxy":       self.v_proxy.get(),
        })

    def _on_close(self):
        self._save_prefs()
        self.destroy()

    def _center_window(self, w, h):
        self.update_idletasks()
        sw, sh = self.winfo_screenwidth(), self.winfo_screenheight()
        self.geometry(f"{w}x{h}+{(sw-w)//2}+{max(0, (sh-h)//2 - 30)}")


# ─────────────────────────────────────────────────────────────────────────────
# Modale Paramètres avancés
# ─────────────────────────────────────────────────────────────────────────────

class AdvancedSettingsDialog(ctk.CTkToplevel):

    def __init__(self, parent: GleifApp):
        super().__init__(parent)
        self.parent = parent
        self.title("Paramètres avancés")
        self.configure(fg_color=SG_GREY_LIGHT)
        self.resizable(False, False)
        self.transient(parent)
        self.after(50, self.grab_set)
        self._build_ui()
        self._center(parent, 620, 600)

    def _build_ui(self):
        # Header
        hdr = ctk.CTkFrame(self, fg_color=SG_WHITE, corner_radius=0, height=52)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(
            hdr, text="⚙️  Paramètres avancés",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color=SG_ANTHRACITE,
        ).pack(side="left", padx=20, pady=14)
        ctk.CTkFrame(self, fg_color=SG_RED, height=2, corner_radius=0
                     ).pack(fill="x")

        # Cartes empilées
        body = ctk.CTkScrollableFrame(self, fg_color=SG_GREY_LIGHT,
                                      corner_radius=0)
        body.pack(fill="both", expand=True, padx=20, pady=14)

        # ── Carte Colonnes ────────────────────────────────────────────────
        c1 = BentoCard(body)
        c1.pack(fill="x", pady=(0, 12))
        ctk.CTkLabel(
            c1, text="📋  Noms des colonnes (fichier source)",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color=SG_ANTHRACITE, anchor="w",
        ).pack(anchor="w", padx=CARD_PADDING, pady=(CARD_PADDING, 8))

        for label, var, hint in [
            ("RCS",            self.parent.v_col_rcs,    "obligatoire"),
            ("Nom entreprise", self.parent.v_col_name,   "obligatoire"),
            ("Pays",           self.parent.v_col_pays,   "obligatoire"),
            ("LEI existant",   self.parent.v_col_lei,    "optionnel — active la validation"),
            ("Date validité",  self.parent.v_col_date,   "optionnel — format dd-mm-yyyy"),
            ("Code Postal",    self.parent.v_col_postal, "optionnel — affine le matching"),
        ]:
            self._labeled_entry(c1, label, var, hint)
        ctk.CTkFrame(c1, fg_color="transparent", height=10).pack()

        # ── Carte Seuils ───────────────────────────────────────────────────
        c2 = BentoCard(body)
        c2.pack(fill="x", pady=(0, 12))
        ctk.CTkLabel(
            c2, text="🎯  Seuils de correspondance",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color=SG_ANTHRACITE, anchor="w",
        ).pack(anchor="w", padx=CARD_PADDING, pady=(CARD_PADDING, 8))

        self._slider_row(c2, "Seuil similarité nom/pays",
                         self.parent.v_threshold, 70, 100)
        self._slider_row(c2, "Seuil RCS approché",
                         self.parent.v_rcs_threshold, 70, 100)
        ctk.CTkFrame(c2, fg_color="transparent", height=10).pack()

        # ── Carte Options ──────────────────────────────────────────────────
        c3 = BentoCard(body)
        c3.pack(fill="x", pady=(0, 12))
        ctk.CTkLabel(
            c3, text="⚡  Options",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color=SG_ANTHRACITE, anchor="w",
        ).pack(anchor="w", padx=CARD_PADDING, pady=(CARD_PADDING, 8))
        ctk.CTkSwitch(
            c3, text="Utiliser la base slim (plus rapide, moins de mémoire)",
            variable=self.parent.v_use_slim,
            progress_color=SG_RED, button_color=SG_WHITE,
            font=ctk.CTkFont(size=11),
        ).pack(anchor="w", padx=CARD_PADDING, pady=4)
        ctk.CTkSwitch(
            c3, text="LEI actifs uniquement (Entity=ACTIVE & LEI=ISSUED)",
            variable=self.parent.v_active,
            progress_color=SG_RED, button_color=SG_WHITE,
            font=ctk.CTkFont(size=11),
        ).pack(anchor="w", padx=CARD_PADDING, pady=4)
        ctk.CTkFrame(c3, fg_color="transparent", height=10).pack()

        # ── Carte Proxy ────────────────────────────────────────────────────
        c4 = BentoCard(body)
        c4.pack(fill="x", pady=(0, 12))
        ctk.CTkLabel(
            c4, text="🌐  Proxy réseau",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color=SG_ANTHRACITE, anchor="w",
        ).pack(anchor="w", padx=CARD_PADDING, pady=(CARD_PADDING, 8))
        ctk.CTkLabel(
            c4, text="Laissez vide pour utiliser le proxy système (recommandé).",
            font=ctk.CTkFont(size=10),
            text_color=SG_GREY, anchor="w",
        ).pack(anchor="w", padx=CARD_PADDING, pady=(0, 4))
        ctk.CTkEntry(
            c4, textvariable=self.parent.v_proxy, height=32,
            border_color=SG_GREY_BORDER, border_width=1,
        ).pack(fill="x", padx=CARD_PADDING, pady=(0, CARD_PADDING))

        # Bouton fermer
        bottom = ctk.CTkFrame(self, fg_color=SG_WHITE, corner_radius=0,
                              height=52)
        bottom.pack(fill="x", side="bottom")
        bottom.pack_propagate(False)
        ctk.CTkButton(
            bottom, text="Fermer", width=120, height=34,
            fg_color=SG_RED, hover_color=SG_RED_HOVER, text_color=SG_WHITE,
            corner_radius=8, font=ctk.CTkFont(size=11, weight="bold"),
            command=self.destroy,
        ).pack(side="right", padx=20, pady=10)

    def _labeled_entry(self, parent, label: str, var: StringVar, hint: str):
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill="x", padx=CARD_PADDING, pady=3)
        ctk.CTkLabel(
            row, text=label, font=ctk.CTkFont(size=11),
            text_color=SG_ANTHRACITE, width=130, anchor="w",
        ).pack(side="left")
        ctk.CTkEntry(
            row, textvariable=var, width=180, height=28,
            border_color=SG_GREY_BORDER, border_width=1,
            font=ctk.CTkFont(size=11),
        ).pack(side="left", padx=8)
        ctk.CTkLabel(
            row, text=hint, font=ctk.CTkFont(size=10, slant="italic"),
            text_color=SG_GREY,
        ).pack(side="left")

    def _slider_row(self, parent, label: str, var: IntVar, mn: int, mx: int):
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill="x", padx=CARD_PADDING, pady=6)
        ctk.CTkLabel(
            row, text=label, font=ctk.CTkFont(size=11),
            text_color=SG_ANTHRACITE, anchor="w", width=200,
        ).pack(side="left")
        val_lbl = ctk.CTkLabel(
            row, text=f"{var.get()} %",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color=SG_RED, width=50,
        )
        val_lbl.pack(side="right")
        slider = ctk.CTkSlider(
            row, from_=mn, to=mx, variable=var, width=260,
            progress_color=SG_RED, button_color=SG_RED,
            button_hover_color=SG_RED_HOVER,
            command=lambda v: val_lbl.configure(text=f"{int(float(v))} %"),
        )
        slider.pack(side="left", padx=8)

    def _center(self, parent, w, h):
        parent.update_idletasks()
        px, py = parent.winfo_rootx(), parent.winfo_rooty()
        pw, ph = parent.winfo_width(), parent.winfo_height()
        self.geometry(f"{w}x{h}+{px+(pw-w)//2}+{py+(ph-h)//2}")


# ─────────────────────────────────────────────────────────────────────────────
# Modale Mise à jour GLEIF
# ─────────────────────────────────────────────────────────────────────────────

class UpdateDialog(ctk.CTkToplevel):

    def __init__(self, parent: GleifApp, v_gleif_path: StringVar, v_proxy: StringVar):
        super().__init__(parent)
        self.parent = parent
        self.title("Mise à jour de la base GLEIF")
        self.configure(fg_color=SG_GREY_LIGHT)
        self.resizable(False, False)
        self.transient(parent)
        self.after(50, self.grab_set)

        self.v_gleif_path    = v_gleif_path
        self.v_proxy         = v_proxy
        self.v_progress      = DoubleVar(value=0)
        self.v_slim_progress = DoubleVar(value=0)
        self.v_status        = StringVar(value="Prêt à vérifier la version disponible.")
        self.v_prepare_slim  = BooleanVar(value=False)
        self.v_prepare_cache = BooleanVar(value=True)  # ⚡ activé par défaut v2.2
        self._running = False
        self._meta    = None

        self._build_ui()
        self._center(parent, 600, 540)

    def _build_ui(self):
        # Header
        hdr = ctk.CTkFrame(self, fg_color=SG_WHITE, corner_radius=0, height=52)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(
            hdr, text="🔄  Mise à jour de la base GLEIF",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color=SG_ANTHRACITE,
        ).pack(side="left", padx=20, pady=14)
        ctk.CTkFrame(self, fg_color=SG_RED, height=2, corner_radius=0
                     ).pack(fill="x")

        body = ctk.CTkFrame(self, fg_color=SG_GREY_LIGHT)
        body.pack(fill="both", expand=True, padx=20, pady=14)

        card = BentoCard(body)
        card.pack(fill="both", expand=True)

        # Info
        ctk.CTkLabel(
            card,
            text="Le fichier Golden Copy GLEIF est mis à jour quotidiennement.\n"
                 "Vérification ↓ comparaison locale vs serveurs GLEIF (~450 Mo).",
            font=ctk.CTkFont(size=10),
            text_color=SG_GREY, anchor="w", justify="left",
        ).pack(anchor="w", padx=CARD_PADDING, pady=(CARD_PADDING, 10))

        # Dossier
        ctk.CTkLabel(
            card, text="Dossier de destination",
            font=ctk.CTkFont(size=10), text_color=SG_GREY, anchor="w",
        ).pack(anchor="w", padx=CARD_PADDING)
        row = ctk.CTkFrame(card, fg_color="transparent")
        row.pack(fill="x", padx=CARD_PADDING, pady=(2, 8))
        row.grid_columnconfigure(0, weight=1)
        ctk.CTkEntry(
            row, textvariable=self.v_gleif_path, height=32,
            border_color=SG_GREY_BORDER, border_width=1,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ctk.CTkButton(
            row, text="…", width=40, height=32,
            fg_color=SG_ANTHRACITE, hover_color=SG_BLACK,
            corner_radius=6,
            command=self._browse_dest,
        ).grid(row=0, column=1)

        # Proxy
        ctk.CTkLabel(
            card, text="Proxy HTTP(S)",
            font=ctk.CTkFont(size=10), text_color=SG_GREY, anchor="w",
        ).pack(anchor="w", padx=CARD_PADDING)
        ctk.CTkEntry(
            card, textvariable=self.v_proxy, height=32,
            border_color=SG_GREY_BORDER, border_width=1,
        ).pack(fill="x", padx=CARD_PADDING, pady=(2, 6))

        # Avertissement proxy
        warn = ctk.CTkFrame(card, fg_color=C_WARN_BG, corner_radius=8)
        warn.pack(fill="x", padx=CARD_PADDING, pady=(0, 10))
        ctk.CTkLabel(
            warn,
            text="ℹ  Si erreur HTTP 407 : videz le champ Proxy.\n"
                 "L'authentification NTLM/Kerberos est gérée automatiquement.",
            font=ctk.CTkFont(size=10),
            text_color="#92400E", anchor="w", justify="left",
        ).pack(anchor="w", padx=10, pady=8)

        # Switches post-téléchargement
        ctk.CTkSwitch(
            card,
            text="⚡  Préparer le cache SQLite (recommandé — speedup ×10)",
            variable=self.v_prepare_cache,
            progress_color=SG_RED, font=ctk.CTkFont(size=11),
        ).pack(anchor="w", padx=CARD_PADDING, pady=(0, 4))

        ctk.CTkSwitch(
            card,
            text="Préparer la base slim CSV (legacy, optionnel)",
            variable=self.v_prepare_slim,
            progress_color=SG_RED, font=ctk.CTkFont(size=11),
        ).pack(anchor="w", padx=CARD_PADDING, pady=(0, 10))

        # Progressions
        self.progress_bar = ctk.CTkProgressBar(
            card, variable=self.v_progress, height=8,
            corner_radius=4, progress_color=SG_RED,
        )
        self.progress_bar.pack(fill="x", padx=CARD_PADDING, pady=(0, 4))
        self.progress_bar.set(0)
        self.slim_bar = ctk.CTkProgressBar(
            card, variable=self.v_slim_progress, height=8,
            corner_radius=4, progress_color=SG_RED,
        )
        self.slim_bar.pack(fill="x", padx=CARD_PADDING, pady=(0, 4))
        self.slim_bar.set(0)

        self.lbl_status = ctk.CTkLabel(
            card, textvariable=self.v_status,
            font=ctk.CTkFont(size=11),
            text_color=SG_ANTHRACITE, anchor="w", justify="left",
            wraplength=520,
        )
        self.lbl_status.pack(fill="x", padx=CARD_PADDING, pady=(2, CARD_PADDING))

        # Boutons
        btns = ctk.CTkFrame(self, fg_color=SG_WHITE, corner_radius=0, height=52)
        btns.pack(fill="x", side="bottom")
        btns.pack_propagate(False)

        self.btn_check = ctk.CTkButton(
            btns, text="Vérifier la version", height=34, width=160,
            fg_color=SG_RED, hover_color=SG_RED_HOVER, text_color=SG_WHITE,
            corner_radius=8, font=ctk.CTkFont(size=11, weight="bold"),
            command=self._start_check,
        )
        self.btn_check.pack(side="left", padx=(20, 8), pady=10)

        self.btn_download = ctk.CTkButton(
            btns, text="Télécharger maintenant", height=34, width=180,
            fg_color=SG_WHITE, hover_color=SG_GREY_LIGHT,
            text_color=SG_ANTHRACITE,
            border_color=SG_GREY_BORDER, border_width=1,
            corner_radius=8, font=ctk.CTkFont(size=11),
            state="disabled", command=self._start_download,
        )
        self.btn_download.pack(side="left", padx=8, pady=10)

        ctk.CTkButton(
            btns, text="Fermer", height=34, width=90,
            fg_color="transparent", hover_color=SG_GREY_LIGHT,
            text_color=SG_GREY, border_width=0,
            corner_radius=8, font=ctk.CTkFont(size=11),
            command=self.destroy,
        ).pack(side="right", padx=20, pady=10)

    def _browse_dest(self):
        path = filedialog.askopenfilename(
            title="Choisir le fichier GLEIF (ou son dossier)",
            filetypes=[("CSV", "*.csv"), ("Tous", "*.*")])
        if path:
            self.v_gleif_path.set(path)

    def _set_status(self, msg: str, color: str = SG_ANTHRACITE):
        def _u():
            self.v_status.set(msg)
            self.lbl_status.configure(text_color=color)
        self.after(0, _u)

    def _set_dl_progress(self, done, total):
        if total > 0:
            pct = min(done / total, 1.0)
            mb_d, mb_t = done / 1_048_576, total / 1_048_576
            def _u():
                self.v_progress.set(pct)
                self.v_status.set(
                    f"Téléchargement : {mb_d:.1f} Mo / {mb_t:.1f} Mo  ({pct*100:.0f} %)"
                )
            self.after(0, _u)

    def _set_slim_progress(self, done, total):
        if total > 0:
            self.after(0, lambda: self.v_slim_progress.set(min(done / total, 1.0)))

    def _start_check(self):
        if self._running:
            return
        self._running = True
        self.btn_check.configure(state="disabled")
        self.btn_download.configure(state="disabled")
        self.v_progress.set(0)
        self._set_status("Connexion aux serveurs GLEIF…")
        threading.Thread(target=self._do_check, daemon=True).start()

    def _do_check(self):
        from gleif_updater import fetch_latest_metadata, read_local_version, is_update_available
        try:
            proxy = self.v_proxy.get().strip() or None
            meta  = fetch_latest_metadata(proxy=proxy)
            self._meta = meta
            gleif_path = self.v_gleif_path.get().strip()

            # Détermination du dossier de destination — robuste si rien n'est
            # configuré (premier démarrage)
            if gleif_path and Path(gleif_path).exists():
                dest_dir = Path(gleif_path).parent
            elif gleif_path:
                # Chemin saisi mais inexistant : on prend le dossier parent
                # (utile si l'utilisateur vient juste de pointer vers un futur emplacement)
                dest_dir = Path(gleif_path).parent if Path(gleif_path).parent.exists() else Path.cwd()
            else:
                dest_dir = Path.cwd()

            # Lecture de la version locale (None si jamais téléchargé)
            local_date = read_local_version(dest_dir)
            local_label = local_date[:10] if local_date else "Aucune"

            remote_date = meta["publish_date"][:10]

            if local_date is None:
                # Premier téléchargement
                msg = (
                    f"📥  Premier téléchargement\n"
                    f"   Version locale  : Aucune\n"
                    f"   Version distante : {remote_date}\n"
                    f"   Taille           : {meta['size_human']}\n"
                    f"   Entités          : {meta['record_count']:,}"
                )
                self.after(0, lambda: self.btn_download.configure(state="normal"))
                self._set_status(msg, C_OK)
            elif is_update_available(local_date, meta["publish_date"]):
                msg = (
                    f"✓ Nouvelle version disponible\n"
                    f"   Version locale   : {local_label}\n"
                    f"   Version distante : {remote_date}\n"
                    f"   Taille           : {meta['size_human']}\n"
                    f"   Entités          : {meta['record_count']:,}"
                )
                self.after(0, lambda: self.btn_download.configure(state="normal"))
                self._set_status(msg, C_OK)
            else:
                self._set_status(
                    f"✓ Base à jour — version {remote_date} "
                    f"({meta['record_count']:,} entités).",
                    C_OK,
                )
        except Exception as e:
            self._set_status(_translate_error(e), C_ERR)
        finally:
            self._running = False
            self.after(0, lambda: self.btn_check.configure(state="normal"))

    def _start_download(self):
        if self._running or not self._meta:
            return
        self._running = True
        self.btn_download.configure(state="disabled")
        self.btn_check.configure(state="disabled")
        self.v_progress.set(0)
        self.v_slim_progress.set(0)
        self._set_status("Démarrage du téléchargement…")
        threading.Thread(target=self._do_download, daemon=True).start()

    def _do_download(self):
        from gleif_updater import download_gleif, extract_csv, write_local_version
        try:
            meta     = self._meta
            proxy    = self.v_proxy.get().strip() or None
            gstr     = self.v_gleif_path.get().strip()
            dest_dir = Path(gstr).parent if gstr else Path.cwd()

            zip_path = download_gleif(
                url=meta["download_url"], dest_dir=dest_dir,
                total_bytes=meta["size_bytes"],
                progress_cb=self._set_dl_progress, proxy=proxy,
            )

            self._set_status("Extraction du CSV…")
            self.after(0, lambda: self.progress_bar.configure(mode="indeterminate"))
            self.after(0, self.progress_bar.start)

            final_csv = extract_csv(zip_path, dest_dir)
            write_local_version(dest_dir, meta["publish_date"], final_csv.name)
            self.v_gleif_path.set(str(final_csv))

            self.after(0, lambda: [
                self.progress_bar.stop(),
                self.progress_bar.configure(mode="determinate"),
                self.v_progress.set(1.0),
            ])

            artefacts: list = [f"CSV : {final_csv.name}"]

            if self.v_prepare_slim.get():
                from gleif_matcher import prepare_slim
                slim_path = dest_dir / "gleif_slim.csv"
                self._set_status("Génération de la base slim…")
                prepare_slim(
                    str(final_csv), str(slim_path),
                    active_only=True,
                    progress_cb=self._set_slim_progress,
                    status_cb=lambda m: self._set_status(m),
                )
                artefacts.append(f"Slim : {slim_path.name}")

            if self.v_prepare_cache.get():
                from gleif_matcher import prepare_sqlite_cache
                cache_path = dest_dir / "gleif_cache.db"
                self.after(0, lambda: self.v_slim_progress.set(0))
                self._set_status("⚡ Construction du cache SQLite (≈ 30 s)…")
                prepare_sqlite_cache(
                    str(final_csv), str(cache_path),
                    active_only=True,
                    progress_cb=self._set_slim_progress,
                    status_cb=lambda m: self._set_status(m),
                )
                self.after(0, lambda: self.v_slim_progress.set(1.0))
                # Pointe automatiquement la GUI sur le cache (mode turbo)
                self.v_gleif_path.set(str(cache_path))
                artefacts.append(f"Cache ⚡ : {cache_path.name}")

            self._set_status(
                f"✓ Mise à jour terminée — {meta['publish_date'][:10]}\n"
                f"   " + "  •  ".join(artefacts),
                C_OK,
            )

        except Exception as e:
            self.after(0, lambda: [
                self.progress_bar.stop(),
                self.progress_bar.configure(mode="determinate"),
            ])
            self._set_status(_translate_error(e), C_ERR)
        finally:
            self._running = False
            self.after(0, lambda: self.btn_check.configure(state="normal"))

    def _center(self, parent, w, h):
        parent.update_idletasks()
        px, py = parent.winfo_rootx(), parent.winfo_rooty()
        pw, ph = parent.winfo_width(), parent.winfo_height()
        self.geometry(f"{w}x{h}+{px+(pw-w)//2}+{py+(ph-h)//2}")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = GleifApp()
    app.mainloop()
