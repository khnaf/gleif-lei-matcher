"""
gleif_gui.py
============
Interface graphique GLEIF LEI Matcher v2.0 — Société Générale Middle Office Edition.

Refonte UX :
  • Charte visuelle SG (rouge #E60028 / noir / gris).
  • Mode Simple par défaut — Options avancées masquables.
  • Messages d'erreur explicites (action requise, pas de jargon technique).
  • Bandeau disclaimer permanent ("aide à la décision").
  • Avertissement fort pour les correspondances non-OK.

Dépendances : pandas openpyxl rapidfuzz pillow (logo optionnel)
"""

import os
import sys
import json
import threading
import subprocess
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

TEAM_CONFIG_PATH = BASE_DIR / "gleif_config.json"
USER_PREFS_PATH  = Path.home() / ".gleif_matcher_prefs.json"
LOGO_PATH        = BASE_DIR / "assets" / "logo_sg.png"

# ─────────────────────────────────────────────────────────────────────────────
# Charte Société Générale
# ─────────────────────────────────────────────────────────────────────────────
SG_RED        = "#E60028"
SG_RED_DARK   = "#B3001F"
SG_BLACK      = "#000000"
SG_GREY_DARK  = "#333333"
SG_GREY       = "#6B7280"
SG_GREY_LIGHT = "#F5F5F5"
SG_WHITE      = "#FFFFFF"
SG_BORDER     = "#D1D5DB"

# Sémantique fiabilité
C_OK_FG        = "#1E7E34"
C_OK_BG        = "#D4EDDA"
C_VERIF_FG     = "#856404"
C_VERIF_BG     = "#FFF3CD"
C_KO_FG        = "#842029"
C_KO_BG        = "#F8D7DA"

# Compat (utilisé par UpdateDialog)
C_BG       = SG_GREY_LIGHT
C_PANEL    = SG_WHITE
C_ACCENT   = SG_RED
C_ACCENT2  = SG_RED_DARK
C_TEXT     = SG_BLACK
C_SUBTLE   = SG_GREY
C_GREEN    = C_OK_FG
C_RED      = C_KO_FG
C_WARN_BG  = C_VERIF_BG
C_WARN_FG  = C_VERIF_FG


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
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

def _browse_file(var, title, filetypes, save=False):
    if save:
        path = filedialog.asksaveasfilename(title=title, filetypes=filetypes, defaultextension=".xlsx")
    else:
        path = filedialog.askopenfilename(title=title, filetypes=filetypes)
    if path:
        var.set(path)


def _is_onedrive_path(path: str) -> bool:
    return "onedrive" in path.lower()


def _translate_error(exc: BaseException) -> str:
    """
    Traduit une exception technique en message d'action métier.
    """
    msg = str(exc)
    low = msg.lower()
    name = type(exc).__name__

    if "407" in msg or "proxy authentication" in low:
        return ("Action requise : authentification proxy refusée.\n\n"
                "• Videz le champ Proxy dans Options avancées (l'authentification "
                "NTLM/Kerberos est gérée par Windows).\n"
                "• Si le problème persiste, contactez votre support IT.")
    if "errno 13" in low or "permission denied" in low or name == "PermissionError":
        return ("Action requise : fichier verrouillé.\n\n"
                "• Fermez le fichier dans Excel s'il est ouvert.\n"
                "• Si c'est un fichier OneDrive, attendez la fin de la synchronisation "
                "ou copiez-le sur votre disque local.")
    if name == "FileNotFoundError" or "no such file" in low:
        return f"Action requise : fichier introuvable.\n\nVérifiez le chemin : {msg}"
    if name == "MemoryError" or "memory" in low and "error" in low:
        return ("Action requise : mémoire insuffisante.\n\n"
                "• Activez 'Utiliser la base slim' dans Options avancées.\n"
                "• Ou utilisez Python 64 bits si vous êtes en 32 bits.")
    if "11001" in msg or "getaddrinfo" in low or "name or service" in low:
        return ("Action requise : impossible de joindre le serveur.\n\n"
                "Vérifiez votre connexion réseau (VPN éventuel).")
    if "timeout" in low or "timed out" in low:
        return ("Action requise : délai d'attente dépassé.\n\n"
                "Le serveur GLEIF ne répond pas. Réessayez dans quelques minutes.")
    if "ssl" in low or "certificate" in low:
        return ("Action requise : problème de certificat SSL.\n\n"
                "Cela arrive parfois en environnement corporate. "
                "Contactez votre support IT.")
    if "colonnes manquantes" in low or "colonnes introuvables" in low:
        return f"Action requise : colonnes du fichier source incorrectes.\n\n{msg}"
    return f"Erreur inattendue ({name}) :\n\n{msg}"


# ─────────────────────────────────────────────────────────────────────────────
# Application principale
# ─────────────────────────────────────────────────────────────────────────────

class GleifApp(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("GLEIF LEI Matcher — Société Générale")
        self.resizable(False, False)
        self.configure(bg=SG_GREY_LIGHT)
        self._center_window(820, 720)

        cfg = load_config()
        self.v_input      = tk.StringVar(value=cfg.get("last_input", ""))
        self.v_gleif      = tk.StringVar(value=cfg.get("gleif_path", ""))
        self.v_output     = tk.StringVar(value=cfg.get("last_output", ""))
        self.v_col_rcs    = tk.StringVar(value=cfg.get("col_rcs",    "RCS"))
        self.v_col_name   = tk.StringVar(value=cfg.get("col_name",   "NomEntreprise"))
        self.v_col_pays   = tk.StringVar(value=cfg.get("col_pays",   "Pays"))
        self.v_col_lei    = tk.StringVar(value=cfg.get("col_lei",    "LEI_Existant"))
        self.v_col_date   = tk.StringVar(value=cfg.get("col_date",   "LEI_DateValidite"))
        self.v_col_postal = tk.StringVar(value=cfg.get("col_postal", "CodePostal"))
        self.v_threshold     = tk.IntVar(value=int(cfg.get("fuzzy_threshold", 90)))
        self.v_rcs_threshold = tk.IntVar(value=int(cfg.get("rcs_fuzzy_threshold", 88)))
        self.v_active   = tk.BooleanVar(value=bool(cfg.get("active_only", True)))
        self.v_use_slim = tk.BooleanVar(value=bool(cfg.get("use_slim", False)))
        self.v_progress   = tk.DoubleVar(value=0)
        self.v_status_msg = tk.StringVar(value="Prêt — sélectionnez vos fichiers et lancez le rapprochement.")
        self.v_show_advanced = tk.BooleanVar(value=False)

        # Proxy
        _saved_proxy = cfg.get("proxy", None)
        if _saved_proxy is None:
            try:
                from gleif_updater import detect_system_proxy
                _saved_proxy = detect_system_proxy() or ""
            except Exception:
                _saved_proxy = ""
        self.v_proxy = tk.StringVar(value=_saved_proxy)

        self._build_ui()
        self._check_python_arch()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _check_python_arch(self):
        if sys.maxsize <= 2**31:
            self.after(500, lambda: messagebox.showwarning(
                "Python 32 bits détecté",
                "Vous utilisez Python en 32 bits.\n\n"
                "Le chargement du fichier GLEIF (~450 Mo) risque de provoquer une "
                "erreur mémoire.\n\nRecommandation : activez la base SLIM dans "
                "Options avancées, ou installez Python 64 bits.",
            ))

    # ── Layout ───────────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Bandeau supérieur SG ─────────────────────────────────────────────
        header = tk.Frame(self, bg=SG_BLACK, height=64)
        header.pack(fill="x")
        header.pack_propagate(False)

        # Logo (image si disponible, sinon placeholder texte)
        logo_frame = tk.Frame(header, bg=SG_BLACK)
        logo_frame.pack(side="left", padx=20, pady=8)
        self._render_logo(logo_frame)

        tk.Label(
            header, text="LEI Matcher",
            font=("Segoe UI", 16, "bold"),
            fg=SG_WHITE, bg=SG_BLACK,
        ).pack(side="left", padx=(20, 0), pady=18)

        tk.Label(
            header, text="Middle Office  •  v2.0",
            font=("Segoe UI", 9),
            fg=SG_GREY, bg=SG_BLACK,
        ).pack(side="right", padx=20, pady=24)

        # ── Liseré rouge SG ──────────────────────────────────────────────────
        tk.Frame(self, bg=SG_RED, height=4).pack(fill="x")

        # ── Zone principale ──────────────────────────────────────────────────
        body = tk.Frame(self, bg=SG_GREY_LIGHT)
        body.pack(fill="both", expand=True, padx=24, pady=14)

        # ── Section Fichiers (Mode Simple — toujours visible) ────────────────
        self._section_title(body, "1.  Sélection des fichiers")

        self._file_row(body, "Fichier sociétés (.xlsx)", self.v_input,
                       [("Excel", "*.xlsx")], save=False)

        # Base GLEIF + bouton mise à jour
        grow = tk.Frame(body, bg=SG_GREY_LIGHT)
        grow.pack(fill="x", pady=4)
        tk.Label(grow, text="Base GLEIF (.csv)",
                 font=("Segoe UI", 10), fg=SG_BLACK, bg=SG_GREY_LIGHT,
                 width=26, anchor="w").pack(side="left")
        ttk.Entry(grow, textvariable=self.v_gleif, width=46,
                  font=("Segoe UI", 10)).pack(side="left", padx=(4, 4))
        tk.Button(grow, text="Parcourir…", font=("Segoe UI", 9),
                  fg=SG_WHITE, bg=SG_GREY_DARK, activebackground=SG_BLACK,
                  activeforeground=SG_WHITE, relief="flat", cursor="hand2",
                  padx=10, pady=2,
                  command=lambda: _browse_file(self.v_gleif, "Base GLEIF",
                                               [("CSV", "*.csv"), ("Tous", "*.*")])).pack(side="left")
        tk.Button(grow, text="🔄 Mettre à jour", font=("Segoe UI", 9, "bold"),
                  fg=SG_WHITE, bg=SG_RED, activebackground=SG_RED_DARK,
                  activeforeground=SG_WHITE, relief="flat", cursor="hand2",
                  padx=10, pady=2,
                  command=self._open_update_dialog).pack(side="left", padx=(8, 0))

        self._file_row(body, "Fichier de sortie (.xlsx)", self.v_output,
                       [("Excel", "*.xlsx")], save=True)

        # ── Bouton bascule "Options avancées" ────────────────────────────────
        adv_toggle = tk.Frame(body, bg=SG_GREY_LIGHT)
        adv_toggle.pack(fill="x", pady=(14, 0))
        self.btn_toggle_adv = tk.Button(
            adv_toggle, text="▸  Options avancées",
            font=("Segoe UI", 10, "bold"),
            fg=SG_RED, bg=SG_GREY_LIGHT, activebackground=SG_GREY_LIGHT,
            activeforeground=SG_RED_DARK, relief="flat", cursor="hand2",
            anchor="w", command=self._toggle_advanced,
        )
        self.btn_toggle_adv.pack(fill="x")

        # ── Container Options avancées (caché par défaut) ────────────────────
        self.frame_advanced = tk.Frame(
            body, bg=SG_WHITE, highlightbackground=SG_BORDER, highlightthickness=1
        )
        self._build_advanced_panel(self.frame_advanced)
        # NB : non packé tant que toggle_advanced ne le demande pas

        # ── Bouton principal Lancer ──────────────────────────────────────────
        run_frame = tk.Frame(body, bg=SG_GREY_LIGHT)
        run_frame.pack(fill="x", pady=(18, 4))
        self.btn_run = tk.Button(
            run_frame, text="▶  LANCER LE RAPPROCHEMENT",
            font=("Segoe UI", 12, "bold"),
            fg=SG_WHITE, bg=SG_RED,
            activebackground=SG_RED_DARK, activeforeground=SG_WHITE,
            relief="flat", cursor="hand2", padx=32, pady=12,
            command=self._start_matching,
        )
        self.btn_run.pack(side="left")

        self.btn_open = tk.Button(
            run_frame, text="📂 Ouvrir les résultats",
            font=("Segoe UI", 10), fg=SG_BLACK, bg=SG_WHITE,
            activebackground=SG_GREY_LIGHT, activeforeground=SG_BLACK,
            relief="solid", cursor="hand2", padx=18, pady=10, bd=1,
            command=self._open_result, state="disabled",
        )
        self.btn_open.pack(side="left", padx=14)

        # ── Barre de progression ─────────────────────────────────────────────
        self.progress_bar = ttk.Progressbar(
            body, variable=self.v_progress, maximum=100, length=760, mode="determinate")
        self.progress_bar.pack(fill="x", pady=(14, 4))

        self.lbl_status = tk.Label(
            body, textvariable=self.v_status_msg,
            font=("Segoe UI", 9), fg=SG_GREY_DARK, bg=SG_GREY_LIGHT,
            anchor="w", justify="left", wraplength=760,
        )
        self.lbl_status.pack(fill="x", pady=(2, 6))

        # ── Cartes de synthèse ───────────────────────────────────────────────
        self.frame_summary = tk.Frame(body, bg=SG_GREY_LIGHT)
        self.frame_summary.pack(fill="x", pady=(4, 0))

        # ── Bandeau disclaimer permanent ─────────────────────────────────────
        disclaimer = tk.Frame(self, bg=C_VERIF_BG, height=42)
        disclaimer.pack(side="bottom", fill="x")
        disclaimer.pack_propagate(False)
        tk.Label(
            disclaimer,
            text=("⚠  Outil d'aide à la décision — toute correspondance « À vérifier » "
                  "ou « KO » doit être validée manuellement avant usage opérationnel."),
            font=("Segoe UI", 9, "bold"), fg=C_KO_FG, bg=C_VERIF_BG,
            anchor="w", padx=20,
        ).pack(fill="both", expand=True)

    def _render_logo(self, parent):
        """Affiche logo_sg.png si présent, sinon placeholder texte stylisé."""
        try:
            if LOGO_PATH.exists():
                from tkinter import PhotoImage
                img = PhotoImage(file=str(LOGO_PATH))
                # Sous-échantillonner si trop grand
                w, h = img.width(), img.height()
                if h > 48:
                    factor = max(1, h // 48)
                    img = img.subsample(factor, factor)
                self._logo_img_ref = img  # garde une référence
                tk.Label(parent, image=img, bg=SG_BLACK).pack()
                return
        except Exception:
            pass
        # Placeholder texte
        ph = tk.Frame(parent, bg=SG_BLACK)
        ph.pack()
        tk.Frame(ph, bg=SG_RED, width=8, height=40).pack(side="left", padx=(0, 6))
        txt = tk.Frame(ph, bg=SG_BLACK)
        txt.pack(side="left")
        tk.Label(txt, text="SOCIÉTÉ", font=("Arial Black", 10, "bold"),
                 fg=SG_WHITE, bg=SG_BLACK).pack(anchor="w")
        tk.Label(txt, text="GÉNÉRALE", font=("Arial Black", 10, "bold"),
                 fg=SG_RED, bg=SG_BLACK).pack(anchor="w")

    def _build_advanced_panel(self, parent):
        inner = tk.Frame(parent, bg=SG_WHITE)
        inner.pack(fill="x", padx=14, pady=12)

        tk.Label(inner, text="Noms des colonnes (fichier source)",
                 font=("Segoe UI", 10, "bold"), fg=SG_BLACK, bg=SG_WHITE,
                 anchor="w").pack(fill="x", pady=(0, 4))

        for label, var, hint in [
            ("Colonne RCS",            self.v_col_rcs,    "obligatoire"),
            ("Colonne Nom entreprise", self.v_col_name,   "obligatoire"),
            ("Colonne Pays",           self.v_col_pays,   "obligatoire"),
            ("Colonne LEI existant",   self.v_col_lei,    "optionnelle — active le mode validation"),
            ("Colonne Date validité",  self.v_col_date,   "optionnelle — format dd-mm-yyyy"),
            ("Colonne Code Postal",    self.v_col_postal, "optionnelle — affine le matching nom/pays"),
        ]:
            row = tk.Frame(inner, bg=SG_WHITE)
            row.pack(fill="x", pady=2)
            tk.Label(row, text=label, font=("Segoe UI", 9),
                     fg=SG_BLACK, bg=SG_WHITE, width=24, anchor="w").pack(side="left")
            ttk.Entry(row, textvariable=var, width=24,
                      font=("Segoe UI", 9)).pack(side="left", padx=(4, 6))
            tk.Label(row, text=hint, font=("Segoe UI", 8, "italic"),
                     fg=SG_GREY, bg=SG_WHITE).pack(side="left")

        ttk.Separator(inner, orient="horizontal").pack(fill="x", pady=10)
        tk.Label(inner, text="Seuils de correspondance",
                 font=("Segoe UI", 10, "bold"), fg=SG_BLACK, bg=SG_WHITE,
                 anchor="w").pack(fill="x", pady=(0, 4))

        # Seuil nom/pays
        thr = tk.Frame(inner, bg=SG_WHITE)
        thr.pack(fill="x", pady=4)
        tk.Label(thr, text="Seuil similarité nom/pays",
                 font=("Segoe UI", 9), fg=SG_BLACK, bg=SG_WHITE,
                 width=24, anchor="w").pack(side="left")
        self._lbl_threshold = tk.Label(
            thr, text=f"{self.v_threshold.get()} %",
            font=("Segoe UI", 9, "bold"), fg=SG_RED, bg=SG_WHITE, width=6)
        self._lbl_threshold.pack(side="right")
        ttk.Scale(thr, from_=70, to=100, orient="horizontal",
                  variable=self.v_threshold, length=260,
                  command=lambda v: self._lbl_threshold.config(text=f"{int(float(v))} %")
                  ).pack(side="left", padx=(4, 0))

        # Seuil RCS approché
        rthr = tk.Frame(inner, bg=SG_WHITE)
        rthr.pack(fill="x", pady=4)
        tk.Label(rthr, text="Seuil RCS approché",
                 font=("Segoe UI", 9), fg=SG_BLACK, bg=SG_WHITE,
                 width=24, anchor="w").pack(side="left")
        self._lbl_rcs_threshold = tk.Label(
            rthr, text=f"{self.v_rcs_threshold.get()} %",
            font=("Segoe UI", 9, "bold"), fg=SG_RED, bg=SG_WHITE, width=6)
        self._lbl_rcs_threshold.pack(side="right")
        ttk.Scale(rthr, from_=70, to=100, orient="horizontal",
                  variable=self.v_rcs_threshold, length=260,
                  command=lambda v: self._lbl_rcs_threshold.config(text=f"{int(float(v))} %")
                  ).pack(side="left", padx=(4, 0))

        # Cases à cocher
        ttk.Separator(inner, orient="horizontal").pack(fill="x", pady=10)
        ttk.Checkbutton(
            inner,
            text="Utiliser la base slim (gleif_slim.csv — plus rapide, moins de mémoire)",
            variable=self.v_use_slim, command=self._on_slim_toggle,
        ).pack(anchor="w", pady=2)
        ttk.Checkbutton(
            inner,
            text="LEI actifs uniquement (Entity=ACTIVE & LEI=ISSUED) — désactivé en mode validation",
            variable=self.v_active,
        ).pack(anchor="w", pady=2)

    def _toggle_advanced(self):
        if self.frame_advanced.winfo_ismapped():
            self.frame_advanced.pack_forget()
            self.btn_toggle_adv.config(text="▸  Options avancées")
        else:
            self.frame_advanced.pack(fill="x", pady=(4, 0),
                                     after=self.btn_toggle_adv)
            self.btn_toggle_adv.config(text="▾  Options avancées")

    def _section_title(self, parent, text):
        f = tk.Frame(parent, bg=SG_GREY_LIGHT)
        f.pack(fill="x", pady=(2, 6))
        tk.Label(f, text=text, font=("Segoe UI", 11, "bold"),
                 fg=SG_RED, bg=SG_GREY_LIGHT, anchor="w").pack(side="left")

    def _file_row(self, parent, label, var, filetypes, save):
        row = tk.Frame(parent, bg=SG_GREY_LIGHT)
        row.pack(fill="x", pady=4)
        tk.Label(row, text=label, font=("Segoe UI", 10),
                 fg=SG_BLACK, bg=SG_GREY_LIGHT, width=26, anchor="w").pack(side="left")
        ttk.Entry(row, textvariable=var, width=46,
                  font=("Segoe UI", 10)).pack(side="left", padx=(4, 4))
        tk.Button(row, text="Parcourir…", font=("Segoe UI", 9),
                  fg=SG_WHITE, bg=SG_GREY_DARK, activebackground=SG_BLACK,
                  activeforeground=SG_WHITE, relief="flat", cursor="hand2",
                  padx=10, pady=2,
                  command=lambda: _browse_file(var, label, filetypes, save=save)
                  ).pack(side="left")

    def _on_slim_toggle(self):
        gleif = self.v_gleif.get().strip()
        if not gleif:
            return
        slim = str(Path(gleif).parent / "gleif_slim.csv")
        full = str(Path(gleif).parent / "gleif_golden_copy.csv")
        if self.v_use_slim.get():
            if Path(slim).exists():
                self.v_gleif.set(slim)
            else:
                messagebox.showinfo(
                    "Base slim absente",
                    f"Le fichier slim n'existe pas :\n{slim}\n\n"
                    "Cliquez sur « Mettre à jour » → cochez « Préparer base slim ».",
                )
                self.v_use_slim.set(False)
        else:
            if Path(full).exists():
                self.v_gleif.set(full)

    # ── Validation ──────────────────────────────────────────────────────────

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

    # ── Lancement ───────────────────────────────────────────────────────────

    def _start_matching(self):
        if not self._validate():
            return

        # OneDrive avertissement
        if _is_onedrive_path(self.v_input.get()):
            if not messagebox.askyesno(
                "Fichier OneDrive détecté",
                "Le fichier sociétés est dans OneDrive Entreprise.\n\n"
                "Si le fichier est en mode 'cloud-only', une copie temporaire sera "
                "créée automatiquement.\n\nContinuer ?",
            ):
                return

        # Nettoyer les cartes de la précédente exécution
        for w in self.frame_summary.winfo_children():
            w.destroy()

        self.btn_run.config(state="disabled", text="⏳  Traitement en cours…")
        self.btn_open.config(state="disabled")
        self.v_progress.set(0)
        self.v_status_msg.set("Initialisation…")
        threading.Thread(target=self._run_matching, daemon=True).start()

    def _run_matching(self):
        try:
            from gleif_matcher import match_companies

            col_rcs    = self.v_col_rcs.get().strip()
            col_name   = self.v_col_name.get().strip()
            col_pays   = self.v_col_pays.get().strip()
            col_lei    = self.v_col_lei.get().strip()    or None
            col_date   = self.v_col_date.get().strip()   or None
            col_postal = self.v_col_postal.get().strip() or None

            _, stats = match_companies(
                input_path  = self.v_input.get(),
                gleif_path  = self.v_gleif.get(),
                output_path = self.v_output.get(),
                col_rcs     = col_rcs, col_name = col_name, col_pays = col_pays,
                col_lei     = col_lei, col_date = col_date, col_postal = col_postal,
                fuzzy_threshold     = int(self.v_threshold.get()),
                rcs_fuzzy_threshold = int(self.v_rcs_threshold.get()),
                active_only         = bool(self.v_active.get()),
                progress_cb = self._on_progress,
                status_cb   = self._set_status,
            )
            self.after(0, lambda: self._show_summary(stats))

        except Exception as exc:
            friendly = _translate_error(exc)
            self._show_error(friendly)

    def _on_progress(self, done: int, total: int):
        if total > 0:
            pct = min(done / total * 100, 100)
            self.after(0, lambda: (
                self.v_progress.set(pct),
                self.v_status_msg.set(f"Rapprochement : {done:,} / {total:,} lignes  ({pct:.0f} %)"),
            ))

    def _set_status(self, msg: str):
        self.after(0, lambda: self.v_status_msg.set(msg))

    def _show_error(self, msg: str):
        def _s():
            messagebox.showerror("Erreur", msg)
            self.btn_run.config(state="normal", text="▶  LANCER LE RAPPROCHEMENT")
            self.v_status_msg.set("Erreur — voir le message ci-dessus.")
        self.after(0, _s)

    # ── Synthèse ────────────────────────────────────────────────────────────

    def _show_summary(self, stats: dict):
        self.btn_run.config(state="normal", text="▶  LANCER LE RAPPROCHEMENT")
        self.btn_open.config(state="normal")
        self.v_progress.set(100)
        total = stats.get("total", 0)
        pct_ok = (stats.get("ok", 0) / total * 100) if total else 0
        self.v_status_msg.set(
            f"✅  Traitement terminé — {total:,} lignes  •  "
            f"OK {stats.get('ok', 0):,} ({pct_ok:.0f} %)  •  "
            f"À vérifier {stats.get('a_verifier', 0):,}  •  "
            f"KO {stats.get('ko', 0):,}"
        )
        self._save_prefs()

        # Avertissement fort si non-OK ≥ 30 %
        non_ok = stats.get("a_verifier", 0) + stats.get("ko", 0)
        if total and non_ok / total >= 0.3:
            self.after(200, lambda: messagebox.showwarning(
                "Vérification manuelle nécessaire",
                f"⚠ {non_ok:,} ligne(s) sur {total:,} ({non_ok/total*100:.0f} %) "
                f"sont marquées « À vérifier » ou « KO ».\n\n"
                f"Ces lignes nécessitent une validation manuelle avant usage "
                f"opérationnel. Consultez la colonne ActionRequise dans le fichier "
                f"de sortie.",
            ))

        # Cartes de synthèse — 3 niveaux fiabilité + 4 sous-types
        cards = [
            (f"{total:,}",                          "Total",          SG_BLACK,  SG_WHITE),
            (f"{stats.get('ok', 0):,}",             "🟢 OK",          C_OK_FG,   C_OK_BG),
            (f"{stats.get('a_verifier', 0):,}",     "🟡 À vérifier",  C_VERIF_FG, C_VERIF_BG),
            (f"{stats.get('ko', 0):,}",             "🔴 KO",          C_KO_FG,   C_KO_BG),
            (f"{stats.get('exact_rcs', 0):,}",      "Exact RCS+Pays", SG_GREY_DARK, SG_WHITE),
            (f"{stats.get('approx_rcs', 0):,}",     "Approx RCS",     SG_GREY_DARK, SG_WHITE),
            (f"{stats.get('approx_nom', 0):,}",     "Approx Nom",     SG_GREY_DARK, SG_WHITE),
            (f"{stats.get('lei_valide', 0):,}",     "LEI Valide",     C_OK_FG,   SG_WHITE),
            (f"{stats.get('lei_discordant', 0):,}", "LEI Discordant", C_KO_FG,   SG_WHITE),
        ]
        for val, label, fg, bg in cards:
            card = tk.Frame(self.frame_summary, bg=bg,
                            highlightbackground=SG_BORDER, highlightthickness=1)
            card.pack(side="left", padx=(0, 5), ipadx=8, ipady=4)
            tk.Label(card, text=val,   font=("Segoe UI", 14, "bold"),
                     fg=fg, bg=bg).pack()
            tk.Label(card, text=label, font=("Segoe UI", 8),
                     fg=fg, bg=bg).pack()

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
        self.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")


# ─────────────────────────────────────────────────────────────────────────────
# Fenêtre de mise à jour GLEIF (cosmétique SG)
# ─────────────────────────────────────────────────────────────────────────────

class UpdateDialog(tk.Toplevel):

    def __init__(self, parent, v_gleif_path: tk.StringVar, v_proxy: tk.StringVar):
        super().__init__(parent)
        self.title("Mise à jour de la base GLEIF")
        self.resizable(False, False)
        self.configure(bg=SG_GREY_LIGHT)
        self.transient(parent)
        self.grab_set()

        self.v_gleif_path    = v_gleif_path
        self.v_proxy         = v_proxy
        self.v_progress      = tk.DoubleVar(value=0)
        self.v_slim_progress = tk.DoubleVar(value=0)
        self.v_status        = tk.StringVar(value="Prêt.")
        self.v_prepare_slim  = tk.BooleanVar(value=False)
        self._running = False
        self._meta    = None

        self._build_ui()
        self._center(parent, 600, 480)

    def _build_ui(self):
        hdr = tk.Frame(self, bg=SG_BLACK)
        hdr.pack(fill="x")
        tk.Label(hdr, text="  🔄 Mise à jour de la base GLEIF",
                 font=("Segoe UI", 12, "bold"),
                 fg=SG_WHITE, bg=SG_BLACK, anchor="w"
                 ).pack(fill="x", padx=16, pady=10)
        tk.Frame(self, bg=SG_RED, height=3).pack(fill="x")

        body = tk.Frame(self, bg=SG_GREY_LIGHT)
        body.pack(fill="both", expand=True, padx=20, pady=12)

        tk.Label(body,
                 text="Le fichier Golden Copy GLEIF est mis à jour quotidiennement.\n"
                      "La vérification compare votre version locale avec la dernière\n"
                      "disponible (~450 Mo compressé).",
                 font=("Segoe UI", 9), fg=SG_GREY_DARK, bg=SG_GREY_LIGHT, justify="left",
                 ).pack(anchor="w", pady=(0, 8))

        # Dossier de destination
        dr = tk.Frame(body, bg=SG_GREY_LIGHT)
        dr.pack(fill="x", pady=3)
        tk.Label(dr, text="Dossier destination :",
                 font=("Segoe UI", 10), fg=SG_BLACK, bg=SG_GREY_LIGHT,
                 width=22, anchor="w").pack(side="left")
        ttk.Entry(dr, textvariable=self.v_gleif_path, width=40,
                  font=("Segoe UI", 10)).pack(side="left", padx=4)
        tk.Button(dr, text="…", font=("Segoe UI", 10),
                  fg=SG_WHITE, bg=SG_GREY_DARK,
                  relief="flat", cursor="hand2", padx=8,
                  command=self._browse_dest).pack(side="left")

        # Proxy
        pr = tk.Frame(body, bg=SG_GREY_LIGHT)
        pr.pack(fill="x", pady=3)
        tk.Label(pr, text="Proxy HTTP(S) :",
                 font=("Segoe UI", 10), fg=SG_BLACK, bg=SG_GREY_LIGHT,
                 width=22, anchor="w").pack(side="left")
        ttk.Entry(pr, textvariable=self.v_proxy, width=40,
                  font=("Segoe UI", 10)).pack(side="left", padx=4)

        _pv = self.v_proxy.get()
        if _pv:
            _ph, _pc = "✓ proxy détecté", C_OK_FG
        else:
            try:
                import winreg
                with winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                                    r"Software\Microsoft\Windows\CurrentVersion\Internet Settings") as k:
                    winreg.QueryValueEx(k, "AutoConfigURL")
                _ph, _pc = "✓ PAC système détecté", C_OK_FG
            except Exception:
                _ph, _pc = "vide = proxy système (recommandé)", SG_GREY
        tk.Label(pr, text=_ph, font=("Segoe UI", 8, "italic"),
                 fg=_pc, bg=SG_GREY_LIGHT).pack(side="left", padx=4)

        # Avertissement proxy
        warn_fr = tk.Frame(body, bg=C_VERIF_BG, padx=8, pady=6)
        warn_fr.pack(fill="x", pady=(0, 6))
        tk.Label(warn_fr,
                 text="ℹ Si erreur HTTP 407 : videz le champ Proxy.\n"
                      "  L'authentification NTLM/Kerberos est gérée automatiquement.",
                 font=("Segoe UI", 8), fg=C_VERIF_FG, bg=C_VERIF_BG,
                 justify="left", anchor="w").pack(fill="x")

        # Option slim
        ttk.Checkbutton(
            body,
            text="Préparer la base slim après téléchargement (recommandé — réduit la taille ~3×)",
            variable=self.v_prepare_slim,
        ).pack(anchor="w", pady=4)

        # Progressions
        self.progress_bar = ttk.Progressbar(
            body, variable=self.v_progress, maximum=100, length=540, mode="determinate")
        self.progress_bar.pack(fill="x", pady=(10, 2))

        self.slim_bar = ttk.Progressbar(
            body, variable=self.v_slim_progress, maximum=100, length=540, mode="determinate")
        self.slim_bar.pack(fill="x", pady=(0, 4))

        self.lbl_status = tk.Label(
            body, textvariable=self.v_status,
            font=("Segoe UI", 9), fg=SG_GREY_DARK, bg=SG_GREY_LIGHT,
            anchor="w", wraplength=540, justify="left")
        self.lbl_status.pack(fill="x")

        # Boutons
        btn_row = tk.Frame(body, bg=SG_GREY_LIGHT)
        btn_row.pack(fill="x", pady=(12, 0))
        self.btn_check = tk.Button(
            btn_row, text="Vérifier la version",
            font=("Segoe UI", 10, "bold"),
            fg=SG_WHITE, bg=SG_RED,
            activebackground=SG_RED_DARK, activeforeground=SG_WHITE,
            relief="flat", cursor="hand2", padx=16, pady=6,
            command=self._start_check)
        self.btn_check.pack(side="left")

        self.btn_download = tk.Button(
            btn_row, text="Télécharger maintenant",
            font=("Segoe UI", 10), fg=SG_BLACK, bg=SG_WHITE,
            relief="solid", bd=1, cursor="hand2", padx=16, pady=6,
            state="disabled", command=self._start_download)
        self.btn_download.pack(side="left", padx=10)

        tk.Button(btn_row, text="Fermer", font=("Segoe UI", 10),
                  fg=SG_GREY, bg=SG_GREY_LIGHT,
                  relief="flat", cursor="hand2", padx=16, pady=6,
                  command=self.destroy).pack(side="right")

    def _browse_dest(self):
        path = filedialog.askopenfilename(
            title="Choisir le fichier GLEIF (ou son dossier)",
            filetypes=[("CSV", "*.csv"), ("Tous", "*.*")])
        if path:
            self.v_gleif_path.set(path)

    def _set_status(self, msg, color=SG_GREY_DARK):
        def _u():
            self.v_status.set(msg)
            self.lbl_status.config(fg=color)
        self.after(0, _u)

    def _set_dl_progress(self, done, total):
        if total > 0:
            pct = min(done / total * 100, 100)
            mb_d, mb_t = done / 1_048_576, total / 1_048_576
            def _u():
                self.v_progress.set(pct)
                self.v_status.set(
                    f"Téléchargement : {mb_d:.1f} Mo / {mb_t:.1f} Mo  ({pct:.0f} %)"
                )
            self.after(0, _u)

    def _set_slim_progress(self, done, total):
        if total > 0:
            pct = min(done / total * 100, 100)
            self.after(0, lambda: self.v_slim_progress.set(pct))

    def _start_check(self):
        if self._running:
            return
        self._running = True
        self.btn_check.config(state="disabled")
        self.btn_download.config(state="disabled")
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
            dest_dir   = Path(gleif_path).parent if gleif_path else Path.cwd()
            local_date = read_local_version(dest_dir)

            if is_update_available(local_date, meta["publish_date"]):
                msg = (
                    f"✓ Nouvelle version disponible\n"
                    f"   Date    : {meta['publish_date'][:10]}\n"
                    f"   Taille  : {meta['size_human']}  (ZIP)\n"
                    f"   Entités : {meta['record_count']:,}\n\n"
                    f"Cliquez sur « Télécharger maintenant »."
                )
                self.after(0, lambda: self.btn_download.config(state="normal"))
                self._set_status(msg, C_OK_FG)
            else:
                self._set_status(
                    f"✓ Base à jour — version {meta['publish_date'][:10]}\n"
                    f"   {meta['record_count']:,} entités  ({meta['size_human']})",
                    C_OK_FG)
        except Exception as e:
            friendly = _translate_error(e)
            self._set_status(friendly, C_KO_FG)
        finally:
            self._running = False
            self.after(0, lambda: self.btn_check.config(state="normal"))

    def _start_download(self):
        if self._running or not self._meta:
            return
        self._running = True
        self.btn_download.config(state="disabled")
        self.btn_check.config(state="disabled")
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
                progress_cb=self._set_dl_progress, proxy=proxy)

            self._set_status("Extraction du CSV…", SG_GREY_DARK)
            self.after(0, lambda: self.progress_bar.config(mode="indeterminate"))
            self.after(0, self.progress_bar.start)

            final_csv = extract_csv(zip_path, dest_dir)
            write_local_version(dest_dir, meta["publish_date"], final_csv.name)
            self.v_gleif_path.set(str(final_csv))

            self.after(0, lambda: [self.progress_bar.stop(),
                                   self.progress_bar.config(mode="determinate"),
                                   self.v_progress.set(100)])

            if self.v_prepare_slim.get():
                from gleif_matcher import prepare_slim
                slim_path = dest_dir / "gleif_slim.csv"
                self._set_status("Génération de la base slim…", SG_GREY_DARK)
                prepare_slim(
                    str(final_csv), str(slim_path),
                    active_only=True,
                    progress_cb=self._set_slim_progress,
                    status_cb=lambda m: self._set_status(m, SG_GREY_DARK),
                )
                self.after(0, lambda: self.v_slim_progress.set(100))
                self.v_gleif_path.set(str(slim_path))
                self._set_status(
                    f"✓ Mise à jour + slim terminés — {meta['publish_date'][:10]}\n"
                    f"   Slim : {slim_path.name}", C_OK_FG)
            else:
                self._set_status(
                    f"✓ Mise à jour terminée — {meta['publish_date'][:10]}\n"
                    f"   Fichier : {final_csv.name}", C_OK_FG)

        except Exception as e:
            self.after(0, lambda: [self.progress_bar.stop(),
                                   self.progress_bar.config(mode="determinate")])
            self._set_status(_translate_error(e), C_KO_FG)
        finally:
            self._running = False
            self.after(0, lambda: self.btn_check.config(state="normal"))

    def _center(self, parent, w, h):
        parent.update_idletasks()
        px, py = parent.winfo_rootx(), parent.winfo_rooty()
        pw, ph = parent.winfo_width(), parent.winfo_height()
        self.geometry(f"{w}x{h}+{px+(pw-w)//2}+{py+(ph-h)//2}")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = GleifApp()
    app.mainloop()
