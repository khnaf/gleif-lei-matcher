"""
gleif_gui.py
============
Interface graphique tkinter pour GLEIF LEI Matcher v1.2.
Double-clic sur LANCER.bat (Windows) pour démarrer.

Nouveautés v1.2 :
  - Champ optionnel « Colonne LEI existant » pour le mode validation
  - Résumé étendu : LEI Valide / LEI Discordant / LEI Inconnu GLEIF
  - Date de renouvellement dans le fichier de sortie

Dépendances : pandas openpyxl rapidfuzz  (installées automatiquement par LANCER.bat)
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

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    defaults = {
        "gleif_path": "", "col_rcs": "RCS", "col_name": "NomEntreprise",
        "col_pays": "Pays", "col_lei": "LEI_Existant",
        "fuzzy_threshold": 80, "active_only": True,
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
# Palette couleurs
# ─────────────────────────────────────────────────────────────────────────────
C_BG         = "#F5F7FA"
C_PANEL      = "#FFFFFF"
C_ACCENT     = "#1F4E79"
C_ACCENT2    = "#2E75B6"
C_TEXT       = "#2C2C2C"
C_SUBTLE     = "#6B7280"
C_GREEN      = "#1E7E34"
C_YELLOW     = "#856404"
C_ORANGE     = "#8B4513"
C_BLUE       = "#1F4E79"
C_RED        = "#842029"
C_GREEN_BG   = "#D4EDDA"
C_YELLOW_BG  = "#FFF3CD"
C_ORANGE_BG  = "#FCE8D0"
C_BLUE_BG    = "#DAE8FC"
C_RED_BG     = "#F8D7DA"
C_BORDER     = "#D1D5DB"
C_WARN_BG    = "#FFF3CD"
C_WARN_FG    = "#856404"


def _browse_file(var, title, filetypes, save=False):
    if save:
        path = filedialog.asksaveasfilename(title=title, filetypes=filetypes, defaultextension=".xlsx")
    else:
        path = filedialog.askopenfilename(title=title, filetypes=filetypes)
    if path:
        var.set(path)


def _is_onedrive_path(path: str) -> bool:
    return "onedrive" in path.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Fenêtre principale
# ─────────────────────────────────────────────────────────────────────────────

class GleifApp(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("GLEIF LEI Matcher")
        self.resizable(False, False)
        self.configure(bg=C_BG)
        self._center_window(800, 780)

        cfg = load_config()

        self.v_input      = tk.StringVar(value=cfg.get("last_input", ""))
        self.v_gleif      = tk.StringVar(value=cfg.get("gleif_path", ""))
        self.v_output     = tk.StringVar(value=cfg.get("last_output", ""))
        self.v_col_rcs    = tk.StringVar(value=cfg.get("col_rcs",  "RCS"))
        self.v_col_name   = tk.StringVar(value=cfg.get("col_name", "NomEntreprise"))
        self.v_col_pays   = tk.StringVar(value=cfg.get("col_pays", "Pays"))
        self.v_col_lei    = tk.StringVar(value=cfg.get("col_lei",  "LEI_Existant"))
        self.v_threshold      = tk.IntVar(value=int(cfg.get("fuzzy_threshold", 80)))
        self.v_rcs_threshold  = tk.IntVar(value=int(cfg.get("rcs_fuzzy_threshold", 88)))
        self.v_active         = tk.BooleanVar(value=bool(cfg.get("active_only", True)))
        self.v_use_slim   = tk.BooleanVar(value=bool(cfg.get("use_slim", False)))
        self.v_progress   = tk.DoubleVar(value=0)
        self.v_status_msg = tk.StringVar(value="En attente…")

        # Proxy : préférence sauvegardée ou auto-détection
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

    # ── Vérifications au démarrage ───────────────────────────────────────────

    def _check_python_arch(self):
        """Avertit si Python est en 32 bits (risque OOM sur le CSV de 450 Mo)."""
        import sys as _sys
        if _sys.maxsize <= 2**31:
            self.after(500, lambda: messagebox.showwarning(
                "Python 32 bits détecté",
                "Vous utilisez Python en 32 bits.\n\n"
                "Le chargement du fichier GLEIF (~450 Mo) risque de provoquer\n"
                "une erreur mémoire (out of memory).\n\n"
                "✅  Recommandation : utilisez la base SLIM (option ci-dessous)\n"
                "   ou installez Python/Miniforge en 64 bits.",
            ))

    # ── Layout ───────────────────────────────────────────────────────────────

    def _build_ui(self):
        # En-tête
        header = tk.Frame(self, bg=C_ACCENT, height=56)
        header.pack(fill="x")
        tk.Label(
            header, text="  GLEIF LEI Matcher  v1.2",
            font=("Segoe UI", 14, "bold"), fg="white", bg=C_ACCENT, anchor="w",
        ).pack(fill="x", padx=20, pady=12)

        body = tk.Frame(self, bg=C_BG)
        body.pack(fill="both", expand=True, padx=20, pady=12)

        # ── Fichiers ─────────────────────────────────────────────────────────
        self._section(body, "Fichiers")
        self._file_row(body, "Fichier sociétés (.xlsx)", self.v_input,
                       [("Excel", "*.xlsx")], save=False)

        # Ligne Base GLEIF + bouton mise à jour
        grow = tk.Frame(body, bg=C_BG)
        grow.pack(fill="x", pady=3)
        tk.Label(grow, text="Base GLEIF (.csv)",
                 font=("Segoe UI", 10), fg=C_TEXT, bg=C_BG, width=26, anchor="w").pack(side="left")
        ttk.Entry(grow, textvariable=self.v_gleif, width=46,
                  font=("Segoe UI", 10)).pack(side="left", padx=(4, 4))
        tk.Button(grow, text="...", font=("Segoe UI", 10), fg=C_ACCENT2,
                  bg=C_PANEL, relief="flat", cursor="hand2", padx=8, pady=2,
                  command=lambda: _browse_file(self.v_gleif, "Base GLEIF",
                                               [("CSV", "*.csv"), ("Tous", "*.*")])).pack(side="left")
        self.btn_update = tk.Button(
            grow, text="Mettre a jour",
            font=("Segoe UI", 10), fg=C_ACCENT2, bg=C_PANEL,
            relief="flat", cursor="hand2", padx=10, pady=2,
            command=self._open_update_dialog,
        )
        self.btn_update.pack(side="left", padx=(8, 0))

        self._file_row(body, "Fichier de sortie (.xlsx)", self.v_output,
                       [("Excel", "*.xlsx")], save=True)

        # Option base slim
        slim_row = tk.Frame(body, bg=C_BG)
        slim_row.pack(fill="x", pady=2)
        tk.Label(slim_row, text="", bg=C_BG, width=26).pack(side="left")
        ttk.Checkbutton(
            slim_row,
            text="Utiliser la base slim  (gleif_slim.csv — plus rapide, moins de mémoire)",
            variable=self.v_use_slim,
            command=self._on_slim_toggle,
        ).pack(side="left")

        ttk.Separator(body, orient="horizontal").pack(fill="x", pady=10)

        # ── Colonnes ─────────────────────────────────────────────────────────
        self._section(body, "Noms des colonnes  (fichier sociétés)")
        cols_frame = tk.Frame(body, bg=C_BG)
        cols_frame.pack(fill="x")

        for label, var in [
            ("Colonne RCS",            self.v_col_rcs),
            ("Colonne Nom entreprise", self.v_col_name),
            ("Colonne Pays",           self.v_col_pays),
        ]:
            row = tk.Frame(cols_frame, bg=C_BG)
            row.pack(fill="x", pady=2)
            tk.Label(row, text=label, font=("Segoe UI", 10),
                     fg=C_TEXT, bg=C_BG, width=26, anchor="w").pack(side="left")
            ttk.Entry(row, textvariable=var, width=28,
                      font=("Segoe UI", 10)).pack(side="left", padx=(4, 0))

        # Colonne LEI existant (optionnelle)
        lei_col_row = tk.Frame(cols_frame, bg=C_BG)
        lei_col_row.pack(fill="x", pady=2)
        tk.Label(
            lei_col_row,
            text="Colonne LEI existant",
            font=("Segoe UI", 10), fg=C_TEXT, bg=C_BG, width=26, anchor="w",
        ).pack(side="left")
        ttk.Entry(
            lei_col_row, textvariable=self.v_col_lei, width=28,
            font=("Segoe UI", 10),
        ).pack(side="left", padx=(4, 4))
        tk.Label(
            lei_col_row,
            text="(optionnel — laisser vide si pas de LEI en base)",
            font=("Segoe UI", 8, "italic"), fg=C_SUBTLE, bg=C_BG,
        ).pack(side="left")

        # Bulle d'info mode validation
        info_row = tk.Frame(body, bg="#EEF4FF", padx=8, pady=4)
        info_row.pack(fill="x", pady=(2, 0))
        tk.Label(
            info_row,
            text=(
                "ℹ  Si la colonne LEI existant est renseignée :\n"
                "   • Lignes avec LEI → validation : comparaison GLEIF vs votre base (RCS, nom, pays)\n"
                "   • Lignes sans LEI → recherche normale (RCS puis nom approché)"
            ),
            font=("Segoe UI", 8), fg="#1F4E79", bg="#EEF4FF", justify="left", anchor="w",
        ).pack(fill="x")

        ttk.Separator(body, orient="horizontal").pack(fill="x", pady=10)

        # ── Options ──────────────────────────────────────────────────────────
        self._section(body, "Options de correspondance")
        opt_frame = tk.Frame(body, bg=C_BG)
        opt_frame.pack(fill="x")

        # Seuil RCS approché
        rcs_thr_row = tk.Frame(opt_frame, bg=C_BG)
        rcs_thr_row.pack(fill="x", pady=4)
        tk.Label(rcs_thr_row, text="Seuil RCS approché",
                 font=("Segoe UI", 10), fg=C_TEXT, bg=C_BG, width=26, anchor="w").pack(side="left")
        self._lbl_rcs_threshold = tk.Label(
            rcs_thr_row, text=f"{self.v_rcs_threshold.get()} %",
            font=("Segoe UI", 10, "bold"), fg=C_ACCENT2, bg=C_BG, width=6)
        self._lbl_rcs_threshold.pack(side="right")
        ttk.Scale(rcs_thr_row, from_=70, to=100, orient="horizontal",
                  variable=self.v_rcs_threshold, length=280,
                  command=lambda v: self._lbl_rcs_threshold.config(text=f"{int(float(v))} %")
                  ).pack(side="left", padx=(4, 0))

        rcs_hint = tk.Frame(opt_frame, bg=C_BG)
        rcs_hint.pack(fill="x", pady=(0, 4))
        tk.Label(rcs_hint, text="",
                 bg=C_BG, width=26).pack(side="left")
        tk.Label(rcs_hint,
                 text="Ex: '1513210151' ≈ '01513210151' (zéro de tête) → détecté à 95%+    |    100% = exact uniquement",
                 font=("Segoe UI", 8, "italic"), fg=C_SUBTLE, bg=C_BG).pack(side="left")

        # Seuil similarité nom/pays
        thr_row = tk.Frame(opt_frame, bg=C_BG)
        thr_row.pack(fill="x", pady=4)
        tk.Label(thr_row, text="Seuil similarité nom/pays",
                 font=("Segoe UI", 10), fg=C_TEXT, bg=C_BG, width=26, anchor="w").pack(side="left")
        self._lbl_threshold = tk.Label(
            thr_row, text=f"{self.v_threshold.get()} %",
            font=("Segoe UI", 10, "bold"), fg=C_ACCENT2, bg=C_BG, width=6)
        self._lbl_threshold.pack(side="right")
        ttk.Scale(thr_row, from_=50, to=100, orient="horizontal",
                  variable=self.v_threshold, length=280,
                  command=lambda v: self._lbl_threshold.config(text=f"{int(float(v))} %")
                  ).pack(side="left", padx=(4, 0))

        chk_row = tk.Frame(opt_frame, bg=C_BG)
        chk_row.pack(fill="x", pady=4)
        ttk.Checkbutton(
            chk_row,
            text="LEI actifs uniquement  (Entity=ACTIVE & LEI=ISSUED)  "
                 "— désactivé automatiquement en mode validation",
            variable=self.v_active,
        ).pack(side="left")

        ttk.Separator(body, orient="horizontal").pack(fill="x", pady=10)

        # ── Boutons d'action ─────────────────────────────────────────────────
        btn_frame = tk.Frame(body, bg=C_BG)
        btn_frame.pack(fill="x")
        self.btn_run = tk.Button(
            btn_frame, text="Lancer le rapprochement",
            font=("Segoe UI", 11, "bold"), fg="white", bg=C_ACCENT2,
            activeforeground="white", activebackground=C_ACCENT,
            relief="flat", cursor="hand2", padx=24, pady=8,
            command=self._start_matching,
        )
        self.btn_run.pack(side="left")

        self.btn_open = tk.Button(
            btn_frame, text="Ouvrir les resultats",
            font=("Segoe UI", 11), fg=C_ACCENT2, bg=C_PANEL,
            activeforeground=C_ACCENT, activebackground="#E8F0FA",
            relief="flat", cursor="hand2", padx=24, pady=8,
            command=self._open_result, state="disabled",
        )
        self.btn_open.pack(side="left", padx=12)

        # Barre de progression
        self.progress_bar = ttk.Progressbar(
            body, variable=self.v_progress, maximum=100, length=760, mode="determinate")
        self.progress_bar.pack(fill="x", pady=(12, 0))

        self.lbl_status = tk.Label(
            body, textvariable=self.v_status_msg,
            font=("Segoe UI", 9), fg=C_SUBTLE, bg=C_BG, anchor="w")
        self.lbl_status.pack(fill="x", pady=(4, 0))

        self.frame_summary = tk.Frame(body, bg=C_BG)
        self.frame_summary.pack(fill="x", pady=(8, 0))

    def _section(self, parent, text):
        tk.Label(parent, text=text, font=("Segoe UI", 10, "bold"),
                 fg=C_ACCENT, bg=C_BG, anchor="w").pack(fill="x", pady=(6, 2))

    def _file_row(self, parent, label, var, filetypes, save):
        row = tk.Frame(parent, bg=C_BG)
        row.pack(fill="x", pady=3)
        tk.Label(row, text=label, font=("Segoe UI", 10),
                 fg=C_TEXT, bg=C_BG, width=26, anchor="w").pack(side="left")
        ttk.Entry(row, textvariable=var, width=46,
                  font=("Segoe UI", 10)).pack(side="left", padx=(4, 4))
        tk.Button(row, text="...", font=("Segoe UI", 10), fg=C_ACCENT2,
                  bg=C_PANEL, relief="flat", cursor="hand2", padx=8, pady=2,
                  command=lambda: _browse_file(var, label, filetypes, save=save)).pack(side="left")

    def _on_slim_toggle(self):
        """Met à jour le chemin GLEIF quand l'option slim est activée."""
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
                    f"Le fichier slim n'existe pas encore :\n{slim}\n\n"
                    "Cliquez sur Mettre a jour -> cochez 'Preparer base slim' pour le generer.",
                )
                self.v_use_slim.set(False)
        else:
            if Path(full).exists():
                self.v_gleif.set(full)

    # ── Rapprochement ────────────────────────────────────────────────────────

    def _validate(self):
        errors = []
        if not self.v_input.get().strip():
            errors.append("Fichier societes manquant")
        elif not Path(self.v_input.get()).exists():
            errors.append("Fichier societes introuvable")
        if not self.v_gleif.get().strip():
            errors.append("Base GLEIF manquante")
        elif not Path(self.v_gleif.get()).exists():
            errors.append("Base GLEIF introuvable")
        if not self.v_output.get().strip():
            errors.append("Fichier de sortie non defini")
        if errors:
            messagebox.showerror("Champs manquants",
                                 "Veuillez corriger :\n\n" + "\n".join(errors))
            return False
        return True

    def _start_matching(self):
        if not self._validate():
            return

        # Avertissement OneDrive sur le fichier d'entrée
        inp = self.v_input.get()
        if _is_onedrive_path(inp):
            if not messagebox.askyesno(
                "Fichier OneDrive detecte",
                "Le fichier societes est dans OneDrive Entreprise.\n\n"
                "Si le fichier est en mode 'cloud-only' (non telecharge localement),\n"
                "une erreur de permission peut survenir.\n\n"
                "Une copie temporaire sera creee automatiquement si necessaire.\n\n"
                "Continuer ?",
            ):
                return

        for w in self.frame_summary.winfo_children():
            w.destroy()
        self.btn_run.config(state="disabled", text="Traitement en cours...")
        self.btn_open.config(state="disabled")
        self.v_progress.set(0)
        self.v_status_msg.set("Initialisation...")
        threading.Thread(target=self._run_matching, daemon=True).start()

    def _run_matching(self):
        try:
            import pandas as pd
            from gleif_matcher import (
                load_gleif, build_indices,
                search_by_rcs, search_by_rcs_fuzzy, search_by_name_country, search_by_lei,
                _check_lei_discordance, normalize_rcs, normalize_name,
                country_to_iso, _export_excel, _safe_read_excel,
            )
            from rapidfuzz import fuzz as _fuzz

            self._set_status("Lecture du fichier societes...")
            try:
                df_input = _safe_read_excel(self.v_input.get()).fillna("")
            except PermissionError:
                self._show_error(
                    "Impossible de lire le fichier societes.\n\n"
                    "Si le fichier est dans OneDrive Entreprise :\n"
                    "  . Assurez-vous qu'il est telecharge localement\n"
                    "  . Ou copiez-le dans un dossier local avant traitement."
                )
                return

            n_total  = len(df_input)
            col_rcs  = self.v_col_rcs.get().strip()
            col_name = self.v_col_name.get().strip()
            col_pays = self.v_col_pays.get().strip()
            col_lei  = self.v_col_lei.get().strip() or None

            # Vérification colonnes obligatoires
            missing = [c for c in [col_rcs, col_name, col_pays] if c not in df_input.columns]
            if missing:
                self._show_error(
                    f"Colonnes introuvables dans le fichier societes :\n{missing}\n\n"
                    f"Colonnes disponibles : {list(df_input.columns)}"
                )
                return

            # Détermination du mode LEI
            has_lei_col = bool(col_lei) and col_lei in df_input.columns
            active_only = bool(self.v_active.get())
            # En mode validation, charger tous les statuts pour trouver les LEI expirés
            _active_only_load = active_only if not has_lei_col else False

            self._set_status("Chargement de la base GLEIF...")
            df_gleif = load_gleif(
                self.v_gleif.get(),
                active_only=_active_only_load,
                status_cb=self._set_status,
            )

            self._set_status("Construction des index...")
            rcs_index, name_index, lei_index = build_indices(df_gleif)

            threshold     = int(self.v_threshold.get())
            rcs_threshold = int(self.v_rcs_threshold.get())
            results = []
            n_exact = n_approx_rcs = n_approx = n_miss = 0
            n_valid = n_discordant = n_lei_unknown = 0

            for idx, row in df_input.iterrows():
                rcs_raw   = str(row[col_rcs]).strip()  if col_rcs  in df_input.columns else ""
                name_raw  = str(row[col_name]).strip() if col_name in df_input.columns else ""
                pays_raw  = str(row[col_pays]).strip() if col_pays in df_input.columns else ""
                lei_exist = str(row[col_lei]).strip()  if has_lei_col else ""

                rcs_norm  = normalize_rcs(rcs_raw)
                name_norm = normalize_name(name_raw)
                iso       = country_to_iso(pays_raw)

                gleif_row   = None
                match_type  = "Non trouve"
                match_score = ""
                disc_text   = ""

                # ── Mode 1 : validation LEI existant ─────────────────────────
                if lei_exist:
                    gleif_row = search_by_lei(lei_exist, lei_index, df_gleif)

                    if gleif_row is not None:
                        # LEI trouvé directement → vérifier cohérence des données
                        disc_text, is_disc = _check_lei_discordance(
                            gleif_row, rcs_raw, name_raw, iso, client_lei=lei_exist
                        )
                        if is_disc:
                            match_type = "LEI Discordant"
                            n_discordant += 1
                        else:
                            match_type = "LEI Valide"
                            n_valid += 1
                    else:
                        # LEI introuvable → fallback RCS/nom pour retrouver l'entité
                        # et comparer le bon LEI avec celui du client
                        fallback_row = None
                        if rcs_norm:
                            fallback_row = search_by_rcs(rcs_norm, rcs_index, df_gleif)
                        if fallback_row is None and name_norm:
                            fallback_row, _sc = search_by_name_country(
                                name_norm, iso, name_index, df_gleif, threshold)

                        if fallback_row is not None:
                            disc_text, _ = _check_lei_discordance(
                                fallback_row, rcs_raw, name_raw, iso, client_lei=lei_exist
                            )
                            if not disc_text:
                                g_lei = str(fallback_row.get("lei", "")).strip()
                                disc_text = f"LEI: client='{lei_exist}' ≠ GLEIF='{g_lei}'"
                            match_type = "LEI Discordant"
                            n_discordant += 1
                            gleif_row = fallback_row
                        else:
                            match_type = "Non trouvé (LEI invalide)"
                            n_lei_unknown += 1

                # ── Mode 2 : recherche d'un LEI manquant ─────────────────────
                else:
                    # 2a. RCS exact
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

                    # 2b. RCS approché (zéros de tête, fautes légères)
                    if gleif_row is None and rcs_norm and rcs_threshold < 100:
                        approx_r, rcs_sc = search_by_rcs_fuzzy(
                            rcs_norm, rcs_index, df_gleif, rcs_threshold)
                        if approx_r is not None:
                            if active_only:
                                es = str(approx_r.get("entity_status", "")).upper()
                                ls = str(approx_r.get("lei_status", "")).upper()
                                if es != "ACTIVE" or ls != "ISSUED":
                                    approx_r = None
                            if approx_r is not None:
                                gl_nm = normalize_name(str(approx_r.get("name", "")))
                                nm_sc = _fuzz.token_sort_ratio(name_norm, gl_nm) if name_norm and gl_nm else ""
                                match_score = (f"RCS:{rcs_sc}% / Nom:{nm_sc}%"
                                               if nm_sc != "" else f"RCS:{rcs_sc}%")
                                match_type  = "Approx – RCS"
                                gleif_row   = approx_r
                                n_approx_rcs += 1

                    # 2c. Fuzzy nom + pays
                    if gleif_row is None and name_norm:
                        gleif_row, score = search_by_name_country(
                            name_norm, iso, name_index, df_gleif, threshold)
                        if gleif_row is not None:
                            if active_only:
                                es = str(gleif_row.get("entity_status", "")).upper()
                                ls = str(gleif_row.get("lei_status", "")).upper()
                                if es != "ACTIVE" or ls != "ISSUED":
                                    gleif_row = None
                                    score = 0
                            if gleif_row is not None:
                                match_type  = "Approx – Nom/Pays"
                                match_score = score
                                n_approx   += 1

                    if gleif_row is None:
                        n_miss += 1

                results.append({
                    "LEI_GLEIF":                gleif_row["lei"]            if gleif_row is not None else "",
                    "GLEIF_NomLegal":           gleif_row["name"]           if gleif_row is not None else "",
                    "GLEIF_Pays":               gleif_row["country"]        if gleif_row is not None else "",
                    "GLEIF_StatutSociete":      gleif_row["entity_status"]  if gleif_row is not None else "",
                    "GLEIF_StatutLEI":          gleif_row["lei_status"]     if gleif_row is not None else "",
                    "GLEIF_AutoriteRegistre":   gleif_row["ra_id"]          if gleif_row is not None else "",
                    "GLEIF_NumRegistre":        gleif_row["ra_entity"]      if gleif_row is not None else "",
                    "GLEIF_DateRenouvellement": gleif_row["renewal_date"]   if gleif_row is not None else "",
                    "TypeCorrespondance":       match_type,
                    "ScoreCorrespondance":      match_score,
                    "LEI_Discordance":          disc_text,
                })

                if (idx + 1) % 10 == 0 or (idx + 1) == n_total:
                    n_ok  = n_exact + n_approx_rcs + n_approx + n_valid
                    n_ko  = n_discordant + n_lei_unknown + n_miss
                    pct   = (idx + 1) / n_total * 100
                    self._update_progress(
                        pct,
                        f"Traitement : {idx+1}/{n_total}  —  "
                        f"OK {n_ok}  DISC {n_discordant}  MISS {n_ko}"
                    )

            self._set_status("Export Excel...")
            df_results = pd.DataFrame(results)
            df_output  = pd.concat([df_input.reset_index(drop=True), df_results], axis=1)
            _export_excel(df_output, self.v_output.get(), threshold)
            self.after(0, lambda: self._show_summary(
                n_total, n_exact, n_approx_rcs, n_approx, n_miss,
                n_valid, n_discordant, n_lei_unknown
            ))

        except Exception as exc:
            self._show_error(str(exc))

    def _set_status(self, msg):
        self.after(0, lambda: self.v_status_msg.set(msg))

    def _update_progress(self, pct, msg):
        def _u():
            self.v_progress.set(pct)
            self.v_status_msg.set(msg)
        self.after(0, _u)

    def _show_error(self, msg):
        def _s():
            messagebox.showerror("Erreur", msg)
            self.btn_run.config(state="normal", text="Lancer le rapprochement")
            self.v_status_msg.set("Erreur — voir le message ci-dessus.")
        self.after(0, _s)

    def _show_summary(self, n_total, n_exact, n_approx_rcs=0, n_approx=0, n_miss=0,
                      n_valid=0, n_discordant=0, n_lei_unknown=0):
        self.btn_run.config(state="normal", text="Lancer le rapprochement")
        self.btn_open.config(state="normal")
        self.v_progress.set(100)
        self.v_status_msg.set("Traitement termine — fichier Excel genere.")
        self._save_prefs()

        cards = [
            (f"{n_total}",        "Total",              C_ACCENT,  "#E8F0FA"),
            (f"{n_valid}",        "LEI Valide",         C_GREEN,   C_GREEN_BG),
            (f"{n_discordant}",   "LEI Discordant",     C_ORANGE,  C_ORANGE_BG),
            (f"{n_lei_unknown}",  "LEI invalide",       C_BLUE,    C_BLUE_BG),
            (f"{n_exact}",        "Exact – RCS",        C_GREEN,   C_GREEN_BG),
            (f"{n_approx_rcs}",   "Approx – RCS",       C_GREEN,   "#EAF4E4"),
            (f"{n_approx}",       "Approx – Nom",       C_YELLOW,  C_YELLOW_BG),
            (f"{n_miss}",         "Non trouve",         C_RED,     C_RED_BG),
        ]
        for val, label, fg, bg in cards:
            card = tk.Frame(self.frame_summary, bg=bg,
                            highlightbackground=C_BORDER, highlightthickness=1)
            card.pack(side="left", padx=(0, 6), ipadx=10, ipady=6)
            tk.Label(card, text=val,   font=("Segoe UI", 16, "bold"), fg=fg, bg=bg).pack()
            tk.Label(card, text=label, font=("Segoe UI", 8),           fg=fg, bg=bg).pack()

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

    # ── Persistance ──────────────────────────────────────────────────────────

    def _save_prefs(self):
        save_user_prefs({
            "last_input":      self.v_input.get(),
            "gleif_path":      self.v_gleif.get(),
            "last_output":     self.v_output.get(),
            "col_rcs":         self.v_col_rcs.get(),
            "col_name":        self.v_col_name.get(),
            "col_pays":        self.v_col_pays.get(),
            "col_lei":         self.v_col_lei.get(),
            "fuzzy_threshold":     int(self.v_threshold.get()),
            "rcs_fuzzy_threshold": int(self.v_rcs_threshold.get()),
            "active_only":         bool(self.v_active.get()),
            "use_slim":        bool(self.v_use_slim.get()),
            "proxy":           self.v_proxy.get(),
        })

    def _on_close(self):
        self._save_prefs()
        self.destroy()

    def _center_window(self, w, h):
        self.update_idletasks()
        sw, sh = self.winfo_screenwidth(), self.winfo_screenheight()
        self.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")


# ─────────────────────────────────────────────────────────────────────────────
# Fenetre de mise a jour GLEIF
# ─────────────────────────────────────────────────────────────────────────────

class UpdateDialog(tk.Toplevel):

    def __init__(self, parent, v_gleif_path: tk.StringVar, v_proxy: tk.StringVar):
        super().__init__(parent)
        self.title("Mise a jour de la base GLEIF")
        self.resizable(False, False)
        self.configure(bg=C_BG)
        self.transient(parent)
        self.grab_set()

        self.v_gleif_path    = v_gleif_path
        self.v_proxy         = v_proxy
        self.v_progress      = tk.DoubleVar(value=0)
        self.v_slim_progress = tk.DoubleVar(value=0)
        self.v_status        = tk.StringVar(value="Pret.")
        self.v_prepare_slim  = tk.BooleanVar(value=False)
        self._running        = False
        self._meta           = None

        self._build_ui()
        self._center(parent, 580, 480)

    def _build_ui(self):
        hdr = tk.Frame(self, bg=C_ACCENT2)
        hdr.pack(fill="x")
        tk.Label(hdr, text="  Mise a jour de la base GLEIF",
                 font=("Segoe UI", 12, "bold"), fg="white", bg=C_ACCENT2, anchor="w"
                 ).pack(fill="x", padx=16, pady=10)

        body = tk.Frame(self, bg=C_BG)
        body.pack(fill="both", expand=True, padx=20, pady=12)

        tk.Label(body,
                 text=("Le fichier Golden Copy GLEIF est mis a jour quotidiennement.\n"
                       "La verification compare votre version locale avec la derniere\n"
                       "disponible sur les serveurs GLEIF (~450 Mo compresse)."),
                 font=("Segoe UI", 9), fg=C_SUBTLE, bg=C_BG, justify="left",
                 ).pack(anchor="w", pady=(0, 8))

        # Dossier de destination
        dr = tk.Frame(body, bg=C_BG)
        dr.pack(fill="x", pady=3)
        tk.Label(dr, text="Dossier de destination :",
                 font=("Segoe UI", 10), fg=C_TEXT, bg=C_BG, width=22, anchor="w").pack(side="left")
        ttk.Entry(dr, textvariable=self.v_gleif_path, width=36,
                  font=("Segoe UI", 10)).pack(side="left", padx=4)
        tk.Button(dr, text="...", font=("Segoe UI", 10), fg=C_ACCENT2,
                  bg=C_PANEL, relief="flat", cursor="hand2", padx=6,
                  command=self._browse_dest).pack(side="left")

        # Proxy
        pr = tk.Frame(body, bg=C_BG)
        pr.pack(fill="x", pady=3)
        tk.Label(pr, text="Proxy HTTP(S) :",
                 font=("Segoe UI", 10), fg=C_TEXT, bg=C_BG, width=22, anchor="w").pack(side="left")
        ttk.Entry(pr, textvariable=self.v_proxy, width=36,
                  font=("Segoe UI", 10)).pack(side="left", padx=4)

        # Indicateur proxy
        _pv = self.v_proxy.get()
        if _pv:
            _ph, _pc = "OK proxy detecte", C_GREEN
        else:
            try:
                import winreg
                with winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                                    r"Software\Microsoft\Windows\CurrentVersion\Internet Settings") as k:
                    winreg.QueryValueEx(k, "AutoConfigURL")
                _ph, _pc = "OK PAC detecte — proxy systeme automatique", C_GREEN
            except Exception:
                _ph, _pc = "vide = proxy systeme (recommande)", C_SUBTLE
        tk.Label(pr, text=_ph, font=("Segoe UI", 8, "italic"), fg=_pc, bg=C_BG).pack(side="left", padx=4)

        # Avertissement proxy explicite
        warn_fr = tk.Frame(body, bg=C_WARN_BG, padx=8, pady=4)
        warn_fr.pack(fill="x", pady=(0, 6))
        tk.Label(warn_fr,
                 text="ATTENTION: Si vous obtenez HTTP 407, videz le champ proxy.\n"
                      "    L'authentification NTLM/Kerberos est geree automatiquement par Windows.",
                 font=("Segoe UI", 8), fg=C_WARN_FG, bg=C_WARN_BG, justify="left", anchor="w",
                 ).pack(fill="x")

        # Option slim
        slim_chk = tk.Frame(body, bg=C_BG)
        slim_chk.pack(fill="x", pady=4)
        ttk.Checkbutton(
            slim_chk,
            text="Preparer la base slim apres telechargement  (recommande — reduit la taille ~3x)",
            variable=self.v_prepare_slim,
        ).pack(side="left")

        # Barres de progression
        self.progress_bar = ttk.Progressbar(
            body, variable=self.v_progress, maximum=100, length=540, mode="determinate")
        self.progress_bar.pack(fill="x", pady=(10, 2))

        self.slim_bar = ttk.Progressbar(
            body, variable=self.v_slim_progress, maximum=100, length=540, mode="determinate")
        self.slim_bar.pack(fill="x", pady=(0, 4))

        self.lbl_status = tk.Label(
            body, textvariable=self.v_status,
            font=("Segoe UI", 9), fg=C_SUBTLE, bg=C_BG,
            anchor="w", wraplength=540, justify="left")
        self.lbl_status.pack(fill="x")

        # Boutons
        btn_row = tk.Frame(body, bg=C_BG)
        btn_row.pack(fill="x", pady=(12, 0))
        self.btn_check = tk.Button(
            btn_row, text="Verifier la version",
            font=("Segoe UI", 10, "bold"), fg="white", bg=C_ACCENT2,
            activeforeground="white", activebackground=C_ACCENT,
            relief="flat", cursor="hand2", padx=16, pady=6,
            command=self._start_check)
        self.btn_check.pack(side="left")

        self.btn_download = tk.Button(
            btn_row, text="Telecharger maintenant",
            font=("Segoe UI", 10), fg=C_ACCENT2, bg=C_PANEL,
            relief="flat", cursor="hand2", padx=16, pady=6,
            state="disabled", command=self._start_download)
        self.btn_download.pack(side="left", padx=10)

        tk.Button(btn_row, text="Fermer", font=("Segoe UI", 10), fg=C_SUBTLE, bg=C_PANEL,
                  relief="flat", cursor="hand2", padx=16, pady=6,
                  command=self.destroy).pack(side="right")

    def _browse_dest(self):
        path = filedialog.askopenfilename(
            title="Choisir le fichier GLEIF (ou son dossier)",
            filetypes=[("CSV", "*.csv"), ("Tous", "*.*")])
        if path:
            self.v_gleif_path.set(path)

    def _set_status(self, msg, color=C_SUBTLE):
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
                self.v_status.set(f"Telechargement : {mb_d:.1f} Mo / {mb_t:.1f} Mo  ({pct:.0f} %)")
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
        self._set_status("Connexion aux serveurs GLEIF...")
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
                    f"OK Nouvelle version disponible\n"
                    f"   Date    : {meta['publish_date'][:10]}\n"
                    f"   Taille  : {meta['size_human']}  (ZIP compresse)\n"
                    f"   Entites : {meta['record_count']:,}\n\n"
                    f"Cliquez sur « Telecharger maintenant » pour mettre a jour."
                )
                self.after(0, lambda: self.btn_download.config(state="normal"))
                self._set_status(msg, C_GREEN)
            else:
                self._set_status(
                    f"OK Base a jour — version du {meta['publish_date'][:10]}\n"
                    f"   {meta['record_count']:,} entites  ({meta['size_human']})",
                    C_ACCENT2)
        except Exception as e:
            self._set_status(
                f"ERREUR Impossible de contacter GLEIF.\n{e}\n\n"
                "Verifiez votre connexion ou la config proxy.", C_RED)
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
        self._set_status("Demarrage du telechargement...")
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

            self._set_status("Extraction du CSV...", C_SUBTLE)
            self.after(0, lambda: self.progress_bar.config(mode="indeterminate"))
            self.after(0, self.progress_bar.start)

            final_csv = extract_csv(zip_path, dest_dir)
            write_local_version(dest_dir, meta["publish_date"], final_csv.name)
            self.v_gleif_path.set(str(final_csv))

            self.after(0, lambda: [self.progress_bar.stop(),
                                   self.progress_bar.config(mode="determinate"),
                                   self.v_progress.set(100)])

            # Generation slim si demandee
            if self.v_prepare_slim.get():
                from gleif_matcher import prepare_slim
                slim_path = dest_dir / "gleif_slim.csv"
                self._set_status("Generation de la base slim en cours...", C_SUBTLE)
                prepare_slim(
                    str(final_csv), str(slim_path),
                    active_only=True,
                    progress_cb=self._set_slim_progress,
                    status_cb=lambda m: self._set_status(m, C_SUBTLE),
                )
                self.after(0, lambda: self.v_slim_progress.set(100))
                self.v_gleif_path.set(str(slim_path))
                self._set_status(
                    f"OK Mise a jour + slim termines — {meta['publish_date'][:10]}\n"
                    f"   Slim : {slim_path.name}", C_GREEN)
            else:
                self._set_status(
                    f"OK Mise a jour terminee — {meta['publish_date'][:10]}\n"
                    f"   Fichier : {final_csv.name}", C_GREEN)

        except Exception as e:
            self.after(0, lambda: [self.progress_bar.stop(),
                                   self.progress_bar.config(mode="determinate")])
            self._set_status(f"ERREUR : {e}", C_RED)
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
