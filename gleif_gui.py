"""
gleif_gui.py
============
Interface graphique pour le module GLEIF LEI Matcher.
Double-clic sur ce fichier (ou lancer via LANCER.bat) pour démarrer.

Dépendances : pip install pandas openpyxl rapidfuzz
"""

import os
import sys
import json
import threading
import subprocess
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path

# S'assurer que gleif_matcher.py est accessible depuis le même dossier
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

# Chemins des fichiers de configuration
TEAM_CONFIG_PATH = BASE_DIR / "gleif_config.json"        # config partagée (réseau)
USER_PREFS_PATH  = Path.home() / ".gleif_matcher_prefs.json"  # préférences locales utilisateur


def load_config() -> dict:
    """
    Charge la configuration dans l'ordre de priorité :
      1. Préférences utilisateur locales (~/.gleif_matcher_prefs.json)
      2. Config d'équipe partagée (gleif_config.json à côté du script)
      3. Valeurs par défaut codées en dur
    """
    defaults = {
        "gleif_path":       "",
        "col_rcs":          "RCS",
        "col_name":         "NomEntreprise",
        "col_pays":         "Pays",
        "fuzzy_threshold":  80,
        "active_only":      True,
        "last_input":       "",
        "last_output":      "",
    }
    # Fusionner config d'équipe par-dessus les défauts
    if TEAM_CONFIG_PATH.exists():
        try:
            with open(TEAM_CONFIG_PATH, encoding="utf-8") as f:
                team = {k: v for k, v in json.load(f).items()
                        if not k.startswith("_")}
            defaults.update(team)
        except Exception:
            pass
    # Fusionner préférences utilisateur par-dessus (priorité maximale)
    if USER_PREFS_PATH.exists():
        try:
            with open(USER_PREFS_PATH, encoding="utf-8") as f:
                defaults.update(json.load(f))
        except Exception:
            pass
    return defaults


def save_user_prefs(prefs: dict) -> None:
    """Sauvegarde les préférences utilisateur localement."""
    try:
        with open(USER_PREFS_PATH, "w", encoding="utf-8") as f:
            json.dump(prefs, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Palette couleurs
# ─────────────────────────────────────────────────────────────────────────────
C_BG        = "#F5F7FA"
C_PANEL     = "#FFFFFF"
C_ACCENT    = "#1F4E79"
C_ACCENT2   = "#2E75B6"
C_TEXT      = "#2C2C2C"
C_SUBTLE    = "#6B7280"
C_GREEN     = "#1E7E34"
C_YELLOW    = "#856404"
C_RED       = "#842029"
C_GREEN_BG  = "#D4EDDA"
C_YELLOW_BG = "#FFF3CD"
C_RED_BG    = "#F8D7DA"
C_BORDER    = "#D1D5DB"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers UI
# ─────────────────────────────────────────────────────────────────────────────

def _make_label(parent, text, bold=False, size=10, color=C_TEXT, **kw):
    font = ("Segoe UI", size, "bold" if bold else "normal")
    return tk.Label(parent, text=text, font=font, fg=color,
                    bg=parent["bg"] if hasattr(parent, "__getitem__") else C_PANEL, **kw)


def _make_entry(parent, textvariable, width=52):
    e = ttk.Entry(parent, textvariable=textvariable, width=width,
                  font=("Segoe UI", 10))
    return e


def _browse_file(var, title, filetypes, save=False):
    if save:
        path = filedialog.asksaveasfilename(
            title=title, filetypes=filetypes,
            defaultextension=".xlsx"
        )
    else:
        path = filedialog.askopenfilename(title=title, filetypes=filetypes)
    if path:
        var.set(path)


# ─────────────────────────────────────────────────────────────────────────────
# Fenêtre principale
# ─────────────────────────────────────────────────────────────────────────────

class GleifApp(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("GLEIF LEI Matcher")
        self.resizable(False, False)
        self.configure(bg=C_BG)
        self._center_window(780, 660)

        # Charger config (équipe + préférences utilisateur)
        cfg = load_config()

        # Variables — pré-remplies depuis la config
        self.v_input      = tk.StringVar(value=cfg.get("last_input", ""))
        self.v_gleif      = tk.StringVar(value=cfg.get("gleif_path", ""))
        self.v_output     = tk.StringVar(value=cfg.get("last_output", ""))
        self.v_col_rcs    = tk.StringVar(value=cfg.get("col_rcs",  "RCS"))
        self.v_col_name   = tk.StringVar(value=cfg.get("col_name", "NomEntreprise"))
        self.v_col_pays   = tk.StringVar(value=cfg.get("col_pays", "Pays"))
        self.v_threshold  = tk.IntVar(value=int(cfg.get("fuzzy_threshold", 80)))
        self.v_active     = tk.BooleanVar(value=bool(cfg.get("active_only", True)))
        # Proxy : priorité → préférence sauvegardée > auto-détection système
        _saved_proxy = cfg.get("proxy", None)   # None = jamais sauvegardé
        if _saved_proxy is None:
            from gleif_updater import detect_system_proxy
            _saved_proxy = detect_system_proxy() or ""
        self.v_proxy = tk.StringVar(value=_saved_proxy)
        self.v_progress   = tk.DoubleVar(value=0)
        self.v_status_msg = tk.StringVar(value="En attente…")

        self._build_ui()
        # Sauvegarder les préférences à la fermeture
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ── Layout ──────────────────────────────────────────────────────────────

    def _build_ui(self):
        pad = {"padx": 20, "pady": 8}

        # ── Titre ───────────────────────────────────────────────────────────
        header = tk.Frame(self, bg=C_ACCENT, height=56)
        header.pack(fill="x")
        tk.Label(
            header,
            text="  🔍  GLEIF LEI Matcher",
            font=("Segoe UI", 14, "bold"),
            fg="white", bg=C_ACCENT,
            anchor="w",
        ).pack(fill="x", padx=20, pady=12)

        # ── Corps ────────────────────────────────────────────────────────────
        body = tk.Frame(self, bg=C_BG)
        body.pack(fill="both", expand=True, padx=20, pady=12)

        # Fichiers
        self._section(body, "📂  Fichiers")
        self._file_row(body, "Fichier sociétés (.xlsx)",  self.v_input,
                       [("Excel", "*.xlsx")], save=False)

        # Ligne Base GLEIF + bouton mise à jour
        gleif_row = tk.Frame(body, bg=C_BG)
        gleif_row.pack(fill="x", pady=3)
        tk.Label(gleif_row, text="Base GLEIF (.csv ou .json)",
                 font=("Segoe UI", 10), fg=C_TEXT, bg=C_BG,
                 width=26, anchor="w").pack(side="left")
        ttk.Entry(gleif_row, textvariable=self.v_gleif, width=48,
                  font=("Segoe UI", 10)).pack(side="left", padx=(4, 4))
        tk.Button(
            gleif_row, text="…",
            font=("Segoe UI", 10), fg=C_ACCENT2,
            bg=C_PANEL, relief="flat", cursor="hand2", padx=8, pady=2,
            command=lambda: _browse_file(
                self.v_gleif, "Base GLEIF", [("CSV / JSON", "*.csv *.json")]
            ),
        ).pack(side="left")
        self.btn_update = tk.Button(
            gleif_row,
            text="🔄 Mettre à jour",
            font=("Segoe UI", 10), fg=C_ACCENT2,
            bg=C_PANEL, relief="flat", cursor="hand2", padx=10, pady=2,
            command=self._open_update_dialog,
        )
        self.btn_update.pack(side="left", padx=(8, 0))

        self._file_row(body, "Fichier de sortie (.xlsx)", self.v_output,
                       [("Excel", "*.xlsx")], save=True)

        ttk.Separator(body, orient="horizontal").pack(fill="x", pady=10)

        # Colonnes
        self._section(body, "🗂  Noms des colonnes  (fichier sociétés)")
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

        ttk.Separator(body, orient="horizontal").pack(fill="x", pady=10)

        # Options
        self._section(body, "⚙️  Options de correspondance")
        opt_frame = tk.Frame(body, bg=C_BG)
        opt_frame.pack(fill="x")

        # Seuil fuzzy
        thr_row = tk.Frame(opt_frame, bg=C_BG)
        thr_row.pack(fill="x", pady=4)
        tk.Label(thr_row, text="Seuil de similarité (fuzzy)",
                 font=("Segoe UI", 10), fg=C_TEXT, bg=C_BG, width=26, anchor="w"
                 ).pack(side="left")
        self._lbl_threshold = tk.Label(
            thr_row, text=f"{self.v_threshold.get()} %",
            font=("Segoe UI", 10, "bold"), fg=C_ACCENT2, bg=C_BG, width=6
        )
        self._lbl_threshold.pack(side="right")
        ttk.Scale(
            thr_row, from_=50, to=100, orient="horizontal",
            variable=self.v_threshold, length=280,
            command=lambda v: self._lbl_threshold.config(
                text=f"{int(float(v))} %"
            ),
        ).pack(side="left", padx=(4, 0))

        # Checkbox active only
        chk_row = tk.Frame(opt_frame, bg=C_BG)
        chk_row.pack(fill="x", pady=4)
        ttk.Checkbutton(
            chk_row,
            text="Inclure uniquement les LEI actifs  (Entity=ACTIVE & LEI=ISSUED)",
            variable=self.v_active,
        ).pack(side="left")

        ttk.Separator(body, orient="horizontal").pack(fill="x", pady=10)

        # Bouton lancer
        btn_frame = tk.Frame(body, bg=C_BG)
        btn_frame.pack(fill="x")
        self.btn_run = tk.Button(
            btn_frame,
            text="▶   Lancer le rapprochement",
            font=("Segoe UI", 11, "bold"),
            fg="white", bg=C_ACCENT2,
            activeforeground="white", activebackground=C_ACCENT,
            relief="flat", cursor="hand2", padx=24, pady=8,
            command=self._start_matching,
        )
        self.btn_run.pack(side="left")

        self.btn_open = tk.Button(
            btn_frame,
            text="📄  Ouvrir les résultats",
            font=("Segoe UI", 11),
            fg=C_ACCENT2, bg=C_PANEL,
            activeforeground=C_ACCENT, activebackground="#E8F0FA",
            relief="flat", cursor="hand2", padx=24, pady=8,
            command=self._open_result,
            state="disabled",
        )
        self.btn_open.pack(side="left", padx=12)

        # Barre de progression
        prog_frame = tk.Frame(body, bg=C_BG)
        prog_frame.pack(fill="x", pady=(12, 0))
        self.progress_bar = ttk.Progressbar(
            prog_frame, variable=self.v_progress,
            maximum=100, length=740, mode="determinate",
        )
        self.progress_bar.pack(fill="x")

        # Message de statut
        self.lbl_status = tk.Label(
            body, textvariable=self.v_status_msg,
            font=("Segoe UI", 9), fg=C_SUBTLE, bg=C_BG, anchor="w"
        )
        self.lbl_status.pack(fill="x", pady=(4, 0))

        # Résumé résultats
        self.frame_summary = tk.Frame(body, bg=C_BG)
        self.frame_summary.pack(fill="x", pady=(8, 0))

    def _section(self, parent, text):
        tk.Label(
            parent, text=text,
            font=("Segoe UI", 10, "bold"),
            fg=C_ACCENT, bg=C_BG, anchor="w",
        ).pack(fill="x", pady=(6, 2))

    def _file_row(self, parent, label, var, filetypes, save):
        row = tk.Frame(parent, bg=C_BG)
        row.pack(fill="x", pady=3)
        tk.Label(row, text=label, font=("Segoe UI", 10),
                 fg=C_TEXT, bg=C_BG, width=26, anchor="w").pack(side="left")
        ttk.Entry(row, textvariable=var, width=48,
                  font=("Segoe UI", 10)).pack(side="left", padx=(4, 4))
        tk.Button(
            row, text="…",
            font=("Segoe UI", 10), fg=C_ACCENT2,
            bg=C_PANEL, relief="flat", cursor="hand2",
            padx=8, pady=2,
            command=lambda: _browse_file(var, label, filetypes, save=save),
        ).pack(side="left")

    # ── Logique ──────────────────────────────────────────────────────────────

    def _validate(self):
        errors = []
        if not self.v_input.get().strip():
            errors.append("• Fichier sociétés manquant")
        elif not Path(self.v_input.get()).exists():
            errors.append("• Fichier sociétés introuvable")
        if not self.v_gleif.get().strip():
            errors.append("• Base GLEIF manquante")
        elif not Path(self.v_gleif.get()).exists():
            errors.append("• Base GLEIF introuvable")
        if not self.v_output.get().strip():
            errors.append("• Fichier de sortie non défini")
        for col_name, var in [
            ("Colonne RCS", self.v_col_rcs),
            ("Colonne Nom", self.v_col_name),
            ("Colonne Pays", self.v_col_pays),
        ]:
            if not var.get().strip():
                errors.append(f"• {col_name} vide")
        if errors:
            messagebox.showerror(
                "Champs manquants",
                "Veuillez corriger les points suivants :\n\n" + "\n".join(errors)
            )
            return False
        return True

    def _start_matching(self):
        if not self._validate():
            return

        # Vider le résumé précédent
        for w in self.frame_summary.winfo_children():
            w.destroy()

        self.btn_run.config(state="disabled", text="⏳  Traitement en cours…")
        self.btn_open.config(state="disabled")
        self.v_progress.set(0)
        self.v_status_msg.set("Initialisation…")

        thread = threading.Thread(target=self._run_matching, daemon=True)
        thread.start()

    def _run_matching(self):
        try:
            # Import ici pour éviter un délai au démarrage de l'UI
            import pandas as pd
            from gleif_matcher import load_gleif, build_indices, \
                search_by_rcs, search_by_name_country, \
                normalize_rcs, normalize_name, country_to_iso, \
                _export_excel, GLEIF_COLS

            self._set_status("Lecture du fichier sociétés…")

            df_input = pd.read_excel(self.v_input.get(), dtype=str).fillna("")
            n_total  = len(df_input)

            # Vérifier colonnes
            col_rcs  = self.v_col_rcs.get().strip()
            col_name = self.v_col_name.get().strip()
            col_pays = self.v_col_pays.get().strip()
            missing  = [c for c in [col_rcs, col_name, col_pays]
                        if c not in df_input.columns]
            if missing:
                self._show_error(
                    f"Colonnes introuvables dans le fichier sociétés :\n{missing}\n\n"
                    f"Colonnes disponibles : {list(df_input.columns)}"
                )
                return

            self._set_status("Chargement de la base GLEIF (peut prendre quelques minutes)…")
            df_gleif = load_gleif(self.v_gleif.get(), active_only=self.v_active.get())

            self._set_status("Construction des index de recherche…")
            rcs_index, name_index = build_indices(df_gleif)

            threshold = int(self.v_threshold.get())
            results   = []
            n_exact   = 0
            n_approx  = 0
            n_miss    = 0

            for idx, row in df_input.iterrows():
                rcs_norm  = normalize_rcs(str(row[col_rcs]))
                name_norm = normalize_name(str(row[col_name]))
                iso       = country_to_iso(str(row[col_pays]))

                gleif_row   = None
                match_type  = "Non trouvé"
                match_score = ""

                if rcs_norm:
                    gleif_row = search_by_rcs(rcs_norm, rcs_index, df_gleif)
                    if gleif_row is not None:
                        match_type  = "Exact – RCS"
                        match_score = 100
                        n_exact    += 1

                if gleif_row is None and name_norm:
                    gleif_row, score = search_by_name_country(
                        name_norm, iso, name_index, df_gleif, threshold
                    )
                    if gleif_row is not None:
                        match_type  = "Approx – Nom/Pays"
                        match_score = score
                        n_approx   += 1

                if gleif_row is None:
                    n_miss += 1

                if gleif_row is not None:
                    results.append({
                        "LEI":                     gleif_row["lei"],
                        "GLEIF_NomLegal":          gleif_row["name"],
                        "GLEIF_Pays":              gleif_row["country"],
                        "GLEIF_StatutSociete":     gleif_row["entity_status"],
                        "GLEIF_StatutLEI":         gleif_row["lei_status"],
                        "GLEIF_AutoriteRegistre":  gleif_row["ra_id"],
                        "GLEIF_NumRegistre":       gleif_row["ra_entity"],
                        "TypeCorrespondance":      match_type,
                        "ScoreCorrespondance":     match_score,
                    })
                else:
                    results.append({
                        "LEI": "", "GLEIF_NomLegal": "", "GLEIF_Pays": "",
                        "GLEIF_StatutSociete": "", "GLEIF_StatutLEI": "",
                        "GLEIF_AutoriteRegistre": "", "GLEIF_NumRegistre": "",
                        "TypeCorrespondance": match_type,
                        "ScoreCorrespondance": "",
                    })

                # Mise à jour UI (toutes les 10 lignes ou dernière)
                if (idx + 1) % 10 == 0 or (idx + 1) == n_total:
                    pct = ((idx + 1) / n_total) * 100
                    self._update_progress(
                        pct,
                        f"Traitement : {idx+1}/{n_total} lignes  —  "
                        f"✅ Exact: {n_exact}   🟡 Approx: {n_approx}   ❌ Non trouvé: {n_miss}"
                    )

            # Export
            self._set_status("Export Excel en cours…")
            df_results = pd.DataFrame(results)
            df_output  = pd.concat(
                [df_input.reset_index(drop=True), df_results], axis=1
            )
            _export_excel(df_output, self.v_output.get(), threshold)

            # Résumé final UI
            self.after(0, lambda: self._show_summary(n_total, n_exact, n_approx, n_miss))

        except Exception as exc:
            self._show_error(str(exc))

    def _set_status(self, msg):
        self.after(0, lambda: self.v_status_msg.set(msg))

    def _update_progress(self, pct, msg):
        def _update():
            self.v_progress.set(pct)
            self.v_status_msg.set(msg)
        self.after(0, _update)

    def _show_error(self, msg):
        def _show():
            messagebox.showerror("Erreur", msg)
            self.btn_run.config(state="normal", text="▶   Lancer le rapprochement")
            self.v_status_msg.set("❌  Une erreur s'est produite — voir le message ci-dessus.")
        self.after(0, _show)

    def _show_summary(self, n_total, n_exact, n_approx, n_miss):
        self.btn_run.config(state="normal", text="▶   Lancer le rapprochement")
        self.btn_open.config(state="normal")
        self.v_progress.set(100)
        self.v_status_msg.set("✅  Traitement terminé — fichier Excel généré.")
        self._save_prefs()   # mémoriser les chemins pour la prochaine session

        # Cartes résumé
        cards = [
            (f"{n_total}", "Total",          C_ACCENT,    "#E8F0FA"),
            (f"{n_exact}", "Exact – RCS",     C_GREEN,     C_GREEN_BG),
            (f"{n_approx}","Approx – Nom",   C_YELLOW,    C_YELLOW_BG),
            (f"{n_miss}",  "Non trouvé",      C_RED,       C_RED_BG),
        ]
        for val, label, fg, bg in cards:
            card = tk.Frame(self.frame_summary, bg=bg,
                            highlightbackground=C_BORDER,
                            highlightthickness=1)
            card.pack(side="left", padx=(0, 10), ipadx=14, ipady=8)
            tk.Label(card, text=val,   font=("Segoe UI", 20, "bold"),
                     fg=fg, bg=bg).pack()
            tk.Label(card, text=label, font=("Segoe UI", 9),
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
            messagebox.showwarning("Fichier introuvable",
                                   "Le fichier de sortie n'existe pas encore.")

    # ── Mise à jour de la base GLEIF ─────────────────────────────────────────

    def _open_update_dialog(self):
        """Ouvre la fenêtre de mise à jour de la base GLEIF."""
        UpdateDialog(self, self.v_gleif, self.v_proxy)

    # ── Persistance ──────────────────────────────────────────────────────────

    def _save_prefs(self):
        """Sauvegarde les préférences actuelles de l'utilisateur."""
        save_user_prefs({
            "last_input":      self.v_input.get(),
            "gleif_path":      self.v_gleif.get(),
            "last_output":     self.v_output.get(),
            "col_rcs":         self.v_col_rcs.get(),
            "col_name":        self.v_col_name.get(),
            "col_pays":        self.v_col_pays.get(),
            "fuzzy_threshold": int(self.v_threshold.get()),
            "active_only":     bool(self.v_active.get()),
            "proxy":           self.v_proxy.get(),
        })

    def _on_close(self):
        """Sauvegarder les préférences avant de fermer."""
        self._save_prefs()
        self.destroy()

    def _center_window(self, w, h):
        self.update_idletasks()
        sw = self.winfo_screenwidth()
        sh = self.winfo_screenheight()
        x  = (sw - w) // 2
        y  = (sh - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")


# ─────────────────────────────────────────────────────────────────────────────
# Fenêtre de mise à jour GLEIF
# ─────────────────────────────────────────────────────────────────────────────

class UpdateDialog(tk.Toplevel):
    """
    Fenêtre modale de mise à jour de la base GLEIF.
    Permet de vérifier la disponibilité d'une version plus récente,
    configurer le proxy, et lancer le téléchargement avec suivi de progression.
    """

    def __init__(self, parent, v_gleif_path: tk.StringVar, v_proxy: tk.StringVar):
        super().__init__(parent)
        self.title("Mise à jour de la base GLEIF")
        self.resizable(False, False)
        self.configure(bg=C_BG)
        self.transient(parent)
        self.grab_set()

        self.v_gleif_path = v_gleif_path
        self.v_proxy      = v_proxy
        self.v_progress   = tk.DoubleVar(value=0)
        self.v_status     = tk.StringVar(value="Prêt.")
        self._running     = False

        self._build_ui()
        self._center(parent, 560, 380)

    def _build_ui(self):
        # En-tête
        hdr = tk.Frame(self, bg=C_ACCENT2)
        hdr.pack(fill="x")
        tk.Label(hdr, text="  🔄  Mise à jour de la base GLEIF",
                 font=("Segoe UI", 12, "bold"), fg="white", bg=C_ACCENT2,
                 anchor="w").pack(fill="x", padx=16, pady=10)

        body = tk.Frame(self, bg=C_BG)
        body.pack(fill="both", expand=True, padx=20, pady=12)

        # Info
        tk.Label(
            body,
            text=(
                "Le fichier Golden Copy GLEIF est mis à jour quotidiennement.\n"
                "La vérification compare votre version locale avec la dernière\n"
                "disponible sur les serveurs GLEIF (~450 Mo compressé)."
            ),
            font=("Segoe UI", 9), fg=C_SUBTLE, bg=C_BG, justify="left",
        ).pack(anchor="w", pady=(0, 10))

        # Dossier de destination
        dest_row = tk.Frame(body, bg=C_BG)
        dest_row.pack(fill="x", pady=4)
        tk.Label(dest_row, text="Dossier de destination :",
                 font=("Segoe UI", 10), fg=C_TEXT, bg=C_BG, width=22, anchor="w"
                 ).pack(side="left")
        ttk.Entry(dest_row, textvariable=self.v_gleif_path, width=36,
                  font=("Segoe UI", 10)).pack(side="left", padx=4)
        tk.Button(dest_row, text="…", font=("Segoe UI", 10), fg=C_ACCENT2,
                  bg=C_PANEL, relief="flat", cursor="hand2", padx=6,
                  command=self._browse_dest).pack(side="left")

        # Proxy
        proxy_row = tk.Frame(body, bg=C_BG)
        proxy_row.pack(fill="x", pady=4)
        tk.Label(proxy_row, text="Proxy HTTP(S) :",
                 font=("Segoe UI", 10), fg=C_TEXT, bg=C_BG, width=22, anchor="w"
                 ).pack(side="left")
        ttk.Entry(proxy_row, textvariable=self.v_proxy, width=36,
                  font=("Segoe UI", 10)).pack(side="left", padx=4)
        # Indicateur : proxy détecté / PAC / direct
        _proxy_val = self.v_proxy.get()
        if _proxy_val:
            _proxy_hint  = "✔ détecté automatiquement"
            _proxy_color = C_GREEN
        else:
            # Vérifier si un PAC est configuré (Windows)
            try:
                import winreg
                _reg = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"
                with winreg.OpenKey(winreg.HKEY_CURRENT_USER, _reg) as _k:
                    _pac, _ = winreg.QueryValueEx(_k, "AutoConfigURL")
                _proxy_hint  = "✔ PAC détecté — résolution automatique"
                _proxy_color = C_GREEN
            except Exception:
                _proxy_hint  = "vide = connexion directe"
                _proxy_color = C_SUBTLE
        tk.Label(proxy_row, text=_proxy_hint,
                 font=("Segoe UI", 8, "italic"), fg=_proxy_color, bg=C_BG
                 ).pack(side="left", padx=4)

        # Barre de progression
        self.progress_bar = ttk.Progressbar(
            body, variable=self.v_progress, maximum=100,
            length=520, mode="determinate",
        )
        self.progress_bar.pack(fill="x", pady=(12, 4))

        # Statut
        self.lbl_status = tk.Label(
            body, textvariable=self.v_status,
            font=("Segoe UI", 9), fg=C_SUBTLE, bg=C_BG, anchor="w",
            wraplength=520, justify="left",
        )
        self.lbl_status.pack(fill="x")

        # Boutons
        btn_row = tk.Frame(body, bg=C_BG)
        btn_row.pack(fill="x", pady=(14, 0))

        self.btn_check = tk.Button(
            btn_row, text="🔍  Vérifier la version",
            font=("Segoe UI", 10, "bold"), fg="white", bg=C_ACCENT2,
            activeforeground="white", activebackground=C_ACCENT,
            relief="flat", cursor="hand2", padx=16, pady=6,
            command=self._start_check,
        )
        self.btn_check.pack(side="left")

        self.btn_download = tk.Button(
            btn_row, text="⬇️  Télécharger maintenant",
            font=("Segoe UI", 10), fg=C_ACCENT2, bg=C_PANEL,
            relief="flat", cursor="hand2", padx=16, pady=6,
            state="disabled", command=self._start_download,
        )
        self.btn_download.pack(side="left", padx=10)

        tk.Button(
            btn_row, text="Fermer",
            font=("Segoe UI", 10), fg=C_SUBTLE, bg=C_PANEL,
            relief="flat", cursor="hand2", padx=16, pady=6,
            command=self.destroy,
        ).pack(side="right")

        self._meta = None   # métadonnées récupérées lors de la vérification

    def _browse_dest(self):
        path = filedialog.askopenfilename(
            title="Choisir le fichier GLEIF existant (ou son dossier)",
            filetypes=[("CSV / JSON", "*.csv *.json"), ("Tous", "*.*")],
        )
        if path:
            self.v_gleif_path.set(path)

    # ── Logique ──────────────────────────────────────────────────────────────

    def _set_status(self, msg, color=C_SUBTLE):
        self.after(0, lambda: [
            self.v_status.set(msg),
            self.lbl_status.config(fg=color),
        ])

    def _set_progress(self, done, total):
        if total > 0:
            pct = min(done / total * 100, 100)
            mb_done  = done  / 1_048_576
            mb_total = total / 1_048_576
            self.after(0, lambda: [
                self.v_progress.set(pct),
                self.v_status.set(
                    f"Téléchargement : {mb_done:.1f} Mo / {mb_total:.1f} Mo  ({pct:.0f} %)"
                ),
            ])

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
            from pathlib import Path as _Path
            dest_dir   = _Path(gleif_path).parent if gleif_path else _Path.cwd()
            local_date = read_local_version(dest_dir)

            if is_update_available(local_date, meta["publish_date"]):
                msg = (
                    f"✅  Nouvelle version disponible !\n"
                    f"   Date GLEIF : {meta['publish_date'][:10]}\n"
                    f"   Taille     : {meta['size_human']}  (compressé ZIP)\n"
                    f"   Entités    : {meta['record_count']:,}\n\n"
                    f"Cliquez sur « Télécharger maintenant » pour mettre à jour."
                )
                self.after(0, lambda: self.btn_download.config(state="normal"))
                self._set_status(msg, C_GREEN)
            else:
                msg = (
                    f"✔  Base déjà à jour — version du {meta['publish_date'][:10]}.\n"
                    f"   {meta['record_count']:,} entités  ({meta['size_human']})"
                )
                self._set_status(msg, C_ACCENT2)

        except Exception as e:
            self._set_status(
                f"❌  Impossible de contacter GLEIF.\n{e}\n\n"
                "Vérifiez votre connexion ou renseignez l'adresse du proxy.",
                C_RED,
            )
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
        self._set_status("Démarrage du téléchargement…")
        threading.Thread(target=self._do_download, daemon=True).start()

    def _do_download(self):
        from gleif_updater import download_gleif, extract_csv, write_local_version
        from pathlib import Path as _Path
        try:
            meta      = self._meta
            proxy     = self.v_proxy.get().strip() or None
            gleif_str = self.v_gleif_path.get().strip()
            dest_dir  = _Path(gleif_str).parent if gleif_str else _Path.cwd()

            zip_path = download_gleif(
                url=meta["download_url"],
                dest_dir=dest_dir,
                total_bytes=meta["size_bytes"],
                progress_cb=self._set_progress,
                proxy=proxy,
            )

            self._set_status("Extraction du fichier CSV…", C_SUBTLE)
            self.after(0, lambda: self.progress_bar.config(mode="indeterminate"))
            self.after(0, self.progress_bar.start)

            final_csv = extract_csv(zip_path, dest_dir)
            write_local_version(dest_dir, meta["publish_date"], final_csv.name)

            # Mettre à jour le champ GLEIF dans la fenêtre principale
            self.v_gleif_path.set(str(final_csv))

            self.after(0, lambda: [
                self.progress_bar.stop(),
                self.progress_bar.config(mode="determinate"),
                self.v_progress.set(100),
            ])
            self._set_status(
                f"✅  Mise à jour terminée — version du {meta['publish_date'][:10]}\n"
                f"   Fichier : {final_csv.name}",
                C_GREEN,
            )

        except Exception as e:
            self.after(0, lambda: [
                self.progress_bar.stop(),
                self.progress_bar.config(mode="determinate"),
            ])
            self._set_status(f"❌  Erreur lors du téléchargement :\n{e}", C_RED)
        finally:
            self._running = False
            self.after(0, lambda: self.btn_check.config(state="normal"))

    def _center(self, parent, w, h):
        parent.update_idletasks()
        px, py = parent.winfo_rootx(), parent.winfo_rooty()
        pw, ph = parent.winfo_width(), parent.winfo_height()
        x = px + (pw - w) // 2
        y = py + (ph - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = GleifApp()
    app.mainloop()
