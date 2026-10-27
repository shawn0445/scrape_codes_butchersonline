#!/usr/bin/env python3
"""Desktop GUI for the Canada butcher Google Maps scraper."""

from __future__ import annotations

import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any

from cities import TARGET_CITIES
from scraper import (
    BASE_DIR,
    DEFAULT_BROWSER,
    DEFAULT_MAX_PER_QUERY,
    DEFAULT_OUTPUT,
    DEFAULT_WORKERS,
    PROGRESS_FILE,
    SUPPORTED_BROWSERS,
    ScrapeController,
    clear_all_progress,
    clear_completed_cities,
    clear_failed_urls,
    clear_progress_shops,
    count_shops_needing_enrichment,
    enrich_shops_csv,
    export_progress_shops_to_csv,
    get_progress_summary,
    scrape_google_maps,
)

PARENT_FILES = BASE_DIR.parent / "files"


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "--:--"
    seconds = max(int(seconds), 0)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


class ScraperGUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Canada Butcher Scraper")
        self.root.minsize(780, 640)
        self.root.geometry("880x720")

        self.controller = ScrapeController()
        self.worker: threading.Thread | None = None
        self.event_queue: queue.Queue = queue.Queue()
        self.is_running = False
        self.is_paused = False
        self._tick_job: str | None = None
        self._started_at: float | None = None
        self._paused_at: float | None = None
        self._paused_total = 0.0
        self._last_progress: dict[str, Any] = {}

        self._build_style()
        self._build_ui()
        self._set_idle_controls()
        self.root.after(100, self._poll_queue)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_style(self) -> None:
        self.colors = {
            "bg": "#1f2933",
            "panel": "#24303a",
            "card": "#2b3844",
            "text": "#e8eef2",
            "muted": "#9aa7b2",
            "accent": "#2f8f7a",
            "accent_hover": "#247864",
            "warn": "#c9852c",
            "danger": "#b54848",
            "ok": "#3f9d6a",
            "input": "#172027",
            "border": "#3a4753",
        }
        self.root.configure(bg=self.colors["bg"])

        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure("Root.TFrame", background=self.colors["bg"])
        style.configure("Panel.TFrame", background=self.colors["panel"])
        style.configure("Card.TFrame", background=self.colors["card"])
        style.configure(
            "Title.TLabel",
            background=self.colors["bg"],
            foreground=self.colors["text"],
            font=("Segoe UI Semibold", 18),
        )
        style.configure(
            "Sub.TLabel",
            background=self.colors["bg"],
            foreground=self.colors["muted"],
            font=("Segoe UI", 10),
        )
        style.configure(
            "Label.TLabel",
            background=self.colors["panel"],
            foreground=self.colors["text"],
            font=("Segoe UI", 10),
        )
        style.configure(
            "Muted.TLabel",
            background=self.colors["panel"],
            foreground=self.colors["muted"],
            font=("Segoe UI", 9),
        )
        style.configure(
            "StatValue.TLabel",
            background=self.colors["card"],
            foreground=self.colors["text"],
            font=("Segoe UI Semibold", 16),
        )
        style.configure(
            "StatTitle.TLabel",
            background=self.colors["card"],
            foreground=self.colors["muted"],
            font=("Segoe UI", 9),
        )
        style.configure(
            "Status.TLabel",
            background=self.colors["panel"],
            foreground=self.colors["accent"],
            font=("Segoe UI Semibold", 11),
        )
        style.configure(
            "TCheckbutton",
            background=self.colors["panel"],
            foreground=self.colors["text"],
            font=("Segoe UI", 10),
        )
        style.map(
            "TCheckbutton",
            background=[("active", self.colors["panel"])],
            foreground=[("disabled", self.colors["muted"])],
        )
        style.configure(
            "Accent.TButton",
            background=self.colors["accent"],
            foreground="#ffffff",
            font=("Segoe UI Semibold", 10),
            padding=(14, 8),
            borderwidth=0,
        )
        style.map(
            "Accent.TButton",
            background=[
                ("active", self.colors["accent_hover"]),
                ("disabled", "#4a5a63"),
            ],
            foreground=[("disabled", "#c5ced4")],
        )
        style.configure(
            "Warn.TButton",
            background=self.colors["warn"],
            foreground="#ffffff",
            font=("Segoe UI Semibold", 10),
            padding=(14, 8),
            borderwidth=0,
        )
        style.map(
            "Warn.TButton",
            background=[("active", "#a86f20"), ("disabled", "#4a5a63")],
            foreground=[("disabled", "#c5ced4")],
        )
        style.configure(
            "Danger.TButton",
            background=self.colors["danger"],
            foreground="#ffffff",
            font=("Segoe UI Semibold", 10),
            padding=(14, 8),
            borderwidth=0,
        )
        style.map(
            "Danger.TButton",
            background=[("active", "#943c3c"), ("disabled", "#4a5a63")],
            foreground=[("disabled", "#c5ced4")],
        )
        style.configure(
            "Ghost.TButton",
            background=self.colors["card"],
            foreground=self.colors["text"],
            font=("Segoe UI", 10),
            padding=(12, 7),
            borderwidth=0,
        )
        style.map(
            "Ghost.TButton",
            background=[("active", self.colors["border"]), ("disabled", "#4a5a63")],
        )
        style.configure(
            "Horizontal.TProgressbar",
            troughcolor=self.colors["input"],
            background=self.colors["accent"],
            bordercolor=self.colors["border"],
            lightcolor=self.colors["accent"],
            darkcolor=self.colors["accent"],
            thickness=18,
        )
        style.configure(
            "TEntry",
            fieldbackground=self.colors["input"],
            foreground=self.colors["text"],
            insertcolor=self.colors["text"],
            bordercolor=self.colors["border"],
            lightcolor=self.colors["border"],
            darkcolor=self.colors["border"],
            padding=6,
        )
        style.configure(
            "TSpinbox",
            fieldbackground=self.colors["input"],
            foreground=self.colors["text"],
            insertcolor=self.colors["text"],
            arrowsize=12,
            bordercolor=self.colors["border"],
            lightcolor=self.colors["border"],
            darkcolor=self.colors["border"],
            padding=4,
        )

    def _build_ui(self) -> None:
        outer = ttk.Frame(self.root, style="Root.TFrame", padding=18)
        outer.pack(fill="both", expand=True)

        ttk.Label(outer, text="Canada Butcher Scraper", style="Title.TLabel").pack(
            anchor="w"
        )
        ttk.Label(
            outer,
            text="Playwright Google Maps discovery with live progress, ETA, and run controls.",
            style="Sub.TLabel",
        ).pack(anchor="w", pady=(2, 14))

        settings = ttk.Frame(outer, style="Panel.TFrame", padding=14)
        settings.pack(fill="x")

        self.output_var = tk.StringVar(value=str(PARENT_FILES / "01_shops_raw.csv"))
        self.existing_var = tk.StringVar(value=str(PARENT_FILES / "01_shops_raw.csv"))
        self.limit_var = tk.StringVar(value="")
        self.max_per_query_var = tk.StringVar(value=str(DEFAULT_MAX_PER_QUERY))
        self.workers_var = tk.StringVar(value=str(DEFAULT_WORKERS))
        self.headed_var = tk.BooleanVar(value=False)
        self.use_existing_var = tk.BooleanVar(value=True)
        self.auto_enrich_var = tk.BooleanVar(value=True)
        self.browser_var = tk.StringVar(value=DEFAULT_BROWSER)

        self._row_path(settings, "Output CSV", self.output_var, 0, save=True)
        self._row_path(
            settings,
            "Existing CSV (incremental)",
            self.existing_var,
            1,
            save=False,
            optional=True,
        )

        opts = ttk.Frame(settings, style="Panel.TFrame")
        opts.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        opts.columnconfigure(1, weight=1)
        opts.columnconfigure(3, weight=1)
        opts.columnconfigure(5, weight=1)

        ttk.Label(opts, text="Shop limit (blank = all)", style="Label.TLabel").grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Entry(opts, textvariable=self.limit_var, width=12).grid(
            row=0, column=1, sticky="w"
        )
        ttk.Label(opts, text="Max per query", style="Label.TLabel").grid(
            row=0, column=2, sticky="w", padx=(18, 8)
        )
        ttk.Spinbox(
            opts,
            from_=1,
            to=100,
            textvariable=self.max_per_query_var,
            width=8,
        ).grid(row=0, column=3, sticky="w")
        ttk.Label(opts, text="Workers", style="Label.TLabel").grid(
            row=0, column=4, sticky="w", padx=(18, 8)
        )
        ttk.Spinbox(
            opts,
            from_=1,
            to=8,
            textvariable=self.workers_var,
            width=6,
        ).grid(row=0, column=5, sticky="w")

        ttk.Checkbutton(
            opts,
            text="Show browser (headed)",
            variable=self.headed_var,
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(10, 0))
        ttk.Checkbutton(
            opts,
            text="Use existing CSV for incremental skip",
            variable=self.use_existing_var,
        ).grid(row=1, column=2, columnspan=2, sticky="w", pady=(10, 0))
        ttk.Label(opts, text="Browser", style="Label.TLabel").grid(
            row=1, column=4, sticky="w", padx=(18, 8), pady=(10, 0)
        )
        ttk.Combobox(
            opts,
            textvariable=self.browser_var,
            values=list(SUPPORTED_BROWSERS),
            state="readonly",
            width=10,
        ).grid(row=1, column=5, sticky="w", pady=(10, 0))
        ttk.Checkbutton(
            opts,
            text="Auto-refill missing Phone/Website/Postal after scrape",
            variable=self.auto_enrich_var,
        ).grid(row=2, column=0, columnspan=6, sticky="w", pady=(10, 0))
        ttk.Label(
            opts,
            text="Tip: Workers apply to scrape AND refill. Use Chromium + 3–4 workers (headed off).",
            style="Muted.TLabel",
        ).grid(row=3, column=0, columnspan=6, sticky="w", pady=(8, 0))

        maintenance = ttk.Frame(outer, style="Panel.TFrame", padding=14)
        maintenance.pack(fill="x", pady=(10, 0))
        ttk.Label(maintenance, text="Progress tools", style="Label.TLabel").pack(
            anchor="w"
        )
        self.progress_summary_var = tk.StringVar(value="")
        ttk.Label(
            maintenance,
            textvariable=self.progress_summary_var,
            style="Muted.TLabel",
        ).pack(anchor="w", pady=(2, 8))

        maint_btns = ttk.Frame(maintenance, style="Panel.TFrame")
        maint_btns.pack(fill="x")
        maint_row1 = ttk.Frame(maint_btns, style="Panel.TFrame")
        maint_row1.pack(fill="x")
        maint_row2 = ttk.Frame(maint_btns, style="Panel.TFrame")
        maint_row2.pack(fill="x", pady=(6, 0))

        self.refresh_progress_btn = ttk.Button(
            maint_row1,
            text="Refresh status",
            style="Ghost.TButton",
            command=self.refresh_progress_status,
        )
        self.clear_cities_btn = ttk.Button(
            maint_row1,
            text="Clear completed cities",
            style="Warn.TButton",
            command=self.clear_completed_cities_action,
        )
        self.clear_shops_btn = ttk.Button(
            maint_row1,
            text="Clear saved shops",
            style="Warn.TButton",
            command=self.clear_progress_shops_action,
        )
        self.clear_all_btn = ttk.Button(
            maint_row1,
            text="Clear all progress",
            style="Danger.TButton",
            command=self.clear_all_progress_action,
        )
        self.export_progress_btn = ttk.Button(
            maint_row2,
            text="Export progress → CSV",
            style="Ghost.TButton",
            command=self.export_progress_action,
        )
        self.open_progress_btn = ttk.Button(
            maint_row2,
            text="Open progress file",
            style="Ghost.TButton",
            command=self.open_progress_file,
        )
        self.clear_failed_btn = ttk.Button(
            maint_row2,
            text="Clear failed URLs",
            style="Warn.TButton",
            command=self.clear_failed_urls_action,
        )
        self.clear_log_btn = ttk.Button(
            maint_row2,
            text="Clear log",
            style="Ghost.TButton",
            command=self.clear_activity_log,
        )
        self.enrich_btn = ttk.Button(
            maint_row2,
            text="Refill missing fields",
            style="Accent.TButton",
            command=self.start_enrichment,
        )

        for btn in (
            self.refresh_progress_btn,
            self.clear_cities_btn,
            self.clear_shops_btn,
            self.clear_all_btn,
        ):
            btn.pack(side="left", padx=(0, 8))
        for btn in (
            self.export_progress_btn,
            self.open_progress_btn,
            self.clear_failed_btn,
            self.enrich_btn,
            self.clear_log_btn,
        ):
            btn.pack(side="left", padx=(0, 8))

        controls = ttk.Frame(outer, style="Root.TFrame")
        controls.pack(fill="x", pady=(14, 8))

        self.start_btn = ttk.Button(
            controls, text="Start", style="Accent.TButton", command=self.start_scrape
        )
        self.pause_btn = ttk.Button(
            controls, text="Pause", style="Warn.TButton", command=self.pause_scrape
        )
        self.resume_btn = ttk.Button(
            controls, text="Resume", style="Accent.TButton", command=self.resume_scrape
        )
        self.stop_btn = ttk.Button(
            controls, text="Stop", style="Danger.TButton", command=self.stop_scrape
        )
        self.open_btn = ttk.Button(
            controls,
            text="Open output folder",
            style="Ghost.TButton",
            command=self.open_output_folder,
        )

        for btn in (
            self.start_btn,
            self.pause_btn,
            self.resume_btn,
            self.stop_btn,
            self.open_btn,
        ):
            btn.pack(side="left", padx=(0, 8))

        progress_panel = ttk.Frame(outer, style="Panel.TFrame", padding=14)
        progress_panel.pack(fill="x", pady=(6, 10))

        header = ttk.Frame(progress_panel, style="Panel.TFrame")
        header.pack(fill="x")
        self.status_var = tk.StringVar(value="Idle")
        self.percent_var = tk.StringVar(value="0.0%")
        ttk.Label(header, textvariable=self.status_var, style="Status.TLabel").pack(
            side="left"
        )
        ttk.Label(header, textvariable=self.percent_var, style="Status.TLabel").pack(
            side="right"
        )

        self.progress = ttk.Progressbar(
            progress_panel,
            orient="horizontal",
            mode="determinate",
            maximum=100,
            value=0,
        )
        self.progress.pack(fill="x", pady=(10, 12))

        stats = ttk.Frame(progress_panel, style="Panel.TFrame")
        stats.pack(fill="x")
        for i in range(5):
            stats.columnconfigure(i, weight=1)

        self.stat_shops = self._stat_card(stats, 0, "Shops found", "0")
        self.stat_cities = self._stat_card(
            stats, 1, "Cities", f"0 / {len(TARGET_CITIES)}"
        )
        self.stat_elapsed = self._stat_card(stats, 2, "Elapsed", "00:00")
        self.stat_eta = self._stat_card(stats, 3, "ETA", "--:--")
        self.stat_rate = self._stat_card(stats, 4, "Rate", "-- shops/hr")

        detail = ttk.Frame(progress_panel, style="Panel.TFrame")
        detail.pack(fill="x", pady=(12, 0))
        self.city_var = tk.StringVar(value="City: —")
        self.query_var = tk.StringVar(value="Query: —")
        ttk.Label(detail, textvariable=self.city_var, style="Muted.TLabel").pack(
            anchor="w"
        )
        ttk.Label(detail, textvariable=self.query_var, style="Muted.TLabel").pack(
            anchor="w", pady=(2, 0)
        )

        log_frame = ttk.Frame(outer, style="Panel.TFrame", padding=14)
        log_frame.pack(fill="both", expand=True)
        ttk.Label(log_frame, text="Activity log", style="Label.TLabel").pack(anchor="w")

        self.log = tk.Text(
            log_frame,
            height=12,
            wrap="word",
            bg=self.colors["input"],
            fg=self.colors["text"],
            insertbackground=self.colors["text"],
            relief="flat",
            font=("Consolas", 10),
            padx=10,
            pady=10,
        )
        self.log.pack(fill="both", expand=True, pady=(8, 0))
        self.log.configure(state="disabled")

        self.refresh_progress_status()
        self._log("Ready. Configure options, then press Start.")

    def _row_path(
        self,
        parent: ttk.Frame,
        label: str,
        variable: tk.StringVar,
        row: int,
        *,
        save: bool,
        optional: bool = False,
    ) -> None:
        ttk.Label(parent, text=label, style="Label.TLabel").grid(
            row=row, column=0, sticky="w", pady=4
        )
        entry = ttk.Entry(parent, textvariable=variable)
        entry.grid(row=row, column=1, sticky="ew", padx=8, pady=4)
        parent.columnconfigure(1, weight=1)
        ttk.Button(
            parent,
            text="Browse",
            style="Ghost.TButton",
            command=lambda: self._browse(variable, save=save),
        ).grid(row=row, column=2, sticky="e", pady=4)
        if optional:
            pass

    def _stat_card(
        self, parent: ttk.Frame, column: int, title: str, value: str
    ) -> tk.StringVar:
        card = ttk.Frame(parent, style="Card.TFrame", padding=10)
        card.grid(row=0, column=column, sticky="nsew", padx=(0 if column == 0 else 6, 0))
        var = tk.StringVar(value=value)
        ttk.Label(card, text=title, style="StatTitle.TLabel").pack(anchor="w")
        ttk.Label(card, textvariable=var, style="StatValue.TLabel").pack(anchor="w")
        return var

    def _browse(self, variable: tk.StringVar, *, save: bool) -> None:
        if save:
            path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                initialfile=Path(variable.get() or "shops.csv").name,
            )
        else:
            path = filedialog.askopenfilename(
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
            )
        if path:
            variable.set(path)

    def _set_idle_controls(self) -> None:
        self.start_btn.configure(state="normal")
        self.pause_btn.configure(state="disabled")
        self.resume_btn.configure(state="disabled")
        self.stop_btn.configure(state="disabled")
        self._set_maintenance_controls(enabled=True)

    def _set_running_controls(self) -> None:
        self.start_btn.configure(state="disabled")
        self.pause_btn.configure(state="normal")
        self.resume_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self._set_maintenance_controls(enabled=False)

    def _set_paused_controls(self) -> None:
        self.start_btn.configure(state="disabled")
        self.pause_btn.configure(state="disabled")
        self.resume_btn.configure(state="normal")
        self.stop_btn.configure(state="normal")
        self._set_maintenance_controls(enabled=False)

    def _set_maintenance_controls(self, *, enabled: bool) -> None:
        # Always allow status refresh + log clear; block destructive edits while running.
        self.refresh_progress_btn.configure(state="normal")
        self.clear_log_btn.configure(state="normal")
        self.open_progress_btn.configure(state="normal")
        state = "normal" if enabled else "disabled"
        for btn in (
            self.clear_cities_btn,
            self.clear_shops_btn,
            self.clear_all_btn,
            self.clear_failed_btn,
            self.export_progress_btn,
            self.enrich_btn,
        ):
            btn.configure(state=state)

    def _log(self, message: str) -> None:
        stamp = time.strftime("%H:%M:%S")
        self.log.configure(state="normal")
        self.log.insert("end", f"[{stamp}] {message}\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    def _active_elapsed(self) -> float:
        if self._started_at is None:
            return 0.0
        now = time.time()
        paused_extra = 0.0
        if self.is_paused and self._paused_at is not None:
            paused_extra = now - self._paused_at
        return max(now - self._started_at - self._paused_total - paused_extra, 0.0)

    def start_scrape(self) -> None:
        if self.is_running:
            return

        output = Path(self.output_var.get().strip() or DEFAULT_OUTPUT)
        existing = None
        if self.use_existing_var.get():
            existing_text = self.existing_var.get().strip()
            if existing_text:
                existing = Path(existing_text)

        limit_text = self.limit_var.get().strip()
        limit = None
        if limit_text:
            try:
                limit = int(limit_text)
                if limit <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showerror("Invalid limit", "Shop limit must be a positive integer.")
                return

        try:
            max_per_query = int(self.max_per_query_var.get().strip())
            if max_per_query <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror(
                "Invalid max per query", "Max per query must be a positive integer."
            )
            return

        try:
            workers = int(self.workers_var.get().strip())
            if workers < 1 or workers > 8:
                raise ValueError
        except ValueError:
            messagebox.showerror(
                "Invalid workers", "Workers must be an integer from 1 to 8."
            )
            return

        browser = self.browser_var.get().strip().lower()
        if browser not in SUPPORTED_BROWSERS:
            messagebox.showerror(
                "Invalid browser",
                f"Browser must be one of: {', '.join(SUPPORTED_BROWSERS)}",
            )
            return

        self.controller = ScrapeController()
        self.is_running = True
        self.is_paused = False
        self._started_at = time.time()
        self._paused_at = None
        self._paused_total = 0.0
        self._last_progress = {}
        self._set_running_controls()
        self.status_var.set("Running")
        self.progress.configure(value=0)
        self.percent_var.set("0.0%")
        self.stat_shops.set("0")
        self.stat_cities.set(f"0 / {len(TARGET_CITIES)}")
        self.stat_elapsed.set("00:00")
        self.stat_eta.set("--:--")
        self.stat_rate.set("-- shops/hr")
        self.city_var.set("City: —")
        self.query_var.set("Query: —")
        self._log(
            f"Starting scrape -> {output} | limit={limit or 'all'} | "
            f"max_per_query={max_per_query} | workers={workers} | "
            f"browser={browser} | headed={self.headed_var.get()} | "
            f"auto_enrich={self.auto_enrich_var.get()}"
        )
        self._start_ticker()

        def worker() -> None:
            try:
                result = scrape_google_maps(
                    output=output,
                    existing_csv=existing,
                    limit=limit,
                    max_per_query=max_per_query,
                    headed=self.headed_var.get(),
                    workers=workers,
                    browser=browser,
                    controller=self.controller,
                    on_progress=lambda payload: self.event_queue.put(
                        ("progress", payload)
                    ),
                    reset_state=True,
                    enrich_missing=self.auto_enrich_var.get(),
                    enrich_limit=limit,
                )
                self.event_queue.put(("done", result))
            except Exception as err:
                self.event_queue.put(("error", str(err)))

        self.worker = threading.Thread(target=worker, daemon=True)
        self.worker.start()

    def start_enrichment(self) -> None:
        if self.is_running:
            return

        output = Path(self.output_var.get().strip() or DEFAULT_OUTPUT)
        if not output.exists():
            messagebox.showerror(
                "CSV not found",
                f"Output CSV does not exist yet:\n{output}",
            )
            return

        summary = count_shops_needing_enrichment(output)
        need = int(summary.get("needs_enrichment") or 0)
        total = int(summary.get("total") or 0)
        if need <= 0:
            messagebox.showinfo(
                "Nothing to refill",
                f"All {total} shops already have Phone/Website/Postal/Address filled "
                "(or no refillable gaps were found).",
            )
            return

        limit_text = self.limit_var.get().strip()
        limit = None
        if limit_text:
            try:
                limit = int(limit_text)
                if limit <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showerror(
                    "Invalid limit", "Shop limit must be a positive integer."
                )
                return

        browser = self.browser_var.get().strip().lower()
        if browser not in SUPPORTED_BROWSERS:
            messagebox.showerror(
                "Invalid browser",
                f"Browser must be one of: {', '.join(SUPPORTED_BROWSERS)}",
            )
            return

        try:
            workers = int(self.workers_var.get().strip())
            if workers < 1 or workers > 8:
                raise ValueError
        except ValueError:
            messagebox.showerror(
                "Invalid workers", "Workers must be an integer from 1 to 8."
            )
            return

        scope = f"first {limit} needing refill" if limit else f"all {need} needing refill"
        ok = messagebox.askyesno(
            "Refill missing fields",
            f"CSV: {output}\n"
            f"Shops: {total}\n"
            f"Need refill: {need}\n"
            f"Will process: {scope}\n"
            f"Workers: {workers}\n\n"
            "This reopens Google Maps for each incomplete shop to fill "
            "Phone, Website, Postal, Address, Rating where missing.\n\n"
            "Continue?",
        )
        if not ok:
            return

        self.controller = ScrapeController()
        self.is_running = True
        self.is_paused = False
        self._started_at = time.time()
        self._paused_at = None
        self._paused_total = 0.0
        self._last_progress = {}
        self._set_running_controls()
        self.status_var.set("Refilling fields")
        self.progress.configure(value=0)
        self.percent_var.set("0.0%")
        self.stat_shops.set(str(total))
        self.stat_cities.set(f"0 / {need if not limit else min(need, limit)}")
        self.stat_elapsed.set("00:00")
        self.stat_eta.set("--:--")
        self.stat_rate.set("-- shops/hr")
        self.city_var.set("City: —")
        self.query_var.set("Query: enrichment")
        self._log(
            f"Starting field refill -> {output} | need={need} | "
            f"limit={limit or 'all'} | workers={workers} | browser={browser} | "
            f"headed={self.headed_var.get()}"
        )
        self._start_ticker()

        def worker() -> None:
            try:
                result = enrich_shops_csv(
                    output,
                    headed=self.headed_var.get(),
                    browser=browser,
                    workers=workers,
                    limit=limit,
                    controller=self.controller,
                    on_progress=lambda payload: self.event_queue.put(
                        ("progress", payload)
                    ),
                )
                result["mode"] = "enrich"
                result["output"] = str(output)
                result["shops"] = result.get("total") or total
                self.event_queue.put(("done", result))
            except Exception as err:
                self.event_queue.put(("error", str(err)))

        self.worker = threading.Thread(target=worker, daemon=True)
        self.worker.start()

    def pause_scrape(self) -> None:
        if not self.is_running or self.is_paused:
            return
        self.controller.pause()
        self.is_paused = True
        self._paused_at = time.time()
        self._set_paused_controls()
        self.status_var.set("Paused")
        self._log("Paused. Progress is held; click Resume to continue.")

    def resume_scrape(self) -> None:
        if not self.is_running or not self.is_paused:
            return
        if self._paused_at is not None:
            self._paused_total += time.time() - self._paused_at
        self._paused_at = None
        self.controller.resume()
        self.is_paused = False
        self._set_running_controls()
        self.status_var.set("Running")
        self._log("Resumed.")

    def stop_scrape(self) -> None:
        if not self.is_running:
            return
        self.controller.stop()
        self.status_var.set("Stopping...")
        self._log("Stop requested. Saving progress and shutting down browser...")
        self.pause_btn.configure(state="disabled")
        self.resume_btn.configure(state="disabled")
        self.stop_btn.configure(state="disabled")

    def open_output_folder(self) -> None:
        path = Path(self.output_var.get().strip() or DEFAULT_OUTPUT).expanduser()
        folder = path.parent if path.suffix else path
        folder.mkdir(parents=True, exist_ok=True)
        try:
            if sys.platform.startswith("win"):
                subprocess.Popen(["explorer", str(folder)])
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(folder)])
            else:
                subprocess.Popen(["xdg-open", str(folder)])
        except OSError as err:
            messagebox.showerror("Open folder failed", str(err))

    def refresh_progress_status(self) -> None:
        summary = get_progress_summary()
        failed = int(summary.get("failed_urls") or 0)
        self.progress_summary_var.set(
            f"Progress file: {summary['completed_cities']} / "
            f"{summary['total_cities']} cities completed · "
            f"{summary['saved_shops']} shops saved · "
            f"{failed} failed URLs queued"
            + ("" if summary["exists"] else " · (no progress file yet)")
        )
        if hasattr(self, "stat_shops"):
            self.stat_shops.set(str(summary["saved_shops"]))
        if hasattr(self, "stat_cities"):
            self.stat_cities.set(
                f"{summary['completed_cities']} / {summary['total_cities']}"
            )

    def clear_completed_cities_action(self) -> None:
        if self.is_running:
            messagebox.showinfo("Busy", "Stop the scrape before changing progress.")
            return
        summary = get_progress_summary()
        if summary["completed_cities"] == 0:
            messagebox.showinfo("Nothing to clear", "No completed cities are stored.")
            return
        ok = messagebox.askyesno(
            "Clear completed cities",
            f"Clear {summary['completed_cities']} completed city mark(s)?\n\n"
            "Saved shops are kept. Cities will be scraped again on the next run.",
        )
        if not ok:
            return
        result = clear_completed_cities()
        self.refresh_progress_status()
        self._log(
            f"Cleared {result['cleared_cities']} completed cities "
            f"(kept {result['remaining_shops']} shops)."
        )

    def clear_progress_shops_action(self) -> None:
        if self.is_running:
            messagebox.showinfo("Busy", "Stop the scrape before changing progress.")
            return
        summary = get_progress_summary()
        if summary["saved_shops"] == 0:
            messagebox.showinfo("Nothing to clear", "No saved shops are stored.")
            return
        ok = messagebox.askyesno(
            "Clear saved shops",
            f"Clear {summary['saved_shops']} saved shop(s) from progress?\n\n"
            "Completed-city marks are kept. Output CSV is not deleted.",
        )
        if not ok:
            return
        result = clear_progress_shops()
        self.refresh_progress_status()
        self._log(
            f"Cleared {result['cleared_shops']} saved shops "
            f"(kept {result['remaining_cities']} completed cities)."
        )

    def clear_all_progress_action(self) -> None:
        if self.is_running:
            messagebox.showinfo("Busy", "Stop the scrape before changing progress.")
            return
        summary = get_progress_summary()
        if summary["completed_cities"] == 0 and summary["saved_shops"] == 0:
            messagebox.showinfo("Nothing to clear", "Progress is already empty.")
            return
        ok = messagebox.askyesno(
            "Clear all progress",
            "Delete the entire progress file?\n\n"
            f"This removes {summary['completed_cities']} completed cities and "
            f"{summary['saved_shops']} saved shops.\n"
            "Output CSV is not deleted.",
        )
        if not ok:
            return
        result = clear_all_progress()
        self.refresh_progress_status()
        self._log(
            f"Cleared all progress "
            f"({result['cleared_cities']} cities, {result['cleared_shops']} shops)."
        )

    def export_progress_action(self) -> None:
        if self.is_running:
            messagebox.showinfo("Busy", "Stop the scrape before exporting progress.")
            return
        summary = get_progress_summary()
        if summary["saved_shops"] == 0:
            messagebox.showinfo("Nothing to export", "No shops in the progress file.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=Path(self.output_var.get() or DEFAULT_OUTPUT).name,
            initialdir=str(Path(self.output_var.get() or DEFAULT_OUTPUT).expanduser().parent),
        )
        if not path:
            return
        try:
            result = export_progress_shops_to_csv(Path(path))
        except OSError as err:
            messagebox.showerror("Export failed", str(err))
            return
        self._log(f"Exported {result['exported']} shops -> {result['path']}")
        messagebox.showinfo(
            "Export complete",
            f"Wrote {result['exported']} shops to:\n{result['path']}",
        )

    def open_progress_file(self) -> None:
        if not PROGRESS_FILE.exists():
            messagebox.showinfo(
                "No progress file",
                f"No progress file yet at:\n{PROGRESS_FILE}",
            )
            return
        try:
            if sys.platform.startswith("win"):
                subprocess.Popen(["explorer", "/select,", str(PROGRESS_FILE)])
            elif sys.platform == "darwin":
                subprocess.Popen(["open", "-R", str(PROGRESS_FILE)])
            else:
                subprocess.Popen(["xdg-open", str(PROGRESS_FILE.parent)])
        except OSError as err:
            messagebox.showerror("Open failed", str(err))

    def clear_failed_urls_action(self) -> None:
        if self.is_running:
            messagebox.showinfo("Busy", "Stop the scrape before changing progress.")
            return
        summary = get_progress_summary()
        failed = int(summary.get("failed_urls") or 0)
        if failed == 0:
            messagebox.showinfo("Nothing to clear", "No failed URLs are queued.")
            return
        ok = messagebox.askyesno(
            "Clear failed URLs",
            f"Clear {failed} failed place URL(s) from the retry queue?",
        )
        if not ok:
            return
        result = clear_failed_urls()
        self.refresh_progress_status()
        self._log(f"Cleared {result['cleared_failed_urls']} failed URL(s).")

    def clear_activity_log(self) -> None:
        self.log.configure(state="normal")
        self.log.delete("1.0", "end")
        self.log.configure(state="disabled")
        self._log("Log cleared.")

    def _start_ticker(self) -> None:
        self._stop_ticker()
        self._tick()

    def _stop_ticker(self) -> None:
        if self._tick_job is not None:
            self.root.after_cancel(self._tick_job)
            self._tick_job = None

    def _tick(self) -> None:
        if self.is_running:
            elapsed = self._active_elapsed()
            self.stat_elapsed.set(format_duration(elapsed))
            payload = self._last_progress
            shops = int(payload.get("shops", 0) or 0)
            if elapsed > 0 and shops > 0:
                rate = shops / (elapsed / 3600.0)
                self.stat_rate.set(f"{rate:.1f} shops/hr")
            eta = payload.get("eta_seconds")
            if self.is_paused:
                self.stat_eta.set("paused")
            else:
                self.stat_eta.set(format_duration(eta))
        self._tick_job = self.root.after(500, self._tick)

    def _apply_progress(self, payload: dict[str, Any]) -> None:
        self._last_progress = payload
        percent = float(payload.get("percent") or 0.0)
        self.progress.configure(value=percent)
        self.percent_var.set(f"{percent:.1f}%")

        shops = int(payload.get("shops") or 0)
        city_index = int(payload.get("city_index") or 0)
        total_cities = int(payload.get("total_cities") or len(TARGET_CITIES))
        self.stat_shops.set(str(shops))
        self.stat_cities.set(f"{city_index} / {total_cities}")

        city = payload.get("city") or "—"
        query = payload.get("query") or "—"
        status = payload.get("status") or ""
        self.city_var.set(f"City: {city}")
        self.query_var.set(f"Query: {query}")

        if self.is_paused:
            self.status_var.set("Paused")
        elif status:
            short = status if len(status) < 72 else status[:69] + "..."
            self.status_var.set(short)

        # Log meaningful status transitions lightly
        if status.startswith("Entering:") or status.startswith("Searching:"):
            self._log(status)
        elif status.startswith("Extracted:"):
            # Avoid flooding; only log every 5 shops
            if shops % 5 == 0:
                self._log(status)

    def _poll_queue(self) -> None:
        try:
            while True:
                kind, payload = self.event_queue.get_nowait()
                if kind == "progress":
                    self._apply_progress(payload)
                elif kind == "done":
                    self._on_done(payload)
                elif kind == "error":
                    self._on_error(str(payload))
        except queue.Empty:
            pass
        self.root.after(100, self._poll_queue)

    def _on_done(self, result: dict[str, Any]) -> None:
        self.is_running = False
        self.is_paused = False
        self._stop_ticker()
        self._set_idle_controls()
        self.refresh_progress_status()

        status = result.get("status", "completed")
        shops = result.get("shops", 0)
        message = result.get("message") or status
        output = result.get("output")

        if status == "stopped":
            self.status_var.set("Stopped")
            self._log(message)
            messagebox.showinfo("Stopped", f"{message}\n\nOutput: {output}")
        elif status == "already_done":
            self.status_var.set("Already done")
            self.progress.configure(value=100)
            self.percent_var.set("100.0%")
            self._log(message)
            messagebox.showinfo("Nothing to do", message)
        elif result.get("mode") == "enrich":
            self.status_var.set("Refill complete")
            self.progress.configure(value=100)
            self.percent_var.set("100.0%")
            self.stat_shops.set(str(shops))
            self.stat_eta.set("00:00")
            self._log(message)
            messagebox.showinfo("Refill complete", f"{message}\n\nOutput: {output}")
        else:
            self.status_var.set("Completed")
            self.progress.configure(value=100)
            self.percent_var.set("100.0%")
            self.stat_shops.set(str(shops))
            self.stat_eta.set("00:00")
            self._log(message)
            messagebox.showinfo("Completed", f"{message}\n\nOutput: {output}")

    def _on_error(self, message: str) -> None:
        self.is_running = False
        self.is_paused = False
        self._stop_ticker()
        self._set_idle_controls()
        self.refresh_progress_status()
        self.status_var.set("Error")
        self._log(f"ERROR: {message}")
        messagebox.showerror("Scraper error", message)

    def _on_close(self) -> None:
        if self.is_running:
            if not messagebox.askyesno(
                "Quit while running?",
                "A scrape is still running. Stop and quit?",
            ):
                return
            self.controller.stop()
            # Give worker a moment to persist
            self.root.after(400, self.root.destroy)
            return
        self._stop_ticker()
        self.root.destroy()


def run_gui() -> None:
    root = tk.Tk()
    ScraperGUI(root)
    root.mainloop()


if __name__ == "__main__":
    run_gui()
