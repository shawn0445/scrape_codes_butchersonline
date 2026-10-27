#!/usr/bin/env python3
"""Unified desktop GUI for the five-step Canada butcher data pipeline."""

from __future__ import annotations

import asyncio
import queue
import re
import subprocess
import sys
import threading
import time
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox
from typing import Any, Callable

import csv_compat
import paths

paths.ensure_dirs()

# ── palette ────────────────────────────────────────────────────────────────
BG = "#F3F5F7"
CARD = "#FFFFFF"
BORDER = "#E5E7EB"
TEXT = "#111827"
MUTED = "#6B7280"
TEAL = "#0F766E"
TEAL_HOVER = "#0D9488"
BLUE = "#2563EB"
RED = "#DC2626"
RED_HOVER = "#B91C1C"
AMBER = "#D97706"
GREEN = "#059669"
GREEN_BG = "#D1FAE5"
GREEN_FG = "#065F46"
AMBER_BG = "#FEF3C7"
AMBER_FG = "#92400E"
BLUE_BG = "#DBEAFE"
BLUE_FG = "#1E40AF"
TRACK = "#E5E7EB"
ENTRY_BG = "#FFFFFF"
LOG_FG = "#374151"

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_PROGRESS_RE = re.compile(
    r"^::progress::([0-9]+(?:\.[0-9]+)?)::(\d+)::(\d+)::(.*?)::(.*)$"
)
_SHOP_RESULT_RE = re.compile(r"^::shop_result::(ok|fail)::(.*?)::(.*)$")
_RUN_SUMMARY_RE = re.compile(r"^::run_summary::(\d+)::(\d+)::(\d+)::(\d+)::(\d+)$")


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "--:--"
    seconds = max(int(seconds), 0)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


class Card(tk.Frame):
    def __init__(self, parent: tk.Misc, title: str = "", **kwargs) -> None:
        super().__init__(
            parent,
            bg=CARD,
            highlightbackground=BORDER,
            highlightthickness=1,
            **kwargs,
        )
        if title:
            tk.Label(
                self,
                text=title,
                bg=CARD,
                fg=TEXT,
                font=("Segoe UI", 11, "bold"),
                anchor="w",
            ).pack(fill=tk.X, padx=14, pady=(12, 6))
        self.body = tk.Frame(self, bg=CARD)
        self.body.pack(fill=tk.BOTH, expand=True, padx=14, pady=(0, 12))


class Pill(tk.Frame):
    def __init__(self, parent: tk.Misc, text: str, bg: str, fg: str, **kwargs) -> None:
        super().__init__(parent, bg=bg, **kwargs)
        tk.Label(
            self,
            text=f"  {text}  ",
            bg=bg,
            fg=fg,
            font=("Segoe UI", 8),
            pady=3,
        ).pack()


class Toggle(tk.Canvas):
    def __init__(self, parent: tk.Misc, variable: tk.BooleanVar, command=None, **kwargs) -> None:
        super().__init__(parent, width=44, height=24, bg=CARD, highlightthickness=0, **kwargs)
        self.var = variable
        self.command = command
        self.bind("<Button-1>", self._toggle)
        self.var.trace_add("write", lambda *_: self._draw())
        self._draw()

    def _toggle(self, _event=None) -> None:
        self.var.set(not self.var.get())
        if self.command:
            self.command()

    def _draw(self) -> None:
        self.delete("all")
        on = self.var.get()
        fill = TEAL if on else "#D1D5DB"
        self.create_oval(2, 2, 22, 22, fill=fill, outline="")
        self.create_oval(22, 2, 42, 22, fill=fill, outline="")
        self.create_rectangle(12, 2, 32, 22, fill=fill, outline="")
        kx = 30 if on else 4
        self.create_oval(kx, 4, kx + 16, 20, fill="white", outline="")


class FillBar(tk.Frame):
    def __init__(self, parent: tk.Misc, height: int = 14, **kwargs) -> None:
        super().__init__(parent, bg=CARD, height=height, **kwargs)
        self._pct = 0.0
        self._height = height
        self.pack_propagate(False)
        self.track = tk.Frame(self, bg=TRACK, height=height)
        self.track.pack(fill=tk.BOTH, expand=True)
        self.track.pack_propagate(False)
        self.fill = tk.Frame(self.track, bg=TEAL, height=height)
        self.fill.place(x=0, y=0, width=0, height=height)
        self.track.bind("<Configure>", lambda _e: self._redraw())

    def set(self, percent: float) -> None:
        self._pct = max(0.0, min(100.0, float(percent)))
        self._redraw()

    def _redraw(self) -> None:
        w = max(self.track.winfo_width(), 1)
        self.fill.place(x=0, y=0, width=int(w * self._pct / 100.0), height=self._height)


class PipelineGUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Canada Butcher Pipeline")
        self.root.minsize(1080, 700)
        self.root.geometry("1180x780")
        self.root.configure(bg=BG)

        self.event_q: queue.Queue = queue.Queue()
        self.worker: threading.Thread | None = None
        self.reviews_proc: subprocess.Popen[str] | None = None
        self.shops_controller = None
        self.stop_event = threading.Event()
        self.is_running = False
        self.is_paused = False
        self.running_step = ""
        self.pipeline_mode = False
        self._started_at: float | None = None
        self._tick_job: str | None = None
        self.selected_step = "shops"

        self.output_dir_var = tk.StringVar(value=str(paths.OUTPUT_DIR))
        self.limit_var = tk.StringVar(value="")
        self.workers_var = tk.StringVar(value="3")
        self.headed_var = tk.BooleanVar(value=False)
        self.auto_enrich_var = tk.BooleanVar(value=True)
        self.max_per_query_var = tk.StringVar(value="20")
        self.max_reviews_var = tk.StringVar(value="All")
        self.resume_reviews_var = tk.BooleanVar(value=True)
        self.skip_existing_var = tk.BooleanVar(value=True)
        self.include_vars = {meta["key"]: tk.BooleanVar(value=True) for meta in paths.STEP_META}

        self._build()
        self._select_step("shops")
        self._refresh_files()
        self._set_idle_controls()
        self.root.after(80, self._poll_queue)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self._log("Ready. Step 1 builds the master shops CSV. Steps 2–5 read that file.")
        self._log(f"Output folder: {paths.OUTPUT_DIR}")

    # ── layout ────────────────────────────────────────────────────────────

    def _build(self) -> None:
        outer = tk.Frame(self.root, bg=BG)
        outer.pack(fill=tk.BOTH, expand=True, padx=18, pady=14)

        header = tk.Frame(outer, bg=BG)
        header.pack(fill=tk.X, pady=(0, 12))
        tk.Label(
            header,
            text="Canada Butcher Pipeline",
            bg=BG,
            fg=TEXT,
            font=("Segoe UI", 18, "bold"),
            anchor="w",
        ).pack(anchor="w")
        tk.Label(
            header,
            text=(
                "First discover shops across Canada, then collect hours, services, "
                "summaries, and reviews from that master CSV. Five files land in the output folder."
            ),
            bg=BG,
            fg=MUTED,
            font=("Segoe UI", 10),
            wraplength=1100,
            justify=tk.LEFT,
            anchor="w",
        ).pack(anchor="w", pady=(4, 0))

        body = tk.Frame(outer, bg=BG)
        body.pack(fill=tk.BOTH, expand=True)
        body.columnconfigure(0, weight=0)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)

        left = tk.Frame(body, bg=BG, width=400)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
        left.grid_propagate(False)
        right = tk.Frame(body, bg=BG)
        right.grid(row=0, column=1, sticky="nsew")

        self._build_folder(left)
        self._build_steps(left)
        self._build_files(left)
        self._build_settings(right)
        self._build_step_detail(right)
        self._build_progress(right)
        self._build_log(right)
        self._build_actions(outer)

    def _build_folder(self, parent: tk.Misc) -> None:
        card = Card(parent, "Output folder")
        card.pack(fill=tk.X, pady=(0, 10))
        tk.Label(
            card.body,
            text="All five CSVs are written here. Working files (checkpoints) go in /work.",
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 8),
            wraplength=340,
            justify=tk.LEFT,
            anchor="w",
        ).pack(fill=tk.X, pady=(0, 6))
        row = tk.Frame(card.body, bg=CARD)
        row.pack(fill=tk.X)
        entry = tk.Entry(
            row,
            textvariable=self.output_dir_var,
            font=("Segoe UI", 9),
            bg=ENTRY_BG,
            fg=TEXT,
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=BORDER,
        )
        entry.pack(side=tk.LEFT, fill=tk.X, expand=True, ipady=5, padx=(0, 8))
        self._ghost(row, "Browse", self._browse_output_dir).pack(side=tk.RIGHT)

    def _build_steps(self, parent: tk.Misc) -> None:
        card = Card(parent, "Pipeline")
        card.pack(fill=tk.X, pady=(0, 10))
        tk.Label(
            card.body,
            text="Tick steps to include in Run all. Click a row to see details.",
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 8),
            anchor="w",
        ).pack(fill=tk.X, pady=(0, 8))
        self.step_rows: dict[str, dict[str, Any]] = {}
        for meta in paths.STEP_META:
            self._step_row(card.body, meta)
        skip_row = tk.Frame(card.body, bg=CARD)
        skip_row.pack(fill=tk.X, pady=(8, 0))
        tk.Checkbutton(
            skip_row,
            text="Skip hours/services if those CSVs already look complete",
            variable=self.skip_existing_var,
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 8),
            activebackground=CARD,
            selectcolor=CARD,
            anchor="w",
        ).pack(anchor="w")

    def _step_row(self, parent: tk.Misc, meta: dict[str, Any]) -> None:
        key = meta["key"]
        wrap = tk.Frame(parent, bg=CARD, cursor="hand2")
        wrap.pack(fill=tk.X, pady=2)
        wrap.bind("<Button-1>", lambda _e, k=key: self._select_step(k))

        cb = tk.Checkbutton(
            wrap,
            variable=self.include_vars[key],
            bg=CARD,
            activebackground=CARD,
            selectcolor=CARD,
        )
        cb.pack(side=tk.LEFT, padx=(0, 4))

        badge = tk.Label(
            wrap,
            text=str(meta["number"]),
            bg=BLUE_BG,
            fg=BLUE_FG,
            font=("Segoe UI", 9, "bold"),
            width=2,
        )
        badge.pack(side=tk.LEFT, padx=(0, 8))
        badge.bind("<Button-1>", lambda _e, k=key: self._select_step(k))

        mid = tk.Frame(wrap, bg=CARD)
        mid.pack(side=tk.LEFT, fill=tk.X, expand=True)
        title = tk.Label(
            mid,
            text=meta["title"],
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 9, "bold"),
            anchor="w",
        )
        title.pack(fill=tk.X)
        file_lbl = tk.Label(
            mid,
            text=meta["csv_name"],
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 8),
            anchor="w",
        )
        file_lbl.pack(fill=tk.X)
        for w in (mid, title, file_lbl):
            w.bind("<Button-1>", lambda _e, k=key: self._select_step(k))

        status = tk.Label(wrap, text="—", bg=CARD, fg=MUTED, font=("Segoe UI", 8))
        status.pack(side=tk.RIGHT)
        self.step_rows[key] = {
            "wrap": wrap,
            "title": title,
            "file": file_lbl,
            "mid": mid,
            "status": status,
            "badge": badge,
        }

    def _build_files(self, parent: tk.Misc) -> None:
        card = Card(parent, "Five CSV files")
        card.pack(fill=tk.BOTH, expand=True)
        self.file_labels: dict[str, tk.StringVar] = {}
        for meta in paths.STEP_META:
            row = tk.Frame(card.body, bg=CARD)
            row.pack(fill=tk.X, pady=2)
            tk.Label(
                row,
                text=meta["csv_name"],
                bg=CARD,
                fg=TEXT,
                font=("Consolas", 9),
                anchor="w",
            ).pack(side=tk.LEFT)
            var = tk.StringVar(value="not created")
            self.file_labels[meta["key"]] = var
            tk.Label(
                row,
                textvariable=var,
                bg=CARD,
                fg=MUTED,
                font=("Segoe UI", 8),
                anchor="e",
            ).pack(side=tk.RIGHT)
        btns = tk.Frame(card.body, bg=CARD)
        btns.pack(fill=tk.X, pady=(10, 0))
        self._ghost(btns, "Open output folder", self._open_output).pack(side=tk.LEFT)
        self._ghost(btns, "Refresh", self._refresh_files).pack(side=tk.LEFT, padx=(8, 0))

    def _build_settings(self, parent: tk.Misc) -> None:
        card = Card(parent, "Shared settings")
        card.pack(fill=tk.X, pady=(0, 10))
        body = card.body

        grid = tk.Frame(body, bg=CARD)
        grid.pack(fill=tk.X)
        for i in range(4):
            grid.columnconfigure(i, weight=1)

        self._labeled_entry(grid, 0, "Shop limit (blank = all)", self.limit_var)
        self._labeled_entry(grid, 1, "Workers / browsers", self.workers_var)
        self._labeled_entry(grid, 2, "Max Maps results per city", self.max_per_query_var)

        headed = tk.Frame(grid, bg=CARD)
        headed.grid(row=0, column=3, sticky="ew", padx=(8, 0))
        tk.Label(headed, text="Show browser", bg=CARD, fg=MUTED, font=("Segoe UI", 8)).pack(anchor="w")
        row = tk.Frame(headed, bg=CARD)
        row.pack(anchor="w", pady=(4, 0))
        Toggle(row, self.headed_var, command=self._sync_headed).pack(side=tk.LEFT)
        self.headed_lbl = tk.Label(row, text="Off", bg=CARD, fg=MUTED, font=("Segoe UI", 8))
        self.headed_lbl.pack(side=tk.LEFT, padx=(8, 0))

        extra = tk.Frame(body, bg=CARD)
        extra.pack(fill=tk.X, pady=(10, 0))
        tk.Checkbutton(
            extra,
            text="After shops: refill missing phone / website / postal",
            variable=self.auto_enrich_var,
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 8),
            activebackground=CARD,
            selectcolor=CARD,
            anchor="w",
        ).pack(side=tk.LEFT)
        tk.Label(extra, text="Reviews cap", bg=CARD, fg=MUTED, font=("Segoe UI", 8)).pack(
            side=tk.LEFT, padx=(18, 6)
        )
        opts = ("All", "10", "25", "50", "100", "200")
        menu = tk.OptionMenu(extra, self.max_reviews_var, *opts)
        menu.config(
            font=("Segoe UI", 8),
            bg=ENTRY_BG,
            highlightthickness=1,
            highlightbackground=BORDER,
            relief=tk.FLAT,
            width=6,
        )
        tk.Checkbutton(
            extra,
            text="Resume reviews from checkpoint",
            variable=self.resume_reviews_var,
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 8),
            activebackground=CARD,
            selectcolor=CARD,
            anchor="w",
        ).pack(side=tk.LEFT, padx=(14, 0))

    def _labeled_entry(self, parent: tk.Misc, col: int, label: str, var: tk.StringVar) -> None:
        cell = tk.Frame(parent, bg=CARD)
        cell.grid(row=0, column=col, sticky="ew", padx=(0 if col == 0 else 8, 0))
        tk.Label(cell, text=label, bg=CARD, fg=MUTED, font=("Segoe UI", 8)).pack(anchor="w")
        tk.Entry(
            cell,
            textvariable=var,
            font=("Segoe UI", 10),
            bg=ENTRY_BG,
            fg=TEXT,
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=BORDER,
            width=12,
        ).pack(fill=tk.X, pady=(4, 0), ipady=3)

    def _build_step_detail(self, parent: tk.Misc) -> None:
        card = Card(parent)
        card.pack(fill=tk.X, pady=(0, 10))
        self.detail_title = tk.StringVar(value="")
        self.detail_body = tk.StringVar(value="")
        self.detail_produces = tk.StringVar(value="")
        tk.Label(
            card.body,
            textvariable=self.detail_title,
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 12, "bold"),
            anchor="w",
        ).pack(fill=tk.X)
        tk.Label(
            card.body,
            textvariable=self.detail_body,
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 9),
            wraplength=680,
            justify=tk.LEFT,
            anchor="w",
        ).pack(fill=tk.X, pady=(6, 0))
        tk.Label(
            card.body,
            textvariable=self.detail_produces,
            bg=CARD,
            fg=TEAL,
            font=("Segoe UI", 8),
            wraplength=680,
            justify=tk.LEFT,
            anchor="w",
        ).pack(fill=tk.X, pady=(6, 0))
        self.step_hint = tk.StringVar(value="")
        tk.Label(
            card.body,
            textvariable=self.step_hint,
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 8),
            wraplength=680,
            justify=tk.LEFT,
            anchor="w",
        ).pack(fill=tk.X, pady=(4, 0))

    def _build_progress(self, parent: tk.Misc) -> None:
        card = Card(parent, "Progress")
        card.pack(fill=tk.X, pady=(0, 10))
        top = tk.Frame(card.body, bg=CARD)
        top.pack(fill=tk.X)
        self.status_var = tk.StringVar(value="Idle — choose a step or run the full pipeline")
        self.pct_var = tk.StringVar(value="0%")
        tk.Label(
            top,
            textvariable=self.status_var,
            bg=CARD,
            fg=TEAL,
            font=("Segoe UI", 10, "bold"),
            anchor="w",
        ).pack(side=tk.LEFT, fill=tk.X, expand=True)
        tk.Label(
            top,
            textvariable=self.pct_var,
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 11, "bold"),
        ).pack(side=tk.RIGHT)
        self.bar = FillBar(card.body, height=14)
        self.bar.pack(fill=tk.X, pady=(8, 8))
        stats = tk.Frame(card.body, bg=CARD)
        stats.pack(fill=tk.X)
        self.stat_step = tk.StringVar(value="Step: —")
        self.stat_elapsed = tk.StringVar(value="Elapsed: 00:00")
        self.stat_detail = tk.StringVar(value="")
        for var in (self.stat_step, self.stat_elapsed, self.stat_detail):
            tk.Label(
                stats,
                textvariable=var,
                bg=CARD,
                fg=MUTED,
                font=("Segoe UI", 8),
                anchor="w",
            ).pack(side=tk.LEFT, padx=(0, 16))

    def _build_log(self, parent: tk.Misc) -> None:
        card = Card(parent, "Activity log")
        card.pack(fill=tk.BOTH, expand=True)
        self.log = tk.Text(
            card.body,
            wrap=tk.WORD,
            font=("Consolas", 9),
            bg="#F8FAFC",
            fg=LOG_FG,
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=BORDER,
            height=10,
            state=tk.DISABLED,
        )
        self.log.pack(fill=tk.BOTH, expand=True)

    def _build_actions(self, parent: tk.Misc) -> None:
        row = tk.Frame(parent, bg=BG)
        row.pack(fill=tk.X, pady=(12, 0))
        self.run_all_btn = self._action(row, "▶  Run selected pipeline", TEAL, TEAL_HOVER, "white", self.start_pipeline)
        self.run_all_btn.pack(side=tk.LEFT, padx=(0, 8))
        self.run_step_btn = self._action(row, "Run this step only", CARD, "#F3F4F6", TEXT, self.start_selected_step, border=True)
        self.run_step_btn.pack(side=tk.LEFT, padx=(0, 8))
        self.pause_btn = self._action(row, "Pause", AMBER, "#B45309", "white", self.pause_run)
        self.pause_btn.pack(side=tk.LEFT, padx=(0, 8))
        self.resume_btn = self._action(row, "Resume", TEAL, TEAL_HOVER, "white", self.resume_run)
        self.resume_btn.pack(side=tk.LEFT, padx=(0, 8))
        self.stop_btn = self._action(row, "Stop", RED, RED_HOVER, "white", self.stop_run)
        self.stop_btn.pack(side=tk.LEFT)
        self.pill_host = tk.Frame(row, bg=BG)
        self.pill_host.pack(side=tk.RIGHT)
        self._set_pills(False)

    # ── small widgets ─────────────────────────────────────────────────────

    def _ghost(self, parent: tk.Misc, text: str, command) -> tk.Label:
        btn = tk.Label(
            parent,
            text=f"  {text}  ",
            bg="#F9FAFB",
            fg=TEXT,
            font=("Segoe UI", 8),
            highlightthickness=1,
            highlightbackground=BORDER,
            cursor="hand2",
            pady=5,
        )
        btn.bind("<Button-1>", lambda _e: command())
        btn.bind("<Enter>", lambda _e: btn.config(bg="#F3F4F6"))
        btn.bind("<Leave>", lambda _e: btn.config(bg="#F9FAFB"))
        return btn

    def _action(self, parent, text, bg, hover, fg, command, border=False) -> tk.Label:
        btn = tk.Label(
            parent,
            text=f"  {text}  ",
            bg=bg,
            fg=fg,
            font=("Segoe UI", 10, "bold"),
            cursor="hand2",
            pady=10,
        )
        if border:
            btn.configure(highlightthickness=1, highlightbackground=BORDER)
        btn._normal_bg = bg  # type: ignore[attr-defined]
        btn._hover_bg = hover  # type: ignore[attr-defined]
        btn._fg = fg  # type: ignore[attr-defined]

        def enter(_e):
            if str(btn.cget("state")) != "disabled":
                btn.config(bg=hover)

        def leave(_e):
            if str(btn.cget("state")) != "disabled":
                btn.config(bg=bg)

        def click(_e):
            if str(btn.cget("state")) != "disabled":
                command()

        btn.bind("<Enter>", enter)
        btn.bind("<Leave>", leave)
        btn.bind("<Button-1>", click)
        return btn

    def _enable(self, btn: tk.Label, on: bool) -> None:
        if on:
            btn.config(state=tk.NORMAL, cursor="hand2", bg=btn._normal_bg, fg=btn._fg)  # type: ignore[attr-defined]
        else:
            btn.config(state=tk.DISABLED, cursor="arrow", bg="#D1D5DB", fg="#9CA3AF")

    def _sync_headed(self) -> None:
        self.headed_lbl.config(
            text="On (visible)" if self.headed_var.get() else "Off (headless)",
            fg=TEXT if self.headed_var.get() else MUTED,
        )

    def _set_pills(self, running: bool) -> None:
        for w in self.pill_host.winfo_children():
            w.destroy()
        if running:
            Pill(self.pill_host, "Running", GREEN_BG, GREEN_FG).pack(side=tk.LEFT)
        else:
            Pill(self.pill_host, "Idle", "#F3F4F6", MUTED).pack(side=tk.LEFT)

    # ── paths ─────────────────────────────────────────────────────────────

    def _out_dir(self) -> Path:
        text = self.output_dir_var.get().strip() or str(paths.OUTPUT_DIR)
        folder = Path(text)
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    def _csv(self, key: str) -> Path:
        names = {
            "shops": "01_shops.csv",
            "hours": "02_hours.csv",
            "services": "03_service_options.csv",
            "summary": "04_summary.csv",
            "reviews": "05_reviews.csv",
        }
        return self._out_dir() / names[key]

    def _browse_output_dir(self) -> None:
        chosen = filedialog.askdirectory(initialdir=self.output_dir_var.get())
        if chosen:
            self.output_dir_var.set(chosen)
            self._refresh_files()

    def _open_output(self) -> None:
        folder = self._out_dir()
        try:
            if sys.platform.startswith("win"):
                subprocess.Popen(["explorer", str(folder)])
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(folder)])
            else:
                subprocess.Popen(["xdg-open", str(folder)])
        except OSError as err:
            messagebox.showerror("Open folder failed", str(err))

    def _refresh_files(self) -> None:
        for meta in paths.STEP_META:
            path = self._csv(meta["key"])
            status_lbl = self.step_rows[meta["key"]]["status"]
            if path.exists():
                n = csv_compat.count_csv_rows(path)
                self.file_labels[meta["key"]].set(f"{n:,} rows")
                status_lbl.config(text=f"{n:,} rows", fg=GREEN)
            else:
                self.file_labels[meta["key"]].set("not created")
                status_lbl.config(text="waiting", fg=MUTED)

    def _select_step(self, key: str) -> None:
        self.selected_step = key
        meta = next(m for m in paths.STEP_META if m["key"] == key)
        self.detail_title.set(f"Step {meta['number']}  ·  {meta['title']}")
        self.detail_body.set(meta["summary"])
        self.detail_produces.set(f"Writes {meta['csv_name']}  —  {meta['produces']}")
        hints = {
            "shops": "This must run first (or you must already have 01_shops.csv). Later steps will not start without it.",
            "hours": "Reads 01_shops.csv. Resumes shops that do not yet have hours in 02_hours.csv.",
            "services": "Reads 01_shops.csv. Uses a checkpoint in /work so you can stop and continue.",
            "summary": "Merges shops + hours + services. Fast local step — no browser.",
            "reviews": "Reads 01_shops.csv. Set shop limit or shops-per-run to test on a small batch first.",
        }
        self.step_hint.set(hints[key])
        for k, widgets in self.step_rows.items():
            selected = k == key
            bg = "#F0FDFA" if selected else CARD
            for part in ("wrap", "title", "file", "mid", "status"):
                widgets[part].config(bg=bg)

    # ── logging / progress ────────────────────────────────────────────────

    def _log(self, message: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        self.log.configure(state=tk.NORMAL)
        self.log.insert(tk.END, f"[{stamp}] {message}\n")
        self.log.see(tk.END)
        self.log.configure(state=tk.DISABLED)

    def _set_progress(self, pct: float, status: str | None = None, detail: str = "") -> None:
        display = int(round(max(0.0, min(100.0, float(pct)))))
        self.bar.set(display)
        self.pct_var.set(f"{display}%")
        if status:
            self.status_var.set(status)
        if detail:
            self.stat_detail.set(detail)

    def _set_idle_controls(self) -> None:
        self._enable(self.run_all_btn, True)
        self._enable(self.run_step_btn, True)
        self._enable(self.pause_btn, False)
        self._enable(self.resume_btn, False)
        self._enable(self.stop_btn, False)
        self._set_pills(False)

    def _set_running_controls(self) -> None:
        self._enable(self.run_all_btn, False)
        self._enable(self.run_step_btn, False)
        can_pause = self.running_step == "shops"
        self._enable(self.pause_btn, can_pause and not self.is_paused)
        self._enable(self.resume_btn, can_pause and self.is_paused)
        self._enable(self.stop_btn, True)
        self._set_pills(True)

    # ── start / stop ──────────────────────────────────────────────────────

    def _parse_limit(self) -> int | None:
        text = self.limit_var.get().strip()
        if not text:
            return None
        try:
            n = int(text)
        except ValueError as err:
            raise ValueError("Shop limit must be a whole number, or leave it blank for all shops.") from err
        if n <= 0:
            raise ValueError("Shop limit must be a positive number.")
        return n

    def _parse_workers(self) -> int:
        try:
            n = int(self.workers_var.get().strip() or "3")
        except ValueError as err:
            raise ValueError("Workers must be a whole number from 1 to 8.") from err
        return max(1, min(8, n))

    def start_pipeline(self) -> None:
        steps = [m["key"] for m in paths.STEP_META if self.include_vars[m["key"]].get()]
        if not steps:
            messagebox.showinfo("Nothing selected", "Tick at least one pipeline step.")
            return
        self._launch(steps, pipeline=True)

    def start_selected_step(self) -> None:
        self._launch([self.selected_step], pipeline=False)

    def _launch(self, steps: list[str], *, pipeline: bool) -> None:
        if self.is_running:
            return
        try:
            self._parse_limit()
            self._parse_workers()
        except ValueError as err:
            messagebox.showerror("Invalid settings", str(err))
            return
        if any(s != "shops" for s in steps) and not self._csv("shops").exists():
            messagebox.showerror(
                "Shops CSV missing",
                "Run Step 1 first (Discover shops). Later steps read 01_shops.csv.",
            )
            return

        self.stop_event = threading.Event()
        self.is_running = True
        self.is_paused = False
        self.pipeline_mode = pipeline
        self._started_at = time.time()
        self.running_step = steps[0]
        self._set_running_controls()
        self._set_progress(0, "Starting…")
        self.stat_step.set(f"Step: {steps[0]}")
        label = "full pipeline" if pipeline else f"step '{steps[0]}'"
        self._log(f"Starting {label} → {self._out_dir()}")
        self._start_ticker()

        def worker() -> None:
            try:
                for key in steps:
                    if self.stop_event.is_set():
                        self.event_q.put(("done", {"status": "stopped", "message": "Stopped by user."}))
                        return
                    if pipeline and self.skip_existing_var.get() and self._should_skip_step(key):
                        path = self._csv(key)
                        self.event_q.put(
                            ("log", f"Skipping {key}: {path.name} already looks complete.")
                        )
                        continue
                    self.event_q.put(("step", key))
                    result = self._run_step(key)
                    self.event_q.put(("step_done", (key, result)))
                    if result.get("status") in {"error", "stopped"}:
                        self.event_q.put(("done", result))
                        return
                self.event_q.put(
                    (
                        "done",
                        {
                            "status": "completed",
                            "message": "Pipeline finished. Check the five CSV files in the output folder.",
                        },
                    )
                )
            except Exception as err:
                self.event_q.put(("error", str(err)))

        self.worker = threading.Thread(target=worker, daemon=True)
        self.worker.start()

    def _should_skip_step(self, key: str) -> bool:
        """Skip later steps when their CSV already looks finished. Never skip shops."""
        if key == "shops":
            return False
        if key == "summary":
            return False
        if key == "reviews":
            return False
        if key == "hours":
            return self._hours_complete()
        if key == "services":
            return self._services_complete()
        return False

    def _hours_complete(self) -> bool:
        path = self._csv("hours")
        if not path.exists():
            return False
        shops = csv_compat.count_csv_rows(self._csv("shops"))
        _fields, rows = csv_compat.read_csv_rows(path)
        if shops <= 0 or len(rows) < shops:
            return False
        day_cols = [
            "hours_monday",
            "hours_tuesday",
            "hours_wednesday",
            "hours_thursday",
            "hours_friday",
            "hours_saturday",
            "hours_sunday",
        ]
        for row in rows:
            filled = 0
            for col in day_cols:
                val = csv_compat.field(row, col)
                if val and val.lower() != "not listed":
                    filled += 1
            if filled < 7:
                return False
        return True

    def _services_complete(self) -> bool:
        path = self._csv("services")
        if not path.exists():
            return False
        shops = csv_compat.count_csv_rows(self._csv("shops"))
        _fields, rows = csv_compat.read_csv_rows(path)
        if shops <= 0 or len(rows) < shops:
            return False
        shopping = re.compile(
            r"delivery|pick-?up|pickup|kerbside|curbside|in-store shopping|"
            r"on-site services|takeout|take-out|takeaway|dine-in|no-contact",
            re.I,
        )
        for row in rows:
            opts = str(row.get("serviceOptions") or "")
            if not shopping.search(opts):
                return False
        return True

    def pause_run(self) -> None:
        if not self.is_running or self.running_step != "shops" or self.is_paused:
            return
        if self.shops_controller:
            self.shops_controller.pause()
        self.is_paused = True
        self.status_var.set("Paused")
        self._set_running_controls()
        self._log("Paused shop discovery. Click Resume to continue.")

    def resume_run(self) -> None:
        if not self.is_running or not self.is_paused:
            return
        if self.shops_controller:
            self.shops_controller.resume()
        self.is_paused = False
        self.status_var.set("Running")
        self._set_running_controls()
        self._log("Resumed.")

    def stop_run(self) -> None:
        if not self.is_running:
            return
        self.stop_event.set()
        if self.shops_controller:
            self.shops_controller.stop()
        proc = self.reviews_proc
        if proc and proc.poll() is None:
            try:
                proc.terminate()
            except OSError:
                pass
        self.status_var.set("Stopping…")
        self._log("Stop requested. Saving progress where possible…")
        self._enable(self.pause_btn, False)
        self._enable(self.resume_btn, False)
        self._enable(self.stop_btn, False)

    # ── step runners (background thread) ──────────────────────────────────

    def _emit_progress(self, payload: dict[str, Any]) -> None:
        self.event_q.put(("progress", payload))

    def _run_step(self, key: str) -> dict[str, Any]:
        runners: dict[str, Callable[[], dict[str, Any]]] = {
            "shops": self._run_shops,
            "hours": self._run_hours,
            "services": self._run_services,
            "summary": self._run_summary,
            "reviews": self._run_reviews,
        }
        return runners[key]()

    def _run_shops(self) -> dict[str, Any]:
        shops_dir = str(paths.ALL_SHOPS_DIR)
        if shops_dir not in sys.path:
            sys.path.insert(0, shops_dir)
        from scraper import ScrapeController, scrape_google_maps

        output = self._csv("shops")
        self.shops_controller = ScrapeController()
        limit = self._parse_limit()
        result = scrape_google_maps(
            output=output,
            existing_csv=output if output.exists() else None,
            limit=limit,
            max_per_query=int(self.max_per_query_var.get().strip() or "20"),
            headed=self.headed_var.get(),
            workers=self._parse_workers(),
            browser="chromium",
            controller=self.shops_controller,
            on_progress=self._emit_progress,
            reset_state=True,
            enrich_missing=self.auto_enrich_var.get(),
            enrich_limit=limit,
        )
        if output.exists():
            csv_compat.ensure_shop_ids(output)
        result.setdefault("output", str(output))
        return result

    def _run_hours(self) -> dict[str, Any]:
        hours_dir = str(paths.HOURS_DIR)
        if hours_dir not in sys.path:
            sys.path.insert(0, hours_dir)
        from hours_scraper import scrape_hours

        csv_compat.ensure_shop_ids(self._csv("shops"))
        return scrape_hours(
            self._csv("shops"),
            self._csv("hours"),
            failed_csv=paths.HOURS_FAILED_CSV,
            limit=self._parse_limit(),
            headed=self.headed_var.get(),
            workers=self._parse_workers(),
            on_progress=self._emit_progress,
            stop_event=self.stop_event,
        )

    def _run_services(self) -> dict[str, Any]:
        svc_dir = str(paths.SERVICE_OPS_DIR)
        if svc_dir not in sys.path:
            sys.path.insert(0, svc_dir)
        from service_ops import load_shops, run_scraper

        csv_compat.ensure_shop_ids(self._csv("shops"))
        shops = load_shops(self._csv("shops"))
        return asyncio.run(
            run_scraper(
                shops=shops,
                headless=not self.headed_var.get(),
                output_path=self._csv("services"),
                checkpoint_path=paths.SERVICE_CHECKPOINT,
                resume=True,
                concurrency=min(5, self._parse_workers()),
                limit=self._parse_limit(),
                on_progress=self._emit_progress,
                stop_event=self.stop_event,
            )
        )

    def _run_summary(self) -> dict[str, Any]:
        summary_dir = str(paths.SUMMARY_DIR)
        if summary_dir not in sys.path:
            sys.path.insert(0, summary_dir)
        from gensum_woid import generate_summary

        self._emit_progress(
            {"percent": 30, "status": "Merging shops, hours, and services…", "shop": ""}
        )
        result = generate_summary(
            self._csv("shops"),
            self._csv("hours"),
            self._csv("services"),
            self._csv("summary"),
        )
        self._emit_progress({"percent": 100, "status": result.get("message") or "Summary done", "shop": ""})
        return result

    def _run_reviews(self) -> dict[str, Any]:
        shops = self._csv("shops")
        output = self._csv("reviews")
        csv_compat.ensure_shop_ids(shops)
        max_reviews = self.max_reviews_var.get().strip()
        max_n = 0 if not max_reviews or max_reviews.lower() == "all" else int(max_reviews)
        cmd = [
            sys.executable,
            "-u",
            str(paths.REVIEWS_DIR / "scrape_main.py"),
            "-s",
            str(shops),
            "-o",
            str(output),
            "--max-reviews",
            str(max_n),
            "--concurrency",
            str(min(3, self._parse_workers())),
            "--checkpoint-file",
            str(paths.REVIEWS_CHECKPOINT),
        ]
        limit = self._parse_limit()
        if limit:
            cmd.extend(["--limit", str(limit)])
        if self.headed_var.get():
            cmd.append("--headed")
        if self.resume_reviews_var.get():
            cmd.append("--resume")

        self.event_q.put(("log", "Launching review scraper…"))
        try:
            self.reviews_proc = subprocess.Popen(
                cmd,
                cwd=str(paths.REVIEWS_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
            )
        except OSError as err:
            return {"status": "error", "message": str(err)}

        proc = self.reviews_proc
        assert proc.stdout is not None
        for raw in proc.stdout:
            if self.stop_event.is_set():
                try:
                    proc.terminate()
                except OSError:
                    pass
                break
            line = _ANSI_RE.sub("", raw).strip()
            if not line:
                continue
            prog = _PROGRESS_RE.match(line)
            if prog:
                pct, done, total, shop, stage = prog.groups()
                self._emit_progress(
                    {
                        "percent": float(pct),
                        "done": int(done),
                        "total": int(total),
                        "status": stage or "Scraping reviews",
                        "shop": shop,
                    }
                )
                continue
            shop_res = _SHOP_RESULT_RE.match(line)
            if shop_res:
                status, label, detail = shop_res.groups()
                self.event_q.put(("log", f"{label} — {detail}"))
                continue
            if line.startswith("::log::"):
                self.event_q.put(("log", line[len("::log::") :]))
                continue
            if line.startswith("::"):
                continue
            if "Shops:" in line or "it/s" in line:
                continue
            self.event_q.put(("log", line[:240]))

        code = proc.wait()
        self.reviews_proc = None
        if self.stop_event.is_set():
            return {"status": "stopped", "message": "Review scrape stopped.", "output": str(output)}
        if code == 0:
            return {
                "status": "completed",
                "message": f"Reviews saved to {output.name}",
                "output": str(output),
            }
        return {
            "status": "error",
            "message": f"Review scraper exited with code {code}",
            "output": str(output),
        }

    # ── UI event loop ─────────────────────────────────────────────────────

    def _start_ticker(self) -> None:
        self._stop_ticker()
        self._tick()

    def _stop_ticker(self) -> None:
        if self._tick_job:
            self.root.after_cancel(self._tick_job)
            self._tick_job = None

    def _tick(self) -> None:
        if self.is_running and self._started_at:
            self.stat_elapsed.set(f"Elapsed: {format_duration(time.time() - self._started_at)}")
        self._tick_job = self.root.after(500, self._tick)

    def _poll_queue(self) -> None:
        try:
            while True:
                kind, payload = self.event_q.get_nowait()
                if kind == "log":
                    self._log(str(payload))
                elif kind == "progress":
                    self._apply_progress(payload)
                elif kind == "step":
                    self.running_step = str(payload)
                    self.stat_step.set(f"Step: {payload}")
                    self._select_step(str(payload))
                    self._set_running_controls()
                    self._log(f"── {payload.upper()} ──")
                elif kind == "step_done":
                    key, result = payload
                    msg = result.get("message") or result.get("status") or "done"
                    self._log(f"{key}: {msg}")
                    self._refresh_files()
                elif kind == "done":
                    self._on_done(payload)
                elif kind == "error":
                    self._on_error(str(payload))
        except queue.Empty:
            pass
        self.root.after(80, self._poll_queue)

    def _apply_progress(self, payload: dict[str, Any]) -> None:
        pct = float(payload.get("percent") or 0)
        status = str(payload.get("status") or "")
        shop = str(payload.get("shop") or payload.get("city") or "")
        done = payload.get("done")
        total = payload.get("total") or payload.get("total_cities")
        detail = shop
        if done is not None and total:
            detail = f"{done}/{total}" + (f"  ·  {shop}" if shop else "")
        elif shop:
            detail = shop
        self._set_progress(pct, status or None, detail)

    def _on_done(self, result: dict[str, Any]) -> None:
        self.is_running = False
        self.is_paused = False
        self.running_step = ""
        self.shops_controller = None
        self._stop_ticker()
        self._set_idle_controls()
        self._refresh_files()
        status = result.get("status", "completed")
        message = result.get("message") or status
        if status == "stopped":
            self.status_var.set("Stopped")
            self._log(message)
            messagebox.showinfo("Stopped", message)
        elif status == "error":
            self.status_var.set("Error")
            self._log(f"ERROR: {message}")
            messagebox.showerror("Pipeline error", message)
        else:
            self._set_progress(100, "Completed")
            self._log(message)
            files = "\n".join(f"• {self._csv(m['key']).name}" for m in paths.STEP_META)
            messagebox.showinfo(
                "Completed",
                f"{message}\n\nOutput folder:\n{self._out_dir()}\n\n{files}",
            )

    def _on_error(self, message: str) -> None:
        self._on_done({"status": "error", "message": message})

    def _on_close(self) -> None:
        if self.is_running:
            if not messagebox.askyesno("Quit", "A job is still running. Stop and quit?"):
                return
            self.stop_run()
            self.root.after(400, self.root.destroy)
            return
        self._stop_ticker()
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    try:
        from ctypes import windll

        windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass
    PipelineGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
