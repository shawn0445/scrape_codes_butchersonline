"""Maps Review Scraper — GUI matching the product mockup."""

from __future__ import annotations

import csv
import json
import queue
import re
import subprocess
import sys
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SCRAPE_MAIN = SCRIPT_DIR / "scrape_main.py"

# ── palette (matches mockup) ──────────────────────────────────────────────
BG = "#F3F4F6"
CARD = "#FFFFFF"
BORDER = "#E5E7EB"
TEXT = "#111827"
MUTED = "#6B7280"
BLUE = "#2563EB"
BLUE_HOVER = "#1D4ED8"
RED = "#EF4444"
RED_HOVER = "#DC2626"
GREEN_BG = "#D1FAE5"
GREEN_FG = "#065F46"
GREEN_DOT = "#10B981"
RED_SOFT_BG = "#FEE2E2"
RED_SOFT_FG = "#991B1B"
AMBER_BG = "#FEF3C7"
AMBER_FG = "#92400E"
BLUE_PILL_BG = "#DBEAFE"
BLUE_PILL_FG = "#1E40AF"
TRACK = "#E5E7EB"
ENTRY_BG = "#FFFFFF"
LOG_FG = "#374151"

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_SHOP_DONE_RE = re.compile(
    r"[✅⚠].*?\[(\d+)/(\d+)\]\s*(.+?)\s*—\s*(.+)$"
)
_SHOP_INDEX_RE = re.compile(r"\[(\d+)/(\d+)\]")
# ::progress::{pct}::{done}::{total}::{shop}::{stage}
_PROGRESS_RE = re.compile(
    r"^::progress::([0-9]+(?:\.[0-9]+)?)::(\d+)::(\d+)::(.*?)::(.*)$"
)
# ::worker_progress::{wid}::{pct}::{shop_key}::{shop}::{stage}
_WORKER_PROGRESS_RE = re.compile(
    r"^::worker_progress::(\d+)::([0-9]+(?:\.[0-9]+)?)::(.*?)::(.*?)::(.*)$"
)
_WORKER_IDLE_RE = re.compile(r"^::worker_idle::(\d+)$")
_WORKERS_RE = re.compile(r"^::workers::(\d+)$")
_STOP_INFLIGHT_RE = re.compile(r"^::stop_inflight::(.*)$")
_RELOAD_PASS_RE = re.compile(r"^::reload_pass::(\d+)::(\d+)$")
_BATCH_PLAN_RE = re.compile(r"^::batch_plan::(\d+)::(\d+)::(\d+)::(\d+)$")
_BATCH_REMAINING_RE = re.compile(r"^::batch_remaining::(\d+)::(\d+)$")
# ::shop_result::{ok|fail}::{label}::{detail}
_SHOP_RESULT_RE = re.compile(r"^::shop_result::(ok|fail)::(.*?)::(.*)$")
# ::run_summary::{ok}::{fail}::{incomplete}::{skipped}::{total}
_RUN_SUMMARY_RE = re.compile(r"^::run_summary::(\d+)::(\d+)::(\d+)::(\d+)::(\d+)$")
# ::summary_row::{status}::{idx}::{total}::{name}::{reviews}::{detail}
_SUMMARY_ROW_RE = re.compile(
    r"^::summary_row::(\w+)::(\d+)::(\d+)::(.*?)::(.*?)::(.*)$"
)


# ── small widgets ─────────────────────────────────────────────────────────


class Card(tk.Frame):
    def __init__(self, parent: tk.Misc, title: str, **kwargs) -> None:
        super().__init__(
            parent,
            bg=CARD,
            highlightbackground=BORDER,
            highlightthickness=1,
            **kwargs,
        )
        hdr = tk.Label(
            self,
            text=title,
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 11, "bold"),
            anchor="w",
        )
        hdr.pack(fill=tk.X, padx=16, pady=(14, 8))
        self.body = tk.Frame(self, bg=CARD)
        self.body.pack(fill=tk.BOTH, expand=True, padx=16, pady=(0, 14))


class Toggle(tk.Canvas):
    """Simple on/off switch."""

    def __init__(
        self,
        parent: tk.Misc,
        variable: tk.BooleanVar,
        command=None,
        **kwargs,
    ) -> None:
        super().__init__(
            parent,
            width=44,
            height=24,
            bg=CARD,
            highlightthickness=0,
            **kwargs,
        )
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
        fill = BLUE if on else "#D1D5DB"
        self.create_oval(2, 2, 22, 22, fill=fill, outline="")
        self.create_oval(22, 2, 42, 22, fill=fill, outline="")
        self.create_rectangle(12, 2, 32, 22, fill=fill, outline="")
        kx = 30 if on else 4
        self.create_oval(kx, 4, kx + 16, 20, fill="white", outline="")


class Pill(tk.Frame):
    def __init__(
        self,
        parent: tk.Misc,
        text: str,
        bg: str,
        fg: str,
        icon: str = "●",
        **kwargs,
    ) -> None:
        super().__init__(parent, bg=bg, **kwargs)
        tk.Label(
            self,
            text=f"  {icon}  {text}  ",
            bg=bg,
            fg=fg,
            font=("Segoe UI", 9),
            pady=4,
        ).pack()


class ScraperGUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Maps Review Scraper")
        self.root.minsize(880, 560)
        self.root.geometry("960x640")
        self.root.configure(bg=BG)

        self._proc: subprocess.Popen[str] | None = None
        self._reader: threading.Thread | None = None
        self._log_q: queue.Queue[tuple[str, str] | None] = queue.Queue()
        self._running = False
        self._resume_mode = False
        self._line_buf = ""
        self._done_count = 0
        self._total_shops = 0
        self._checkpoint_saved = False
        self._last_logged_stage = ""
        self._shop_results: list[dict[str, str]] = []
        self._summary_counts: dict[str, int] | None = None
        self._in_summary = False
        self._worker_rows: list[dict[str, Any]] = []
        self._active_workers: dict[int, dict[str, str]] = {}
        self._stop_purge_keys: set[str] = set()
        self._user_stopped = False

        self.shops_var = tk.StringVar(value="shops-with-10-reviews.csv")
        self.output_var = tk.StringVar(value="butcher-shop-canada-reviews.csv")
        self.max_reviews_var = tk.StringVar(value="All")
        self.shops_per_run_var = tk.StringVar(value="5")
        self.headed_var = tk.BooleanVar(value=False)
        self.concurrency_var = tk.IntVar(value=2)

        self._build()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(80, self._drain_log)
        self._set_idle_state()

    # ── layout ────────────────────────────────────────────────────────────

    def _build(self) -> None:
        outer = tk.Frame(self.root, bg=BG)
        outer.pack(fill=tk.BOTH, expand=True, padx=20, pady=16)

        # header
        header = tk.Frame(outer, bg=BG)
        header.pack(fill=tk.X, pady=(0, 14))
        tk.Label(
            header,
            text="📍  Maps Review Scraper",
            bg=BG,
            fg=TEXT,
            font=("Segoe UI", 16, "bold"),
            anchor="w",
        ).pack(side=tk.LEFT)

        body = tk.Frame(outer, bg=BG)
        body.pack(fill=tk.BOTH, expand=True)
        body.columnconfigure(0, weight=1, uniform="col")
        body.columnconfigure(1, weight=1, uniform="col")
        body.rowconfigure(0, weight=1)

        left = tk.Frame(body, bg=BG)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        right = tk.Frame(body, bg=BG)
        right.grid(row=0, column=1, sticky="nsew", padx=(10, 0))

        self._build_input(left)
        self._build_settings(left)
        self._build_actions(left)

        self._build_progress(right)
        self._build_workers(right)
        self._build_results(right)
        self._build_live_log(right)
        self._build_pills(right)

        # footer
        foot = tk.Frame(outer, bg=BG)
        foot.pack(fill=tk.X, pady=(12, 0))
        tk.Label(
            foot,
            text="Google Maps reviews → CSV  ·  pause & resume supported",
            bg=BG,
            fg=MUTED,
            font=("Segoe UI", 9),
        ).pack(side=tk.LEFT)
        help_fr = tk.Frame(foot, bg=BG)
        help_fr.pack(side=tk.RIGHT)

        def _link(text: str, command) -> None:
            lbl = tk.Label(
                help_fr,
                text=text,
                bg=BG,
                fg=MUTED,
                font=("Segoe UI", 9),
                cursor="hand2",
            )
            lbl.pack(side=tk.LEFT, padx=(12, 0))
            lbl.bind("<Button-1>", lambda _e: command())

        _link("⚙  Settings", self._show_settings_hint)
        _link("?  Help", self._show_help)

    def _build_input(self, parent: tk.Misc) -> None:
        card = Card(parent, "Input")
        card.pack(fill=tk.X, pady=(0, 12))
        self._file_row(card.body, "Shops file", self.shops_var, self._browse_shops)
        self._file_row(card.body, "Output CSV", self.output_var, self._browse_output, top=10)

    def _file_row(
        self,
        parent: tk.Misc,
        label: str,
        var: tk.StringVar,
        browse,
        top: int = 0,
    ) -> None:
        tk.Label(
            parent, text=label, bg=CARD, fg=MUTED, font=("Segoe UI", 9), anchor="w"
        ).pack(fill=tk.X, pady=(top, 4))
        row = tk.Frame(parent, bg=CARD)
        row.pack(fill=tk.X)
        entry = tk.Entry(
            row,
            textvariable=var,
            font=("Segoe UI", 10),
            bg=ENTRY_BG,
            fg=TEXT,
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BLUE,
        )
        entry.pack(side=tk.LEFT, fill=tk.X, expand=True, ipady=6, padx=(0, 8))
        self._ghost_btn(row, "Browse", browse).pack(side=tk.RIGHT)

    def _build_settings(self, parent: tk.Misc) -> None:
        card = Card(parent, "Settings")
        card.pack(fill=tk.X, pady=(0, 12))
        body = card.body

        # shops per run + max reviews (side by side)
        picks = tk.Frame(body, bg=CARD)
        picks.pack(fill=tk.X, pady=(0, 14))
        picks.columnconfigure(0, weight=1)
        picks.columnconfigure(1, weight=1)

        left_pick = tk.Frame(picks, bg=CARD)
        left_pick.grid(row=0, column=0, sticky="ew", padx=(0, 8))
        tk.Label(
            left_pick,
            text="Shops per run",
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 9),
        ).pack(anchor="w")
        shops_spin = tk.Spinbox(
            left_pick,
            from_=0,
            to=100_000,
            textvariable=self.shops_per_run_var,
            font=("Segoe UI", 10),
            bg=ENTRY_BG,
            fg=TEXT,
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=BORDER,
            highlightcolor=BLUE,
            buttonbackground="#F3F4F6",
            width=10,
            justify=tk.LEFT,
        )
        shops_spin.pack(anchor="w", pady=(4, 0), ipady=4, fill=tk.X)
        tk.Label(
            left_pick,
            text="Type any number · 0 = all shops",
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 8),
        ).pack(anchor="w", pady=(2, 0))

        right_pick = tk.Frame(picks, bg=CARD)
        right_pick.grid(row=0, column=1, sticky="ew", padx=(8, 0))
        tk.Label(
            right_pick,
            text="Max reviews per shop",
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 9),
        ).pack(anchor="w")
        review_opts = ("All", "10", "25", "50", "100", "200", "500")
        reviews_menu = tk.OptionMenu(right_pick, self.max_reviews_var, *review_opts)
        self._style_option_menu(reviews_menu)
        reviews_menu.pack(anchor="w", pady=(4, 0), ipady=2)

        # visible browser toggle
        row = tk.Frame(body, bg=CARD)
        row.pack(fill=tk.X, pady=(0, 14))
        tk.Label(
            row, text="Visible browser", bg=CARD, fg=MUTED, font=("Segoe UI", 9)
        ).pack(side=tk.LEFT)
        right = tk.Frame(row, bg=CARD)
        right.pack(side=tk.RIGHT)
        self.headed_label = tk.Label(
            right,
            text="Off (headless)",
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 9),
        )
        self.headed_label.pack(side=tk.RIGHT, padx=(8, 0))
        Toggle(right, self.headed_var, command=self._sync_headed_label).pack(side=tk.RIGHT)
        self._sync_headed_label()

        # concurrency slider
        crow = tk.Frame(body, bg=CARD)
        crow.pack(fill=tk.X)
        tk.Label(
            crow, text="Concurrency (Firefox browsers)", bg=CARD, fg=MUTED, font=("Segoe UI", 9)
        ).pack(side=tk.LEFT)
        self.conc_val_lbl = tk.Label(
            crow,
            text=str(self.concurrency_var.get()),
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 9, "bold"),
        )
        self.conc_val_lbl.pack(side=tk.RIGHT)

        scale = tk.Scale(
            body,
            from_=1,
            to=3,
            orient=tk.HORIZONTAL,
            variable=self.concurrency_var,
            showvalue=0,
            bg=CARD,
            fg=BLUE,
            troughcolor=TRACK,
            highlightthickness=0,
            bd=0,
            activebackground=BLUE,
            sliderrelief=tk.FLAT,
            length=280,
            command=lambda v: self.conc_val_lbl.config(text=str(int(float(v)))),
        )
        scale.pack(fill=tk.X, pady=(6, 0))

    def _count_shops_for_run(self, resume: bool = False) -> int:
        """Best-effort shop count for the progress bar before the scraper reports."""
        path = self._resolve_path(self.shops_var.get())
        if not path.exists():
            return 0
        try:
            if path.suffix.lower() == ".json":
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    rows = data
                elif isinstance(data, dict) and isinstance(data.get("shops"), list):
                    rows = data["shops"]
                else:
                    rows = []
                n = len(rows)
                # Prefer shop ids when counting completed for resume
                ids = []
                for i, raw in enumerate(rows):
                    if not isinstance(raw, dict):
                        ids.append(f"idx_{i}")
                        continue
                    sid = raw.get("id", raw.get("shopId"))
                    ids.append(sid if sid is not None and str(sid).strip() != "" else f"idx_{i}")
            else:
                with path.open(encoding="utf-8", newline="") as fh:
                    reader = csv.DictReader(fh)
                    rows = list(reader)
                n = len(rows)
                ids = []
                for i, raw in enumerate(rows):
                    sid = raw.get("id") or raw.get("shopId")
                    ids.append(sid if sid is not None and str(sid).strip() != "" else f"idx_{i}")
        except Exception:
            return 0

        if resume:
            checkpoint = self._resolve_path("scraper_checkpoint.json")
            completed: set[str] = set()
            if checkpoint.exists():
                try:
                    data = json.loads(checkpoint.read_text(encoding="utf-8"))
                    completed = set()
                    for item in data.get("completed_shops") or []:
                        if isinstance(item, dict) and item.get("id") is not None:
                            completed.add(str(item["id"]))
                        elif item is not None:
                            completed.add(str(item))
                    if not completed:
                        completed = {
                            str(k) for k in (data.get("completed_shop_keys") or [])
                        }
                except (OSError, json.JSONDecodeError, TypeError):
                    completed = set()
            n = sum(1 for sid in ids if str(sid) not in completed)

        limit = self._shops_per_run_arg()
        if limit is not None:
            n = min(n, limit)
        return max(0, n)

    def _build_actions(self, parent: tk.Misc) -> None:
        row = tk.Frame(parent, bg=BG)
        row.pack(fill=tk.X, pady=(4, 0))
        row.columnconfigure(0, weight=1)
        row.columnconfigure(1, weight=1)
        row.columnconfigure(2, weight=1)

        self.start_btn = self._action_btn(
            row, "▶  Start", BLUE, BLUE_HOVER, "white", self._start_fresh
        )
        self.start_btn.grid(row=0, column=0, sticky="ew", padx=(0, 6))

        self.resume_btn = self._action_btn(
            row, "↻  Resume", CARD, "#F9FAFB", TEXT, self._start_resume, border=True
        )
        self.resume_btn.grid(row=0, column=1, sticky="ew", padx=6)

        self.stop_btn = self._action_btn(
            row, "■  Stop", RED, RED_HOVER, "white", self._stop
        )
        self.stop_btn.grid(row=0, column=2, sticky="ew", padx=(6, 0))

    def _build_progress(self, parent: tk.Misc) -> None:
        card = Card(parent, "Progress")
        card.pack(fill=tk.X, pady=(0, 12))
        top = tk.Frame(card.body, bg=CARD)
        top.pack(fill=tk.X)
        self.progress_bar = _RoundedProgress(top, height=14)
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        self.pct_var = tk.StringVar(value="0%")
        tk.Label(
            top,
            textvariable=self.pct_var,
            bg=CARD,
            fg=TEXT,
            font=("Segoe UI", 11, "bold"),
            width=5,
            anchor="e",
        ).pack(side=tk.RIGHT)
        self.progress_detail = tk.StringVar(value="Waiting to start")
        tk.Label(
            card.body,
            textvariable=self.progress_detail,
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 9),
            anchor="w",
        ).pack(fill=tk.X, pady=(8, 0))

    def _build_workers(self, parent: tk.Misc) -> None:
        card = Card(parent, "Workers")
        card.pack(fill=tk.X, pady=(0, 12))
        self.workers_body = card.body
        self.current_shop_var = tk.StringVar(value="—")
        self._worker_rows = []
        self._rebuild_worker_bars(max(1, int(self.concurrency_var.get() or 1)))

    def _rebuild_worker_bars(self, count: int) -> None:
        for child in self.workers_body.winfo_children():
            child.destroy()
        self._worker_rows = []
        n = max(1, min(3, int(count)))
        for i in range(1, n + 1):
            wrap = tk.Frame(self.workers_body, bg=CARD)
            wrap.pack(fill=tk.X, pady=(0 if i == 1 else 8, 0))

            head = tk.Frame(wrap, bg=CARD)
            head.pack(fill=tk.X)
            title = tk.StringVar(value=f"Firefox {i}")
            tk.Label(
                head,
                textvariable=title,
                bg=CARD,
                fg=MUTED,
                font=("Segoe UI", 9),
                anchor="w",
            ).pack(side=tk.LEFT)
            pct_var = tk.StringVar(value="—")
            tk.Label(
                head,
                textvariable=pct_var,
                bg=CARD,
                fg=TEXT,
                font=("Segoe UI", 9, "bold"),
                anchor="e",
                width=5,
            ).pack(side=tk.RIGHT)

            shop_var = tk.StringVar(value="Idle")
            tk.Label(
                wrap,
                textvariable=shop_var,
                bg=CARD,
                fg=TEXT,
                font=("Segoe UI", 10, "bold"),
                anchor="w",
                wraplength=380,
                justify=tk.LEFT,
            ).pack(fill=tk.X, pady=(2, 4))

            bar_row = tk.Frame(wrap, bg=CARD)
            bar_row.pack(fill=tk.X)
            bar = _RoundedProgress(bar_row, height=10)
            bar.pack(side=tk.LEFT, fill=tk.X, expand=True)

            stage_var = tk.StringVar(value="")
            tk.Label(
                wrap,
                textvariable=stage_var,
                bg=CARD,
                fg=MUTED,
                font=("Segoe UI", 8),
                anchor="w",
            ).pack(fill=tk.X, pady=(2, 0))

            self._worker_rows.append(
                {
                    "id": i,
                    "title": title,
                    "shop": shop_var,
                    "pct": pct_var,
                    "stage": stage_var,
                    "bar": bar,
                }
            )

    def _set_worker_progress(
        self,
        worker_id: int,
        pct: float,
        shop_key: str,
        shop_label: str,
        stage: str,
    ) -> None:
        if worker_id < 1:
            return
        if worker_id > len(self._worker_rows):
            self._rebuild_worker_bars(worker_id)
        if worker_id > len(self._worker_rows):
            return
        row = self._worker_rows[worker_id - 1]
        name = re.sub(r"^\[\d+/\d+\]\s*", "", shop_label).strip() or shop_label
        row["shop"].set(name)
        row["stage"].set(stage)
        display = int(round(max(0.0, min(100.0, float(pct)))))
        row["pct"].set(f"{display}%")
        row["bar"].set(float(display))
        self._active_workers[worker_id] = {
            "key": str(shop_key),
            "name": name,
            "stage": stage,
        }
        # Keep a quick glance at the first busy worker in the overall detail
        if self._active_workers:
            first = next(iter(self._active_workers.values()))
            self.current_shop_var.set(first.get("name") or "—")

    def _set_worker_idle(self, worker_id: int) -> None:
        self._active_workers.pop(worker_id, None)
        if 1 <= worker_id <= len(self._worker_rows):
            row = self._worker_rows[worker_id - 1]
            row["shop"].set("Idle")
            row["stage"].set("")
            row["pct"].set("—")
            row["bar"].set(0.0)
        if self._active_workers:
            first = next(iter(self._active_workers.values()))
            self.current_shop_var.set(first.get("name") or "—")
        else:
            self.current_shop_var.set("—")

    def _reset_worker_bars(self) -> None:
        self._active_workers.clear()
        for row in self._worker_rows:
            row["shop"].set("Idle")
            row["stage"].set("")
            row["pct"].set("—")
            row["bar"].set(0.0)
        self.current_shop_var.set("—")

    def _build_results(self, parent: tk.Misc) -> None:
        card = Card(parent, "Shop results")
        card.pack(fill=tk.BOTH, expand=False, pady=(0, 12))
        self.results_summary_var = tk.StringVar(value="No run yet")
        tk.Label(
            card.body,
            textvariable=self.results_summary_var,
            bg=CARD,
            fg=MUTED,
            font=("Segoe UI", 9),
            anchor="w",
            justify=tk.LEFT,
        ).pack(fill=tk.X, pady=(0, 6))

        wrap = tk.Frame(card.body, bg=CARD)
        wrap.pack(fill=tk.BOTH, expand=True)
        self.results = tk.Text(
            wrap,
            wrap=tk.WORD,
            font=("Segoe UI", 9),
            bg=CARD,
            fg=LOG_FG,
            relief=tk.FLAT,
            highlightthickness=0,
            height=7,
            state=tk.DISABLED,
            cursor="arrow",
        )
        scroll = tk.Scrollbar(wrap, command=self.results.yview)
        self.results.configure(yscrollcommand=scroll.set)
        self.results.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.results.tag_configure("ok", foreground=GREEN_FG)
        self.results.tag_configure("fail", foreground=RED_SOFT_FG)
        self.results.tag_configure("incomplete", foreground=AMBER_FG)
        self.results.tag_configure("skipped_done", foreground=BLUE_PILL_FG)
        self.results.tag_configure("head", foreground=TEXT, font=("Segoe UI", 9, "bold"))
        self.results.tag_configure("muted", foreground=MUTED)

    def _build_live_log(self, parent: tk.Misc) -> None:
        card = Card(parent, "Live log")
        card.pack(fill=tk.BOTH, expand=True, pady=(0, 12))
        self.log = tk.Text(
            card.body,
            wrap=tk.WORD,
            font=("Consolas", 9),
            bg=CARD,
            fg=LOG_FG,
            relief=tk.FLAT,
            highlightthickness=0,
            height=8,
            state=tk.DISABLED,
            cursor="arrow",
        )
        self.log.pack(fill=tk.BOTH, expand=True)
        self.log.tag_configure("ts", foreground=MUTED)
        self.log.tag_configure("msg", foreground=LOG_FG)

    def _build_pills(self, parent: tk.Misc) -> None:
        row = tk.Frame(parent, bg=BG)
        row.pack(fill=tk.X)
        self.pill_host = row
        self._refresh_pills(running=False, checkpoint=False)

    # ── widget helpers ────────────────────────────────────────────────────

    def _style_option_menu(self, menu: tk.OptionMenu) -> None:
        menu.config(
            font=("Segoe UI", 10),
            bg=ENTRY_BG,
            fg=TEXT,
            activebackground=ENTRY_BG,
            highlightthickness=1,
            highlightbackground=BORDER,
            relief=tk.FLAT,
            width=8,
            anchor="w",
        )
        menu["menu"].config(font=("Segoe UI", 10), bg=CARD)

    def _ghost_btn(self, parent: tk.Misc, text: str, command) -> tk.Label:
        btn = tk.Label(
            parent,
            text=f"  {text}  ",
            bg="#F9FAFB",
            fg=TEXT,
            font=("Segoe UI", 9),
            relief=tk.FLAT,
            highlightthickness=1,
            highlightbackground=BORDER,
            cursor="hand2",
            pady=6,
        )
        btn.bind("<Button-1>", lambda _e: command())
        btn.bind("<Enter>", lambda _e: btn.config(bg="#F3F4F6"))
        btn.bind("<Leave>", lambda _e: btn.config(bg="#F9FAFB"))
        return btn

    def _action_btn(
        self,
        parent: tk.Misc,
        text: str,
        bg: str,
        hover: str,
        fg: str,
        command,
        border: bool = False,
    ) -> tk.Label:
        kw = {
            "text": text,
            "bg": bg,
            "fg": fg,
            "font": ("Segoe UI", 10, "bold"),
            "cursor": "hand2",
            "pady": 12,
        }
        btn = tk.Label(parent, **kw)
        if border:
            btn.configure(
                highlightthickness=1,
                highlightbackground=BORDER,
                highlightcolor=BORDER,
            )

        def on_enter(_e):
            if str(btn.cget("state")) != "disabled":
                btn.config(bg=hover)

        def on_leave(_e):
            if str(btn.cget("state")) != "disabled":
                btn.config(bg=bg)

        def on_click(_e):
            if str(btn.cget("state")) != "disabled":
                command()

        btn.bind("<Enter>", on_enter)
        btn.bind("<Leave>", on_leave)
        btn.bind("<Button-1>", on_click)
        btn._normal_bg = bg  # type: ignore[attr-defined]
        btn._hover_bg = hover  # type: ignore[attr-defined]
        return btn

    def _set_btn_enabled(self, btn: tk.Label, enabled: bool, fg: str | None = None) -> None:
        if enabled:
            btn.config(
                state=tk.NORMAL,
                cursor="hand2",
                bg=btn._normal_bg,  # type: ignore[attr-defined]
                fg=fg if fg is not None else btn.cget("fg"),
            )
        else:
            btn.config(state=tk.DISABLED, cursor="arrow", bg="#D1D5DB", fg="#9CA3AF")

    def _sync_headed_label(self) -> None:
        if self.headed_var.get():
            self.headed_label.config(text="On (visible)", fg=TEXT)
        else:
            self.headed_label.config(text="Off (headless)", fg=MUTED)

    def _refresh_pills(self, running: bool, checkpoint: bool) -> None:
        for w in self.pill_host.winfo_children():
            w.destroy()
        if running:
            Pill(self.pill_host, "Running", GREEN_BG, GREEN_FG, "●").pack(
                side=tk.LEFT, padx=(0, 8)
            )
        else:
            Pill(self.pill_host, "Idle", "#F3F4F6", MUTED, "○").pack(
                side=tk.LEFT, padx=(0, 8)
            )
        if checkpoint:
            Pill(
                self.pill_host, "Checkpoint saved", BLUE_PILL_BG, BLUE_PILL_FG, "✓"
            ).pack(side=tk.LEFT)

    def _set_idle_state(self) -> None:
        self.start_btn._normal_bg = BLUE  # type: ignore[attr-defined]
        self.resume_btn._normal_bg = CARD  # type: ignore[attr-defined]
        self.stop_btn._normal_bg = RED  # type: ignore[attr-defined]
        self._set_btn_enabled(self.start_btn, True, fg="white")
        self._set_btn_enabled(self.resume_btn, True, fg=TEXT)
        self._set_btn_enabled(self.stop_btn, False)
        self._refresh_pills(running=False, checkpoint=self._checkpoint_saved)

    def _set_running_state(self) -> None:
        self._set_btn_enabled(self.start_btn, False)
        self._set_btn_enabled(self.resume_btn, False)
        self.stop_btn._normal_bg = RED  # type: ignore[attr-defined]
        self._set_btn_enabled(self.stop_btn, True, fg="white")
        self._refresh_pills(running=True, checkpoint=self._checkpoint_saved)

    # ── file dialogs / help ───────────────────────────────────────────────

    def _resolve_path(self, value: str) -> Path:
        p = Path(value.strip())
        if not p.is_absolute():
            p = SCRIPT_DIR / p
        return p

    def _browse_shops(self) -> None:
        path = filedialog.askopenfilename(
            title="Select shops file",
            initialdir=SCRIPT_DIR,
            filetypes=[
                ("CSV / JSON", "*.csv;*.json"),
                ("CSV", "*.csv"),
                ("JSON", "*.json"),
                ("All", "*.*"),
            ],
        )
        if path:
            try:
                self.shops_var.set(str(Path(path).relative_to(SCRIPT_DIR)))
            except ValueError:
                self.shops_var.set(path)

    def _browse_output(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Output CSV",
            initialdir=SCRIPT_DIR,
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv"), ("All", "*.*")],
        )
        if path:
            try:
                self.output_var.set(str(Path(path).relative_to(SCRIPT_DIR)))
            except ValueError:
                self.output_var.set(path)

    def _show_settings_hint(self) -> None:
        messagebox.showinfo(
            "Settings",
            "Use the Settings card on the left to choose shops per run, "
            "max reviews, browser visibility, and concurrency.",
        )

    def _show_help(self) -> None:
        messagebox.showinfo(
            "Help",
            "1. Pick a shops CSV/JSON and an output CSV.\n"
            "2. Set Shops per run (e.g. 50). 0 = all shops in one run.\n"
            "3. Start scrapes the first batch (and clears the checkpoint).\n"
            "4. Resume continues unfinished shops, then the next batch "
            "(51–100, etc.) using the checkpoint.\n"
            "5. Stop ends the current run; in-flight shops are undone so Resume can retry them.",
        )

    # ── logging / progress ────────────────────────────────────────────────

    def _log_line(self, message: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        # If a line somehow arrives as Python unicode-escapes, decode for display
        text = message
        if "\\u" in text or "\\U" in text:
            try:
                text = text.encode("utf-8").decode("unicode_escape")
            except Exception:
                text = message
        self.log.configure(state=tk.NORMAL)
        self.log.insert(tk.END, f"{ts}  ", "ts")
        self.log.insert(tk.END, f"{text}\n", "msg")
        self.log.see(tk.END)
        self.log.configure(state=tk.DISABLED)

    def _clear_results(self) -> None:
        self._shop_results = []
        self._summary_counts = None
        self._in_summary = False
        self.results_summary_var.set("Waiting for shops…")
        self.results.configure(state=tk.NORMAL)
        self.results.delete("1.0", tk.END)
        self.results.configure(state=tk.DISABLED)

    def _append_result_line(self, status: str, text: str) -> None:
        tag = status if status in ("ok", "fail", "incomplete", "skipped_done") else "muted"
        self.results.configure(state=tk.NORMAL)
        self.results.insert(tk.END, text + "\n", tag)
        self.results.see(tk.END)
        self.results.configure(state=tk.DISABLED)

    def _record_shop_result(self, status: str, label: str, detail: str) -> None:
        name = re.sub(r"^\[\d+/\d+\]\s*", "", label).strip() or label
        self._shop_results.append(
            {"status": status, "name": name, "detail": detail, "label": label}
        )
        mark = {
            "ok": "OK",
            "fail": "FAIL",
            "incomplete": "INCOMPLETE",
            "skipped_done": "DONE",
        }.get(status, status.upper())
        self._append_result_line(status, f"[{mark}] {name} — {detail}")
        ok_n = sum(1 for r in self._shop_results if r["status"] == "ok")
        fail_n = sum(1 for r in self._shop_results if r["status"] == "fail")
        total = self._total_shops or len(self._shop_results)
        self.results_summary_var.set(
            f"So far: {ok_n} OK · {fail_n} failed · {len(self._shop_results)}/{total} finished"
        )

    def _apply_run_summary(
        self, ok_n: int, fail_n: int, inc_n: int, skip_n: int, total: int
    ) -> None:
        self._summary_counts = {
            "ok": ok_n,
            "fail": fail_n,
            "incomplete": inc_n,
            "skipped": skip_n,
            "total": total,
        }
        self.results_summary_var.set(
            f"Done: {ok_n} OK · {fail_n} failed · {inc_n} incomplete"
            + (f" · {skip_n} already done" if skip_n else "")
            + f"  (of {total})"
        )
        # Highlight incomplete/failed in progress detail
        if fail_n or inc_n:
            self.progress_detail.set(
                f"Finished with issues — {ok_n} OK, {fail_n} failed, {inc_n} incomplete"
            )
        else:
            self.progress_detail.set(f"All {ok_n} shops completed successfully")

    def _finalize_results_panel(self, exit_code: int | None) -> None:
        """Ensure the results panel has a clear end-state even if summary events were missed."""
        if self._summary_counts:
            c = self._summary_counts
            self.results_summary_var.set(
                f"Done: {c['ok']} OK · {c['fail']} failed · {c['incomplete']} incomplete"
                + (f" · {c['skipped']} already done" if c["skipped"] else "")
                + f"  (of {c['total']})"
            )
            return

        ok_n = sum(1 for r in self._shop_results if r["status"] == "ok")
        fail_n = sum(1 for r in self._shop_results if r["status"] == "fail")
        total = self._total_shops or len(self._shop_results)
        finished = len(self._shop_results)
        incomplete = max(0, total - finished) if total else 0

        if exit_code == 0 and not fail_n and not incomplete:
            self.results_summary_var.set(f"All {ok_n or total} shops completed successfully")
            self.progress_detail.set(f"All {ok_n or total} shops completed successfully")
        elif exit_code is None or incomplete:
            self.results_summary_var.set(
                f"Stopped — {ok_n} OK · {fail_n} failed · {incomplete} not finished"
                f"  (of {total})"
            )
            self.progress_detail.set(
                f"Stopped — {ok_n} OK, {fail_n} failed, {incomplete} incomplete"
            )
            if incomplete and total:
                self._append_result_line(
                    "incomplete",
                    f"…and {incomplete} shop(s) were not finished (use Resume)",
                )
        else:
            self.results_summary_var.set(
                f"Done: {ok_n} OK · {fail_n} failed  (of {total})"
            )
            self.progress_detail.set(
                f"Finished — {ok_n} OK, {fail_n} failed"
            )

    def _clear_log(self) -> None:
        self.log.configure(state=tk.NORMAL)
        self.log.delete("1.0", tk.END)
        self.log.configure(state=tk.DISABLED)

    def _set_progress(
        self,
        pct: float,
        done: int = 0,
        total: int = 0,
        *,
        detail: str | None = None,
    ) -> None:
        """Update bar + label from one authoritative percentage (0–100)."""
        if total > 0:
            self._done_count = max(0, done)
            self._total_shops = total
        elif self._total_shops > 0:
            total = self._total_shops
            done = self._done_count
        # Round once so the fill width and the "N%" text always match.
        display = int(round(max(0.0, min(100.0, float(pct)))))
        self.progress_bar.set(float(display))
        self.pct_var.set(f"{display}%")
        if detail is not None:
            self.progress_detail.set(detail)
        elif total <= 0:
            self.progress_detail.set("Working…")
        elif done >= total:
            self.progress_detail.set(f"Finished {total} of {total}")
        else:
            current = min(total, done + 1)
            self.progress_detail.set(f"Scraping shop {current} of {total}")

    def _handle_line(self, raw: str) -> None:
        line = raw.strip()
        if not line:
            return

        # Structured progress from scrape_main — sole source for the progress bar.
        prog = _PROGRESS_RE.match(line)
        if prog:
            pct_s, done_s, total_s, shop, stage = prog.groups()
            done_i, total_i = int(done_s), int(total_s)
            shop_name = re.sub(r"^\[\d+/\d+\]\s*", "", shop).strip()
            if shop_name:
                if not self._active_workers:
                    self.current_shop_var.set(shop_name)
            detail = None
            if total_i > 0 and done_i < total_i:
                current = done_i + 1
                if stage and stage not in ("starting",):
                    detail = f"Scraping shop {current} of {total_i} — {stage}"
                else:
                    detail = f"Scraping shop {current} of {total_i}"
            elif total_i > 0 and done_i >= total_i:
                detail = f"Finished {total_i} of {total_i}"
            self._set_progress(float(pct_s), done_i, total_i, detail=detail)
            # Mirror meaningful stages into the live log so it doesn't look frozen
            if stage and stage not in ("starting",) and stage != self._last_logged_stage:
                if (
                    stage.startswith("loading")
                    or stage.startswith("collecting")
                    or "retry" in stage
                    or stage.startswith("done")
                ):
                    # Log every loading update that changes the collected count prefix
                    short = stage.split(" · ")[0]
                    if short != self._last_logged_stage:
                        self._last_logged_stage = short
                        name = shop_name or "shop"
                        self._log_line(f"{name}: {stage}")
            return

        workers_n = _WORKERS_RE.match(line)
        if workers_n:
            self._rebuild_worker_bars(int(workers_n.group(1)))
            return

        wprog = _WORKER_PROGRESS_RE.match(line)
        if wprog:
            wid_s, pct_s, shop_key, shop_label, stage = wprog.groups()
            self._set_worker_progress(
                int(wid_s), float(pct_s), shop_key, shop_label, stage
            )
            return

        widle = _WORKER_IDLE_RE.match(line)
        if widle:
            self._set_worker_idle(int(widle.group(1)))
            return

        stop_inf = _STOP_INFLIGHT_RE.match(line)
        if stop_inf:
            keys = [k.strip() for k in stop_inf.group(1).split(",") if k.strip()]
            self._stop_purge_keys.update(keys)
            return

        reload_pass = _RELOAD_PASS_RE.match(line)
        if reload_pass:
            pass_n, count_n = (int(x) for x in reload_pass.groups())
            # Drop prior fail/incomplete rows — those shops are being re-scraped
            kept = [
                r
                for r in self._shop_results
                if r.get("status") not in ("fail", "incomplete")
            ]
            self._shop_results = kept
            self.results.configure(state=tk.NORMAL)
            self.results.delete("1.0", tk.END)
            for r in kept:
                status = r.get("status") or "muted"
                mark = {
                    "ok": "OK",
                    "fail": "FAIL",
                    "incomplete": "INCOMPLETE",
                    "skipped_done": "DONE",
                }.get(status, status.upper())
                name = r.get("name") or "?"
                detail = r.get("detail") or ""
                tag = status if status in ("ok", "fail", "incomplete", "skipped_done") else "muted"
                self.results.insert(tk.END, f"[{mark}] {name} — {detail}\n", tag)
            self.results.configure(state=tk.DISABLED)
            ok_n = sum(1 for r in kept if r["status"] == "ok")
            fail_n = sum(1 for r in kept if r["status"] == "fail")
            total = self._total_shops or len(kept)
            self.results_summary_var.set(
                f"Reload pass {pass_n}: retrying {count_n} shop(s) · "
                f"{ok_n} OK kept · {fail_n} failed · {len(kept)}/{total} finished"
            )
            self._log_line(
                f"Reload pass {pass_n}: re-running {count_n} failed/incomplete "
                f"shop(s) with all workers"
            )
            self._reset_worker_bars()
            return

        batch_plan = _BATCH_PLAN_RE.match(line)
        if batch_plan:
            this_run, already, left_after, total = (int(x) for x in batch_plan.groups())
            self._total_shops = this_run
            self._log_line(
                f"Batch plan: {this_run} this run · {already} already done · "
                f"{left_after} left after · {total} in file"
            )
            self.progress_detail.set(
                f"Batch of {this_run} · {already} done earlier · {total} total"
            )
            return

        batch_left = _BATCH_REMAINING_RE.match(line)
        if batch_left:
            remaining, total = (int(x) for x in batch_left.groups())
            if remaining > 0:
                self._checkpoint_saved = True
                self._refresh_pills(running=self._running, checkpoint=True)
                self.progress_detail.set(
                    f"Batch done — {remaining} of {total} left · click Resume"
                )
                self._log_line(
                    f"Batch finished — {remaining} shop(s) remaining. Click Resume."
                )
            else:
                self.progress_detail.set(f"All {total} shops finished")
                self._log_line(f"All {total} shops in the file are done")
            return

        # Per-shop structured result (also mirrored into the results panel)
        shop_res = _SHOP_RESULT_RE.match(line)
        if shop_res:
            status, label, detail = shop_res.groups()
            self._record_shop_result(status, label.strip(), detail.strip())
            name = re.sub(r"^\[\d+/\d+\]\s*", "", label).strip()
            if name:
                self.current_shop_var.set(name)
            self._log_line(f"{name} — {detail.strip()}")
            return

        if line == "::run_summary_begin::":
            self._in_summary = True
            # Rebuild results panel from authoritative summary rows
            self.results.configure(state=tk.NORMAL)
            self.results.delete("1.0", tk.END)
            self.results.insert(tk.END, "Final results\n", "head")
            self.results.configure(state=tk.DISABLED)
            self._shop_results = []
            return

        if line == "::run_summary_end::":
            self._in_summary = False
            return

        run_sum = _RUN_SUMMARY_RE.match(line)
        if run_sum:
            ok_n, fail_n, inc_n, skip_n, total = (int(x) for x in run_sum.groups())
            self._apply_run_summary(ok_n, fail_n, inc_n, skip_n, total)
            return

        sum_row = _SUMMARY_ROW_RE.match(line)
        if sum_row:
            status, idx, total, name, reviews, detail = sum_row.groups()
            mark = {
                "ok": "OK",
                "fail": "FAIL",
                "incomplete": "INCOMPLETE",
                "skipped_done": "DONE(earlier)",
            }.get(status, status.upper())
            reviews_bit = f" ({reviews} reviews)" if reviews not in ("", "None") else ""
            self._shop_results.append(
                {"status": status, "name": name, "detail": detail, "label": name}
            )
            self._append_result_line(
                status, f"[{mark}] [{idx}/{total}] {name}{reviews_bit} — {detail}"
            )
            return

        # Deep debug / status lines from scraper
        if line.startswith("::log::"):
            self._log_line(line[len("::log::") :])
            return

        # Ignore tqdm redraws — they fight with ::progress:: and cause jumps.
        if "Shops:" in line or "shop/s" in line or "it/s" in line:
            return

        pretty = line
        if "Measuring download speed" in line:
            pretty = "Measuring download speed…"
        elif "Measured ~" in line:
            pretty = line.lstrip(" ✅").strip()
        elif (
            "Auto concurrency" in line
            or "forced concurrency" in line
            or "Using forced concurrency" in line
            or "Using concurrency=" in line
            or "separate Firefox" in line
            or "Launching" in line and "Firefox" in line
        ):
            pretty = line.lstrip(" ⚙️").strip()
        elif "Review count mismatch" in line or "Review count matches" in line:
            pretty = line
        elif "Header shows" in line or "Scroll complete" in line or "Feed stopped" in line:
            pretty = line
        elif "checkpoint" in line.lower() or "Loaded checkpoint" in line:
            pretty = line
            self._checkpoint_saved = True
            self._refresh_pills(running=self._running, checkpoint=True)
        elif "Finished scraping" in line:
            pretty = line.lstrip(" ✅").strip()
            if self._total_shops:
                self._set_progress(100.0, self._total_shops, self._total_shops)
            self.progress_detail.set("Finished")
        elif "paused gracefully" in line.lower():
            pretty = "Paused — run Resume to continue"
            self.progress_detail.set("Paused")

        done_m = _SHOP_DONE_RE.search(line)
        if done_m:
            _idx, _total, name, detail = done_m.groups()
            self.current_shop_var.set(name.strip())
            pretty = f"{name.strip()} — {detail.strip()}"
            # Bar is updated only via ::progress:: (avoids [idx/total] misread as %).
        else:
            idx_m = _SHOP_INDEX_RE.search(line)
            if idx_m:
                name = re.sub(r"^.*?\]\s*", "", line)
                name = re.split(r"\s*—\s*", name, maxsplit=1)[0].strip()
                if name and not name.startswith("Shops"):
                    self.current_shop_var.set(name)

        self._log_line(pretty)

    def _drain_log(self) -> None:
        try:
            while True:
                item = self._log_q.get_nowait()
                if item is None:
                    self._on_process_done()
                else:
                    kind, payload = item
                    if kind == "chunk":
                        self._ingest_chunk(payload)
        except queue.Empty:
            pass
        self.root.after(80, self._drain_log)

    def _ingest_chunk(self, chunk: str) -> None:
        text = _ANSI_RE.sub("", chunk).replace("\r\n", "\n")
        # tqdm rewrites the same line with \r — treat each segment as a line.
        parts = re.split(r"[\r\n]+", text)
        # If chunk doesn't end with newline/\r, last part is incomplete.
        ends_complete = text.endswith("\n") or text.endswith("\r")
        if not parts:
            return
        if self._line_buf:
            parts[0] = self._line_buf + parts[0]
            self._line_buf = ""
        if not ends_complete:
            self._line_buf = parts[-1]
            parts = parts[:-1]
        for line in parts:
            self._handle_line(line)

    # ── process control ───────────────────────────────────────────────────

    def _max_reviews_arg(self) -> int:
        val = self.max_reviews_var.get().strip()
        if not val or val.lower() == "all":
            return 0
        return int(val)

    def _shops_per_run_arg(self) -> int | None:
        """Return --limit value, or None when scraping all shops (0 / empty / all)."""
        val = self.shops_per_run_var.get().strip()
        if not val or val.lower() == "all":
            return None
        try:
            n = int(val)
        except ValueError as err:
            raise ValueError("Shops per run must be a whole number (or 0 for all).") from err
        if n < 0:
            raise ValueError("Shops per run cannot be negative.")
        if n == 0:
            return None
        return n

    def _build_cmd(self, resume: bool) -> list[str]:
        shops = self._resolve_path(self.shops_var.get())
        if not shops.exists():
            raise ValueError(f"Shops file not found:\n{shops}")
        output = self._resolve_path(self.output_var.get())

        cmd = [
            sys.executable,
            "-u",
            str(SCRAPE_MAIN),
            "-s",
            str(shops),
            "-o",
            str(output),
            "--max-reviews",
            str(self._max_reviews_arg()),
            "--concurrency",
            str(self.concurrency_var.get()),
        ]
        limit = self._shops_per_run_arg()
        if limit is not None:
            cmd.extend(["--limit", str(limit)])
        if self.headed_var.get():
            cmd.append("--headed")
        if resume:
            cmd.append("--resume")
        # Keep checkpoint next to the project / output so the GUI and Resume share one file
        checkpoint = self._resolve_path("scraper_checkpoint.json")
        cmd.extend(["--checkpoint-file", str(checkpoint)])
        return cmd

    def _start_fresh(self) -> None:
        self._launch(resume=False)

    def _start_resume(self) -> None:
        self._launch(resume=True)

    def _launch(self, resume: bool) -> None:
        if self._running:
            return
        try:
            cmd = self._build_cmd(resume=resume)
        except ValueError as err:
            messagebox.showerror("Invalid options", str(err))
            return
        except Exception as err:
            messagebox.showerror("Invalid options", str(err))
            return

        self._resume_mode = resume
        self._line_buf = ""
        self._done_count = 0
        self._total_shops = self._count_shops_for_run(resume=resume)
        self._checkpoint_saved = False
        self._last_logged_stage = ""
        self._stop_purge_keys = set()
        self._user_stopped = False
        self._clear_log()
        self._clear_results()
        try:
            conc = max(1, min(3, int(self.concurrency_var.get() or 1)))
        except (TypeError, ValueError):
            conc = 1
        self._rebuild_worker_bars(conc)
        self._reset_worker_bars()
        if self._total_shops:
            self._set_progress(
                0.0,
                0,
                self._total_shops,
                detail=f"Starting… 0 of {self._total_shops}",
            )
        else:
            self.progress_bar.set(0)
            self.pct_var.set("0%")
            self.progress_detail.set("Starting…")
        self._log_line("Resume from checkpoint" if resume else "Starting scraper")
        self._running = True
        self._set_running_state()

        try:
            self._proc = subprocess.Popen(
                cmd,
                cwd=str(SCRIPT_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
            )
        except OSError as err:
            self._running = False
            self._set_idle_state()
            self.progress_detail.set("Failed to start")
            messagebox.showerror("Start failed", str(err))
            return

        self._reader = threading.Thread(target=self._read_output, daemon=True)
        self._reader.start()

    def _read_output(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            self._log_q.put(None)
            return
        try:
            while True:
                chunk = proc.stdout.read(256)
                if not chunk:
                    break
                self._log_q.put(("chunk", chunk))
        finally:
            proc.wait()
            self._log_q.put(None)

    def _on_process_done(self) -> None:
        # flush partial line
        if self._line_buf.strip():
            self._handle_line(self._line_buf)
            self._line_buf = ""

        code = self._proc.returncode if self._proc else None
        self._proc = None
        self._running = False
        self._set_idle_state()

        if self._user_stopped:
            purged = self._purge_stopped_shops()
            self._user_stopped = False
            self._stop_purge_keys.clear()
            self._reset_worker_bars()
            self._finalize_results_panel(code)
            if purged:
                self.progress_detail.set("Stopped — in-flight shops undone")
                self._log_line(
                    f"Stopped — removed in-flight shop(s) from checkpoint/CSV "
                    f"({purged})"
                )
            else:
                self.progress_detail.set("Stopped")
                self._log_line("Stopped — see Shop results for what completed")
            return

        self._finalize_results_panel(code)
        self._reset_worker_bars()

        if code == 0:
            if self._total_shops:
                self._set_progress(100.0, self._total_shops, self._total_shops)
            if self._summary_counts:
                c = self._summary_counts
                if c["fail"] or c["incomplete"]:
                    self._log_line(
                        f"Finished with issues — {c['ok']} OK, "
                        f"{c['fail']} failed, {c['incomplete']} incomplete"
                    )
                else:
                    self._log_line(
                        f"Finished successfully — all {c['ok']} shops OK"
                    )
            else:
                self._log_line("Finished successfully")
        elif code is None:
            self._log_line("Stopped — see Shop results for what completed")
        else:
            self.progress_detail.set(f"Exited ({code})")
            self._log_line(f"Process exited with code {code} — see Shop results")

    def _collect_purge_keys(self) -> set[str]:
        keys = set(self._stop_purge_keys)
        for info in self._active_workers.values():
            key = str(info.get("key") or "").strip()
            if key:
                keys.add(key)
        # Also read inflight file written by the scraper (survives hard kill)
        inflight = self._resolve_path("scraper_inflight.json")
        if inflight.exists():
            try:
                data = json.loads(inflight.read_text(encoding="utf-8"))
                for shop in data.get("shops") or []:
                    key = shop.get("key")
                    if key is None:
                        key = shop.get("id")
                    if key is not None and str(key).strip():
                        keys.add(str(key).strip())
            except (OSError, json.JSONDecodeError, TypeError):
                pass
        return keys

    def _purge_stopped_shops(self) -> str:
        """Undo checkpoint + CSV rows for shops that were running when Stop was clicked."""
        keys = self._collect_purge_keys()
        if not keys:
            # Clean leftover inflight file even if empty
            try:
                p = self._resolve_path("scraper_inflight.json")
                if p.exists():
                    p.unlink()
            except OSError:
                pass
            return ""

        removed_cp = 0
        removed_rows = 0
        checkpoint = self._resolve_path("scraper_checkpoint.json")
        if checkpoint.exists():
            try:
                data = json.loads(checkpoint.read_text(encoding="utf-8"))
                completed_raw = data.get("completed_shops")
                if completed_raw is None:
                    completed_raw = data.get("completed_shop_keys") or []
                incomplete_raw = data.get("incomplete_shops") or []

                def _keep_entries(raw_list: list) -> list:
                    kept = []
                    for item in raw_list:
                        if isinstance(item, dict):
                            eid = item.get("id")
                            if eid is not None and str(eid) not in keys:
                                kept.append(item)
                        else:
                            if item is not None and str(item) not in keys:
                                kept.append({"id": item, "name": ""})
                    return kept

                completed_kept = _keep_entries(list(completed_raw or []))
                incomplete_kept = _keep_entries(list(incomplete_raw or []))
                # In-flight stopped shops belong in incomplete, not completed
                for key in keys:
                    # leave them out of completed; optional incomplete add without name
                    pass
                before = len(list(completed_raw or [])) + len(list(incomplete_raw or []))
                after = len(completed_kept) + len(incomplete_kept)
                removed_cp = max(0, before - after)
                payload = {
                    "completed_shops": completed_kept,
                    "incomplete_shops": incomplete_kept,
                }
                tmp = checkpoint.with_suffix(checkpoint.suffix + ".tmp")
                tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                tmp.replace(checkpoint)
                self._checkpoint_saved = bool(completed_kept or incomplete_kept)
                self._refresh_pills(
                    running=False, checkpoint=bool(completed_kept or incomplete_kept)
                )
            except (OSError, json.JSONDecodeError, TypeError) as err:
                self._log_line(f"Could not update checkpoint after stop: {err}")

        output = self._resolve_path(self.output_var.get())
        if output.exists():
            try:
                with output.open(encoding="utf-8", newline="") as handle:
                    reader = csv.DictReader(handle)
                    fieldnames = reader.fieldnames
                    rows = list(reader) if fieldnames else []
                if fieldnames:
                    kept_rows = [
                        r for r in rows if str(r.get("shopId") or "") not in keys
                    ]
                    removed_rows = len(rows) - len(kept_rows)
                    if removed_rows:
                        tmp = output.with_suffix(output.suffix + ".tmp")
                        with tmp.open("w", encoding="utf-8", newline="") as handle:
                            writer = csv.DictWriter(
                                handle, fieldnames=fieldnames, extrasaction="ignore"
                            )
                            writer.writeheader()
                            writer.writerows(kept_rows)
                        tmp.replace(output)
            except (OSError, csv.Error) as err:
                self._log_line(f"Could not purge CSV after stop: {err}")

        try:
            inflight = self._resolve_path("scraper_inflight.json")
            if inflight.exists():
                inflight.unlink()
        except OSError:
            pass

        parts = []
        if keys:
            parts.append("keys " + ", ".join(sorted(keys)))
        if removed_cp:
            parts.append(f"{removed_cp} checkpoint")
        if removed_rows:
            parts.append(f"{removed_rows} CSV row(s)")
        return "; ".join(parts) if parts else ""

    def _stop(self) -> None:
        proc = self._proc
        if not proc or proc.poll() is not None:
            return
        self._user_stopped = True
        # Snapshot in-flight shops before the process dies
        self._stop_purge_keys.update(self._collect_purge_keys())
        self.progress_detail.set("Stopping…")
        names = [
            info.get("name") or info.get("key") or "?"
            for info in self._active_workers.values()
        ]
        if names:
            self._log_line(
                "Stop requested — will undo in-flight: " + ", ".join(names)
            )
        else:
            self._log_line("Stop requested")
        try:
            proc.terminate()
        except OSError:
            pass

        def _force() -> None:
            if proc.poll() is None:
                try:
                    proc.kill()
                except OSError:
                    pass

        self.root.after(3000, _force)

    def _on_close(self) -> None:
        if self._running:
            if not messagebox.askyesno(
                "Quit", "Scraper is still running. Stop it and quit?"
            ):
                return
            self._stop()
        self.root.destroy()


class _RoundedProgress(tk.Frame):
    """Reliable fill bar (Frame-based — avoids Canvas sizing glitches)."""

    def __init__(self, parent: tk.Misc, height: int = 14, **kwargs) -> None:
        super().__init__(parent, bg=CARD, height=height, **kwargs)
        self._pct = 0.0
        self._height = height
        self.pack_propagate(False)
        self.track = tk.Frame(self, bg=TRACK, height=height)
        self.track.pack(fill=tk.BOTH, expand=True)
        self.track.pack_propagate(False)
        self.fill = tk.Frame(self.track, bg=BLUE, height=height)
        self.fill.place(x=0, y=0, width=0, height=height)
        self.track.bind("<Configure>", lambda _e: self._redraw())

    def set(self, percent: float) -> None:
        self._pct = max(0.0, min(100.0, float(percent)))
        self._redraw()

    def _redraw(self) -> None:
        w = max(self.track.winfo_width(), 1)
        fw = int(w * self._pct / 100.0)
        self.fill.place(x=0, y=0, width=fw, height=self._height)


def main() -> None:
    root = tk.Tk()
    try:
        # Slightly sharper text on Windows
        from ctypes import windll

        windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass
    ScraperGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
