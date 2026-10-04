"""
MC Checker - Minecraft username availability checker.   [ qrz ]

Pipeline
  1. Local validation (3-16 chars, A-Z a-z 0-9 _)
  2. Lookup via api.minecraftservices.com
       - bulk  : POST /minecraft/profile/lookup/bulk/byname   (10 names / request)
       - single: GET  /minecraft/profile/lookup/name/{name}
     Name found      -> TAKEN
     Name not found  -> available candidate
  3. Optional verification of candidates (needs an access token)
       GET /minecraft/profile/name/{name}/available   (heavily rate limited)
     AVAILABLE / DUPLICATE / NOT_ALLOWED

Token: set the MINECRAFT_ACCESS_TOKEN environment variable, or paste it into the UI.
"""

import json
import os
import re
import secrets
import threading
import time
import tkinter as tk
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from queue import Empty, Queue
from tkinter import filedialog, messagebox, ttk
from tkinter import font as tkfont
from urllib.parse import quote

import requests

FOUNDER_TEXT = "MC Checker - The best of Humans. The one and The only king of the kings."
CONTACT_URL = "https://t.me/qrozyy"
WATERMARK = "qrz"
USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,16}$")

update_queue = Queue()


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass
class Config:
    lookup_url: str = "https://api.minecraftservices.com/minecraft/profile/lookup/name/{name}"
    bulk_url: str = "https://api.minecraftservices.com/minecraft/profile/lookup/bulk/byname"
    availability_url: str = "https://api.minecraftservices.com/minecraft/profile/name/{name}/available"
    use_bulk: bool = True
    verify_availability: bool = False
    token_env: str = "MINECRAFT_ACCESS_TOKEN"
    verify_requests_per_minute: float = 3.0
    self_test: bool = True

    # runtime tuning
    ids_file: str = "usernames.txt"
    max_workers: int = 4
    request_interval: float = 1.0      # seconds between lookup requests (global)
    timeout: float = 10.0
    bulk_size: int = 10                # API maximum
    max_retries: int = 4
    skip_checked: bool = True
    token: str = ""                    # explicit token (UI) overrides the env var

    def resolve_token(self):
        return (self.token or os.environ.get(self.token_env, "")).strip()


# --------------------------------------------------------------------------- #
# Result categories
# --------------------------------------------------------------------------- #
GREEN, RED, AMBER, PURPLE, ORANGE, PINK, GRAY = (
    "#3ddc97", "#ff6b6b", "#ffc857", "#b48cff", "#ff9f5a", "#ff5d8f", "#7d8597",
)

CATEGORY_META = {
    "available":       ("Available",   GREEN),
    "taken":           ("Taken",       RED),
    "invalid_pattern": ("Invalid",     AMBER),
    "improper":        ("Improper",    PURPLE),
    "rejected_policy": ("Not allowed", PURPLE),
    "forbidden":       ("Forbidden",   ORANGE),
    "unknown":         ("Unknown",     PINK),
    "failed":          ("Failed",      PINK),
    "skipped":         ("Skipped",     GRAY),
}

FILE_FOR = {
    "invalid_pattern": "invalid_pattern.txt",
    "improper": "improper.txt",
    "rejected_policy": "rejected_policy.txt",
    "forbidden": "forbidden.txt",
}


# --------------------------------------------------------------------------- #
# File helpers
# --------------------------------------------------------------------------- #
def save_line(filename, text):
    with open(filename, "a", encoding="utf-8") as f:
        f.write(text + "\n")


def load_checked_ids():
    checked = set()
    files = ["available.json", "taken.txt", "invalid_pattern.txt", "improper.txt",
             "rejected_policy.txt", "forbidden.txt", "unknown_errors.log"]
    for file in files:
        if not os.path.exists(file):
            continue
        try:
            if file.endswith(".json"):
                with open(file, "r", encoding="utf-8") as f:
                    for entry in json.load(f):
                        checked.add(entry["username"].lower())
            else:
                with open(file, "r", encoding="utf-8") as f:
                    for line in f:
                        value = line.strip().split(" | ")[0]
                        if value:
                            checked.add(value.lower())
        except Exception:
            pass
    return checked


def load_ids(path):
    ids = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8-sig") as f:
            for line in f:
                value = line.strip()
                if value and not value.startswith("#"):
                    ids.append(value)
    return ids


def save_available(username, verified, available_file="available.json"):
    data = []
    if os.path.exists(available_file):
        try:
            with open(available_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = []
    if username.lower() not in {e["username"].lower() for e in data}:
        data.append({
            "username": username,
            "date": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "verified": verified,
            "by": WATERMARK,
            "founder": FOUNDER_TEXT,
        })
    with open(available_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


# --------------------------------------------------------------------------- #
# Rate limiting
# --------------------------------------------------------------------------- #
class RateLimiter:
    """Reserves request slots so threads never burst. Supports penalties (backoff)."""

    def __init__(self, interval):
        self.interval = max(0.0, interval)
        self._next = 0.0
        self._lock = threading.Lock()

    def penalize(self, seconds):
        with self._lock:
            self._next = max(self._next, time.monotonic() + seconds)

    def wait(self, stop):
        """Block until a slot is free. Returns False if stopped while waiting."""
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next)
            self._next = slot + self.interval
        delay = slot - now
        if delay > 0:
            stop.wait(delay)
        return not stop.is_set()


# --------------------------------------------------------------------------- #
# Checker engine
# --------------------------------------------------------------------------- #
class Checker:
    def __init__(self, cfg, out_queue, stop_event):
        self.cfg = cfg
        self.out = out_queue
        self.stop = stop_event
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "MC-Checker/2.0 (username availability tool)",
            "Accept": "application/json",
        })
        self.lookup_rl = RateLimiter(cfg.request_interval)
        self.verify_rl = RateLimiter(60.0 / max(cfg.verify_requests_per_minute, 0.1))
        self.file_lock = threading.Lock()
        self.token = cfg.resolve_token()
        self.verify_enabled = False
        self.token_bad = False
        self.verify_q = Queue()
        self.checked = set()

    # ---- output helpers ---------------------------------------------------- #
    def note(self, message, level="info"):
        self.out.put({"type": "log", "level": level, "message": message})

    def emit(self, name, category, message):
        self.out.put({"type": "result", "name": name, "category": category, "message": message})

    def skip(self, name, message):
        self.emit(name, "skipped", message)

    def finalize(self, name, category, message, detail=None, verified=False):
        with self.file_lock:
            try:
                if category == "available":
                    save_available(name, verified)
                elif category == "taken":
                    save_line("taken.txt", f"{name} | {detail}" if detail else name)
                elif category in FILE_FOR:
                    save_line(FILE_FOR[category], name)
                elif category == "unknown":
                    save_line("unknown_errors.log", f"{name} | {message}")
                elif category == "failed":
                    save_line("failed.log", f"{name} | {message}")
            except OSError as e:
                self.note(f"Could not write result file: {e}", "error")
        if category not in ("failed", "skipped"):
            self.checked.add(name.lower())
        self.emit(name, category, message)

    def fail_all(self, names, err):
        for n in names:
            if err == "stopped":
                self.skip(n, "Stopped before request")
            else:
                self.finalize(n, "failed", err)

    # ---- HTTP -------------------------------------------------------------- #
    @staticmethod
    def _backoff(response, attempt):
        if response.status_code == 403:
            return 15.0
        try:
            return min(float(response.headers.get("Retry-After", "")), 120.0)
        except ValueError:
            return min(5.0 * (2 ** attempt), 60.0)

    def request(self, method, url, limiter, label, **kw):
        """Rate-limited request with retry on 429 / 403 / 5xx / network errors.
        Returns (response, error). error == 'stopped' if the scan was stopped."""
        last_err = "request failed"
        for attempt in range(self.cfg.max_retries + 1):
            if not limiter.wait(self.stop):
                return None, "stopped"
            try:
                r = self.session.request(method, url, timeout=self.cfg.timeout, **kw)
            except requests.RequestException as e:
                last_err = f"{type(e).__name__}: {e}"
                self.note(f"Network error on {label} ({type(e).__name__}) - retry {attempt + 1}", "warn")
                if self.stop.wait(min(2 ** attempt, 15)):
                    return None, "stopped"
                continue
            if r.status_code in (403, 429) or r.status_code >= 500:
                if attempt < self.cfg.max_retries:
                    wait = self._backoff(r, attempt)
                    limiter.penalize(wait)
                    self.note(f"HTTP {r.status_code} on {label} - backing off {wait:.0f}s", "warn")
                    if self.stop.wait(wait):
                        return None, "stopped"
                    continue
            return r, None
        return None, last_err

    # ---- lookups ----------------------------------------------------------- #
    def candidate(self, name):
        """Name was not found by lookup."""
        if self.verify_enabled:
            self.verify_q.put(name)
        else:
            self.finalize(name, "available", "AVAILABLE (unverified)")

    def classify_status(self, name, r):
        s = r.status_code
        if s == 403:
            self.finalize(name, "forbidden", "FORBIDDEN 403")
        elif s == 400:
            self.finalize(name, "invalid_pattern", "INVALID (400)")
        else:
            self.finalize(name, "unknown", f"UNHANDLED HTTP {s}")

    def lookup_single(self, name):
        r, err = self.request("GET", self.cfg.lookup_url.format(name=quote(name)),
                              self.lookup_rl, name)
        if r is None:
            return self.fail_all([name], err)
        if r.status_code == 200:
            try:
                profile = r.json()
                uuid = profile.get("id", "")
                shown = profile.get("name", name)
            except ValueError:
                uuid, shown = "", name
            self.finalize(name, "taken", f"TAKEN by {shown}" + (f" ({uuid[:8]}…)" if uuid else ""), uuid)
        elif r.status_code in (204, 404):
            self.candidate(name)
        else:
            self.classify_status(name, r)

    def lookup_bulk(self, names):
        r, err = self.request("POST", self.cfg.bulk_url, self.lookup_rl,
                              f"batch of {len(names)}", json=names)
        if r is None:
            return self.fail_all(names, err)
        if r.status_code == 200:
            try:
                profiles = r.json()
                found = {p["name"].lower(): p.get("id", "") for p in profiles}
            except (ValueError, KeyError, TypeError):
                for n in names:
                    self.finalize(n, "unknown", "Unexpected bulk response")
                return
            for n in names:
                uuid = found.get(n.lower())
                if uuid is not None:
                    self.finalize(n, "taken", f"TAKEN ({uuid[:8]}…)" if uuid else "TAKEN", uuid)
                else:
                    self.candidate(n)
        elif r.status_code == 400:
            self.note("Bulk request rejected (400) - falling back to single lookups", "warn")
            for n in names:
                if self.stop.is_set():
                    self.skip(n, "Stopped before request")
                else:
                    self.lookup_single(n)
        else:
            for n in names:
                self.classify_status(n, r)

    def job(self, names):
        try:
            if self.stop.is_set():
                for n in names:
                    self.skip(n, "Stopped before request")
                return
            if self.cfg.use_bulk:
                self.lookup_bulk(names)
            else:
                for n in names:
                    self.lookup_single(n)
        except Exception as e:  # never let a worker die silently
            for n in names:
                self.finalize(n, "failed", f"{type(e).__name__}: {e}")

    # ---- verification ------------------------------------------------------ #
    def verify_loop(self):
        while True:
            name = self.verify_q.get()
            if name is None:
                return
            try:
                if self.stop.is_set():
                    self.skip(name, "Stopped before verification")
                elif self.token_bad:
                    self.finalize(name, "available", "AVAILABLE (unverified - token rejected)")
                else:
                    self.verify(name)
            except Exception as e:
                self.finalize(name, "failed", f"{type(e).__name__}: {e}")

    def verify(self, name):
        r, err = self.request("GET", self.cfg.availability_url.format(name=quote(name)),
                              self.verify_rl, f"verify {name}",
                              headers={"Authorization": f"Bearer {self.token}"})
        if r is None:
            return self.fail_all([name], err)
        s = r.status_code
        if s == 200:
            try:
                status = str(r.json().get("status", "")).upper()
            except ValueError:
                status = ""
            if status == "AVAILABLE":
                self.finalize(name, "available", "AVAILABLE (verified)", verified=True)
            elif status == "DUPLICATE":
                self.finalize(name, "taken", "TAKEN (verified: DUPLICATE)")
            elif status == "NOT_ALLOWED":
                self.finalize(name, "rejected_policy", "NOT ALLOWED by Mojang")
            else:
                self.finalize(name, "unknown", f"Unexpected status '{status}'")
        elif s == 401:
            self.token_bad = True
            self.note("Access token rejected (401) - remaining names will be reported unverified", "error")
            self.finalize(name, "available", "AVAILABLE (unverified - token rejected)")
        elif s == 400:
            self.finalize(name, "improper", "IMPROPER name (400)")
        else:
            self.classify_status(name, r)

    # ---- self-test --------------------------------------------------------- #
    def run_self_test(self):
        cfg = self.cfg
        self.note("Self-test: probing API endpoints...")
        problems = []
        probe = "mcchk_" + secrets.token_hex(4)  # valid and practically certain to be unregistered

        r, err = self.request("GET", cfg.lookup_url.format(name="Notch"), self.lookup_rl, "self-test")
        if r is None:
            if err == "stopped":
                return False
            problems.append(f"lookup/name unreachable: {err}")
        elif r.status_code != 200:
            problems.append(f"lookup/name 'Notch' returned HTTP {r.status_code} (expected 200)")
        else:
            self.note("  lookup/name .......... OK (known account found)", "ok")

        r, err = self.request("GET", cfg.lookup_url.format(name=probe), self.lookup_rl, "self-test")
        if r is not None and r.status_code not in (204, 404):
            problems.append(f"lookup/name unregistered probe returned HTTP {r.status_code} (expected 404)")
        elif r is not None:
            self.note("  lookup/name (miss) ... OK (unregistered name not found)", "ok")

        if cfg.use_bulk:
            r, err = self.request("POST", cfg.bulk_url, self.lookup_rl, "self-test", json=["Notch", probe])
            if r is None:
                if err == "stopped":
                    return False
                problems.append(f"bulk lookup unreachable: {err}")
            elif r.status_code != 200:
                problems.append(f"bulk lookup returned HTTP {r.status_code} (expected 200)")
            else:
                try:
                    names = {p["name"].lower() for p in r.json()}
                except (ValueError, KeyError, TypeError):
                    names = None
                if names is None or "notch" not in names or probe.lower() in names:
                    problems.append("bulk lookup returned unexpected data")
                else:
                    self.note("  bulk lookup .......... OK", "ok")

        if self.verify_enabled:
            r, err = self.request("GET", cfg.availability_url.format(name="Notch"), self.verify_rl,
                                  "self-test", headers={"Authorization": f"Bearer {self.token}"})
            if r is None and err == "stopped":
                return False
            if r is not None and r.status_code == 200:
                self.note("  availability + token . OK", "ok")
            elif r is not None and r.status_code == 401:
                self.verify_enabled = False
                self.note("  availability: token rejected (401) - verification disabled for this run", "warn")
            else:
                code = r.status_code if r is not None else err
                self.note(f"  availability: unexpected result ({code}) - verification disabled", "warn")
                self.verify_enabled = False

        if problems:
            for p in problems:
                self.note("  FAIL: " + p, "error")
            return False
        self.note("Self-test passed.", "ok")
        return True

    # ---- main -------------------------------------------------------------- #
    def run(self):
        cfg = self.cfg
        try:
            raw = load_ids(cfg.ids_file)
            seen, names = set(), []
            for n in raw:
                if n.lower() not in seen:
                    seen.add(n.lower())
                    names.append(n)
            if not names:
                self.out.put({"type": "error", "message": f"No usernames found in {cfg.ids_file}."})
                return

            self.out.put({"type": "start", "total": len(names)})
            if len(raw) != len(names):
                self.note(f"Removed {len(raw) - len(names)} duplicate entries.", "dim")
            self.checked = load_checked_ids() if cfg.skip_checked else set()

            self.verify_enabled = cfg.verify_availability and bool(self.token)
            if cfg.verify_availability and not self.token:
                self.note(f"No token in ${cfg.token_env} or the UI field - verification off.", "warn")
            if self.verify_enabled:
                self.note(f"Verification on: ~{cfg.verify_requests_per_minute:g} names/min "
                          f"(only unregistered candidates are verified).", "dim")

            if cfg.self_test and not self.run_self_test():
                if self.stop.is_set():
                    self.out.put({"type": "stopped", "message": "Scan stopped by user."})
                else:
                    self.out.put({"type": "error", "message":
                                  "Self-test failed - results would not be reliable. "
                                  "Check your connection / API access, or disable the self-test."})
                return

            todo = []
            for n in names:
                if not USERNAME_RE.match(n):
                    self.finalize(n, "invalid_pattern", "INVALID - must be 3-16 chars: A-Z a-z 0-9 _")
                elif n.lower() in self.checked:
                    self.skip(n, "Already checked")
                else:
                    todo.append(n)

            verifier = None
            if self.verify_enabled:
                verifier = threading.Thread(target=self.verify_loop, daemon=True)
                verifier.start()

            size = max(1, min(cfg.bulk_size, 10)) if cfg.use_bulk else 1
            chunks = [todo[i:i + size] for i in range(0, len(todo), size)]
            executor = ThreadPoolExecutor(max_workers=cfg.max_workers)
            try:
                futures = [executor.submit(self.job, c) for c in chunks]
                for _ in as_completed(futures):
                    if self.stop.is_set():
                        break
            finally:
                executor.shutdown(wait=False, cancel_futures=True)

            if verifier:
                self.verify_q.put(None)
                verifier.join()

            if self.stop.is_set():
                self.out.put({"type": "stopped", "message": "Scan stopped by user."})
            else:
                self.out.put({"type": "done", "message": "Scan completed."})
        except Exception as e:
            self.out.put({"type": "error", "message": f"{type(e).__name__}: {e}"})
        finally:
            self.session.close()


# --------------------------------------------------------------------------- #
# UI
# --------------------------------------------------------------------------- #
BG, PANEL, CARD, FIELD = "#0f1117", "#151823", "#1c2030", "#10131c"
BORDER, TEXT, MUTED = "#2a3042", "#e8eaf2", "#8b93a7"
ACCENT, ACCENT_HOVER, HOVER, SELECT = "#6c7cff", "#8392ff", "#262c42", "#2c3556"

STATE_PILLS = {
    "idle":     ("IDLE", "#2a3042", MUTED),
    "scanning": ("SCANNING", "#26325f", "#9db0ff"),
    "done":     ("DONE", "#1b3d30", GREEN),
    "stopped":  ("STOPPED", "#40361a", AMBER),
    "error":    ("ERROR", "#40222a", RED),
}


def pick_font(root, candidates, fallback):
    families = set(tkfont.families(root))
    for name in candidates:
        if name in families:
            return name
    return fallback


class Tooltip:
    """Small hover tooltip."""

    def __init__(self, widget, text, font_family):
        self.widget, self.text, self.font = widget, text, font_family
        self.tip = None
        widget.bind("<Enter>", self.show, add="+")
        widget.bind("<Leave>", self.hide, add="+")

    def show(self, _e=None):
        if self.tip:
            return
        x = self.widget.winfo_rootx() + 24
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 4
        self.tip = tk.Toplevel(self.widget)
        self.tip.wm_overrideredirect(True)
        self.tip.wm_geometry(f"+{x}+{y}")
        tk.Label(self.tip, text=self.text, justify=tk.LEFT, bg="#242a40", fg=TEXT, padx=10, pady=6,
                 font=(self.font, 9), wraplength=280, highlightbackground=BORDER,
                 highlightthickness=1).pack()

    def hide(self, _e=None):
        if self.tip:
            self.tip.destroy()
            self.tip = None


class StatCard(tk.Frame):
    def __init__(self, parent, title, color, font_family):
        super().__init__(parent, bg=CARD, highlightbackground=BORDER, highlightthickness=1)
        tk.Frame(self, bg=color, height=3).pack(fill=tk.X)
        self.value = tk.Label(self, text="0", font=(font_family, 22, "bold"), fg=TEXT, bg=CARD)
        self.value.pack(anchor=tk.W, padx=12, pady=(8, 0))
        tk.Label(self, text=title.upper(), font=(font_family, 8, "bold"), fg=MUTED,
                 bg=CARD).pack(anchor=tk.W, padx=12, pady=(0, 10))

    def set(self, value):
        self.value.config(text=f"{value:,}")


class MinecraftUsernameCheckerUI:
    def __init__(self, root):
        self.root = root
        self.root.title("MC Checker - Minecraft Username Availability")
        self.root.geometry("1220x780")
        self.root.minsize(1040, 660)
        self.root.configure(bg=BG)

        self.font = pick_font(root, ("Segoe UI", "SF Pro Text", "Helvetica Neue", "Inter", "Ubuntu",
                                     "DejaVu Sans"), "TkDefaultFont")
        self.mono = pick_font(root, ("Cascadia Mono", "Consolas", "SF Mono", "Menlo",
                                     "DejaVu Sans Mono"), "Courier")

        self.running = False
        self.thread = None
        self.stop_event = None
        self.t0 = None
        self.rows = []              # (time, name, category, message)
        self.available_names = []
        self.defaults = Config()

        self.stats = {k: 0 for k in CATEGORY_META}
        self.stats.update(total=0, processed=0)

        self.build_styles()
        self.build_layout()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.bind("<Control-Return>", lambda _e: self.start_scan())
        self.root.bind("<Escape>", lambda _e: self.stop_scan())
        self.pulse_on = True
        self.root.after(100, self.process_queue)
        self.root.after(600, self.pulse)

    def pulse(self):
        if self.running:
            self.pulse_on = not self.pulse_on
            self.pill.config(text=("● " if self.pulse_on else "○ ") + "SCANNING")
        self.root.after(600, self.pulse)

    # ---- styles ------------------------------------------------------------ #
    def build_styles(self):
        F = self.font
        s = ttk.Style(self.root)
        s.theme_use("clam")
        self.root.option_add("*TCombobox*Listbox.background", FIELD)
        self.root.option_add("*TCombobox*Listbox.foreground", TEXT)
        self.root.option_add("*TCombobox*Listbox.selectBackground", ACCENT)
        self.root.option_add("*TCombobox*Listbox.font", (F, 10))

        s.configure(".", background=BG, foreground=TEXT, fieldbackground=FIELD, bordercolor=BORDER,
                    lightcolor=BG, darkcolor=BG, troughcolor=FIELD, focuscolor=PANEL, font=(F, 10))
        s.configure("TFrame", background=BG)
        s.configure("TLabel", background=BG, foreground=TEXT)
        s.configure("Panel.TFrame", background=PANEL)
        s.configure("Panel.TLabel", background=PANEL, foreground=TEXT)
        s.configure("Muted.TLabel", background=PANEL, foreground=MUTED, font=(F, 9))
        s.configure("Section.TLabel", background=PANEL, foreground=MUTED, font=(F, 8, "bold"))
        s.configure("Hint.TLabel", background=PANEL, foreground=MUTED, font=(F, 8))

        s.configure("TButton", background=CARD, foreground=TEXT, bordercolor=BORDER, padding=(12, 8),
                    relief="flat", focusthickness=0, font=(F, 10))
        s.map("TButton", background=[("active", HOVER), ("disabled", PANEL)],
              foreground=[("disabled", "#4d5468")])
        s.configure("Accent.TButton", background=ACCENT, foreground="#ffffff", bordercolor=ACCENT,
                    font=(F, 10, "bold"))
        s.map("Accent.TButton", background=[("active", ACCENT_HOVER), ("disabled", "#2a2f4a")],
              foreground=[("disabled", "#6b7290")], bordercolor=[("disabled", "#2a2f4a")])
        s.configure("Danger.TButton", background="#3a2229", foreground="#ff8e8e", bordercolor="#4d2a33")
        s.map("Danger.TButton", background=[("active", "#522d37"), ("disabled", PANEL)],
              foreground=[("disabled", "#4d5468")], bordercolor=[("disabled", BORDER)])
        s.configure("Ghost.TButton", background=BG, foreground=MUTED, bordercolor=BORDER, padding=(10, 6))
        s.map("Ghost.TButton", background=[("active", HOVER)], foreground=[("active", TEXT)])

        s.configure("TEntry", fieldbackground=FIELD, foreground=TEXT, insertcolor=TEXT,
                    bordercolor=BORDER, padding=6)
        s.map("TEntry", bordercolor=[("focus", ACCENT)])
        s.configure("TSpinbox", fieldbackground=FIELD, foreground=TEXT, insertcolor=TEXT,
                    bordercolor=BORDER, arrowcolor=MUTED, background=CARD, padding=5)
        s.map("TSpinbox", bordercolor=[("focus", ACCENT)])
        s.configure("TCombobox", fieldbackground=FIELD, foreground=TEXT, background=CARD,
                    bordercolor=BORDER, arrowcolor=MUTED, padding=5)
        s.map("TCombobox", fieldbackground=[("readonly", FIELD)], bordercolor=[("focus", ACCENT)],
              foreground=[("readonly", TEXT)])

        s.configure("TCheckbutton", background=PANEL, foreground=TEXT, indicatorbackground=FIELD,
                    indicatormargin=(0, 0, 6, 0))
        s.map("TCheckbutton", background=[("active", PANEL)],
              indicatorbackground=[("selected", ACCENT), ("active", HOVER)],
              indicatorforeground=[("selected", "#ffffff")])

        s.configure("TNotebook", background=BG, borderwidth=0, tabmargins=(0, 0, 0, 0))
        s.configure("TNotebook.Tab", background=BG, foreground=MUTED, padding=(18, 9), borderwidth=0,
                    font=(F, 10, "bold"))
        s.map("TNotebook.Tab", background=[("selected", CARD), ("active", PANEL)],
              foreground=[("selected", TEXT)])

        s.configure("Treeview", background=CARD, fieldbackground=CARD, foreground=TEXT, rowheight=27,
                    borderwidth=0, font=(F, 10))
        s.configure("Treeview.Heading", background=PANEL, foreground=MUTED, relief="flat",
                    padding=(10, 7), font=(F, 9, "bold"))
        s.map("Treeview", background=[("selected", SELECT)], foreground=[("selected", TEXT)])
        s.map("Treeview.Heading", background=[("active", HOVER)])
        s.layout("Treeview", [("Treeview.treearea", {"sticky": "nswe"})])

        for orient in ("Vertical", "Horizontal"):
            s.configure(f"{orient}.TScrollbar", background=BORDER, troughcolor=CARD, bordercolor=CARD,
                        arrowcolor=MUTED, relief="flat")
            s.map(f"{orient}.TScrollbar", background=[("active", "#3a4260")])

        s.configure("Accent.Horizontal.TProgressbar", troughcolor=FIELD, background=ACCENT,
                    bordercolor=FIELD, lightcolor=ACCENT, darkcolor=ACCENT, thickness=8)

    # ---- layout ------------------------------------------------------------ #
    def build_layout(self):
        F = self.font
        self.root.columnconfigure(1, weight=1)
        self.root.rowconfigure(1, weight=1)

        # header
        header = tk.Frame(self.root, bg=BG, padx=20, pady=14)
        header.grid(row=0, column=0, columnspan=2, sticky="ew")
        icon = tk.Canvas(header, width=36, height=36, bg=BG, highlightthickness=0)
        icon.pack(side=tk.LEFT)
        self.draw_logo(icon)
        titles = tk.Frame(header, bg=BG)
        titles.pack(side=tk.LEFT, padx=(12, 60))
        tk.Label(titles, text="MC Checker", font=(F, 17, "bold"), fg=TEXT, bg=BG).pack(anchor=tk.W)
        tk.Label(titles, text="Minecraft username availability", font=(F, 9), fg=MUTED,
                 bg=BG).pack(anchor=tk.W)
        badge = tk.Label(titles, text=f"  by {WATERMARK}  ", font=(F, 8, "bold"), fg=ACCENT, bg="#1d2240",
                         cursor="hand2")
        badge.place(relx=1.0, x=-2, y=2, anchor="ne")
        badge.bind("<Button-1>", lambda _e: webbrowser.open(CONTACT_URL))
        ttk.Button(header, text="Contact", style="Ghost.TButton",
                   command=lambda: webbrowser.open(CONTACT_URL)).pack(side=tk.RIGHT)
        self.pill = tk.Label(header, font=(F, 8, "bold"), padx=12, pady=4)
        self.pill.pack(side=tk.RIGHT, padx=12)
        self.set_state("idle")

        # sidebar
        side = tk.Frame(self.root, bg=PANEL, width=320, highlightbackground=BORDER, highlightthickness=1)
        side.grid(row=1, column=0, sticky="ns", padx=(20, 0), pady=(0, 12))
        side.grid_propagate(False)
        side.pack_propagate(False)
        self.build_sidebar(ttk.Frame(side, style="Panel.TFrame", padding=(16, 14)))

        # main
        main = tk.Frame(self.root, bg=BG)
        main.grid(row=1, column=1, sticky="nsew", padx=20, pady=(0, 12))
        self.build_main(main)

        # footer
        footer = tk.Frame(self.root, bg=BG, padx=20, pady=6)
        footer.grid(row=2, column=0, columnspan=2, sticky="ew")
        self.status_var = tk.StringVar(value="Ready.")
        tk.Label(footer, textvariable=self.status_var, font=(F, 9), fg=MUTED, bg=BG).pack(side=tk.LEFT)
        tk.Label(footer, text=WATERMARK, font=(F, 10, "bold"), fg=ACCENT, bg=BG).pack(side=tk.RIGHT)
        tk.Label(footer, text="Ctrl+Enter start  ·  Esc stop  ·  made by ", font=(F, 8), fg="#4d5468",
                 bg=BG).pack(side=tk.RIGHT)

    def draw_logo(self, canvas):
        grass = ["#4caf50", "#43a047", "#5dbb63", "#3f9a44"]
        dirt = ["#8d6e4a", "#795548", "#9a7b57", "#6d4c35"]
        size = 6
        for row in range(6):
            for col in range(6):
                pool = grass if row < 2 else dirt
                color = pool[(row * 7 + col * 3) % len(pool)]
                canvas.create_rectangle(col * size, row * size, col * size + size, row * size + size,
                                        fill=color, outline="")

    def set_state(self, kind):
        text, bg, fg = STATE_PILLS[kind]
        self.pill.config(text=text, bg=bg, fg=fg)

    def section(self, parent, text):
        ttk.Label(parent, text=text, style="Section.TLabel").pack(anchor=tk.W, pady=(14, 6))

    def build_sidebar(self, body):
        body.pack(fill=tk.BOTH, expand=True)
        d = self.defaults

        ttk.Label(body, text="SOURCE", style="Section.TLabel").pack(anchor=tk.W, pady=(0, 6))
        row = ttk.Frame(body, style="Panel.TFrame")
        row.pack(fill=tk.X)
        self.ids_entry = ttk.Entry(row)
        self.ids_entry.insert(0, d.ids_file)
        self.ids_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.browse_btn = ttk.Button(row, text="Browse", command=self.select_ids_file, padding=(10, 6))
        self.browse_btn.pack(side=tk.LEFT, padx=(6, 0))

        self.section(body, "PERFORMANCE")
        grid = ttk.Frame(body, style="Panel.TFrame")
        grid.pack(fill=tk.X)
        grid.columnconfigure((0, 1), weight=1, uniform="g")
        ttk.Label(grid, text="Threads", style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(grid, text="Delay (sec)", style="Muted.TLabel").grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.workers_spin = ttk.Spinbox(grid, from_=1, to=32, width=6)
        self.workers_spin.set(d.max_workers)
        self.workers_spin.grid(row=1, column=0, sticky="ew", pady=(3, 0))
        self.delay_spin = ttk.Spinbox(grid, from_=0.2, to=30, increment=0.2, width=6)
        self.delay_spin.set(d.request_interval)
        self.delay_spin.grid(row=1, column=1, sticky="ew", padx=(8, 0), pady=(3, 0))

        self.section(body, "API OPTIONS")
        self.bulk_var = tk.BooleanVar(value=d.use_bulk)
        self.verify_var = tk.BooleanVar(value=d.verify_availability)
        self.selftest_var = tk.BooleanVar(value=d.self_test)
        self.skip_var = tk.BooleanVar(value=d.skip_checked)
        self.option_checks = [
            ttk.Checkbutton(body, text="Bulk lookup (10 names / request)", variable=self.bulk_var),
            ttk.Checkbutton(body, text="Verify with availability endpoint", variable=self.verify_var,
                            command=self.refresh_token_state),
            ttk.Checkbutton(body, text="Run API self-test first", variable=self.selftest_var),
            ttk.Checkbutton(body, text="Skip previously checked names", variable=self.skip_var),
        ]
        tips = [
            "Looks up 10 names per request. Much faster and gentler on rate limits.",
            "Confirms each unregistered name with Mojang's availability endpoint. Needs an access token "
            "and is limited to a few names per minute. Catches blocked or reserved names.",
            "Probes the API with a known account and an unregistered name before scanning, "
            "so you don't get silently wrong results.",
            "Ignores names already saved in available.json, taken.txt and the other result files.",
        ]
        for cb, tip in zip(self.option_checks, tips):
            cb.pack(anchor=tk.W, pady=3)
            Tooltip(cb, tip, self.font)

        self.section(body, "ACCESS TOKEN (FOR VERIFICATION)")
        self.token_entry = ttk.Entry(body, show="•")
        self.token_entry.pack(fill=tk.X)
        self.token_hint = ttk.Label(body, style="Hint.TLabel", wraplength=280, justify=tk.LEFT)
        self.token_hint.pack(anchor=tk.W, pady=(5, 0))
        rpm = ttk.Frame(body, style="Panel.TFrame")
        rpm.pack(fill=tk.X, pady=(8, 0))
        ttk.Label(rpm, text="Verify rate / min", style="Muted.TLabel").pack(side=tk.LEFT)
        self.rpm_spin = ttk.Spinbox(rpm, from_=0.5, to=60, increment=0.5, width=6)
        self.rpm_spin.set(d.verify_requests_per_minute)
        self.rpm_spin.pack(side=tk.RIGHT)
        self.refresh_token_state()

        # actions pinned to bottom
        actions = ttk.Frame(body, style="Panel.TFrame")
        actions.pack(side=tk.BOTTOM, fill=tk.X)
        self.start_btn = ttk.Button(actions, text="▶  Start scan", style="Accent.TButton",
                                    command=self.start_scan)
        self.start_btn.pack(fill=tk.X)
        self.stop_btn = ttk.Button(actions, text="■  Stop", style="Danger.TButton",
                                   command=self.stop_scan, state=tk.DISABLED)
        self.stop_btn.pack(fill=tk.X, pady=(6, 0))
        sub = ttk.Frame(actions, style="Panel.TFrame")
        sub.pack(fill=tk.X, pady=(6, 0))
        sub.columnconfigure((0, 1), weight=1, uniform="b")
        self.save_btn = ttk.Button(sub, text="Save results", command=self.save_results, state=tk.DISABLED)
        self.save_btn.grid(row=0, column=0, sticky="ew", padx=(0, 3))
        self.clear_btn = ttk.Button(sub, text="Clear", command=self.clear_all)
        self.clear_btn.grid(row=0, column=1, sticky="ew", padx=(3, 0))

        self.lockable = [self.ids_entry, self.browse_btn, self.workers_spin, self.delay_spin,
                         self.token_entry, self.rpm_spin, self.clear_btn] + self.option_checks

    def build_main(self, main):
        F = self.font
        # stat cards
        cards = tk.Frame(main, bg=BG)
        cards.pack(fill=tk.X)
        self.cards = {}
        spec = [("available", "Available", GREEN), ("taken", "Taken", RED),
                ("invalid", "Invalid", AMBER), ("blocked", "Not allowed", PURPLE),
                ("errors", "Errors", PINK), ("skipped", "Skipped", GRAY)]
        for i, (key, title, color) in enumerate(spec):
            cards.columnconfigure(i, weight=1, uniform="c")
            card = StatCard(cards, title, color, F)
            card.grid(row=0, column=i, sticky="ew", padx=(0 if i == 0 else 5, 0 if i == 5 else 5))
            self.cards[key] = card

        # progress
        prog = tk.Frame(main, bg=CARD, highlightbackground=BORDER, highlightthickness=1, padx=14, pady=10)
        prog.pack(fill=tk.X, pady=(10, 10))
        top = tk.Frame(prog, bg=CARD)
        top.pack(fill=tk.X)
        self.progress_var = tk.StringVar(value="0 / 0   ·   0%")
        self.rate_var = tk.StringVar(value="")
        tk.Label(top, textvariable=self.progress_var, font=(F, 10, "bold"), fg=TEXT, bg=CARD).pack(side=tk.LEFT)
        tk.Label(top, textvariable=self.rate_var, font=(F, 9), fg=MUTED, bg=CARD).pack(side=tk.RIGHT)
        self.progress = ttk.Progressbar(prog, style="Accent.Horizontal.TProgressbar", maximum=100)
        self.progress.pack(fill=tk.X, pady=(8, 0))

        # tabs
        self.nb = ttk.Notebook(main)
        self.nb.pack(fill=tk.BOTH, expand=True)
        self.build_results_tab()
        self.build_available_tab()
        self.build_log_tab()

    def build_results_tab(self):
        tab = tk.Frame(self.nb, bg=CARD)
        self.nb.add(tab, text="Results")

        bar = tk.Frame(tab, bg=CARD, padx=12, pady=10)
        bar.pack(fill=tk.X)
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", lambda *_: self.render_rows())
        search = ttk.Entry(bar, textvariable=self.search_var)
        search.pack(side=tk.LEFT, fill=tk.X, expand=True)
        tk.Label(bar, text="🔍", bg=CARD).pack_forget()
        self.filter_var = tk.StringVar(value="All")
        combo = ttk.Combobox(bar, textvariable=self.filter_var, state="readonly", width=14,
                             values=["All"] + [m[0] for m in CATEGORY_META.values()])
        combo.pack(side=tk.LEFT, padx=(8, 0))
        combo.bind("<<ComboboxSelected>>", lambda _e: self.render_rows())

        wrap = tk.Frame(tab, bg=CARD)
        wrap.pack(fill=tk.BOTH, expand=True)
        cols = ("time", "username", "status", "details")
        self.tree = ttk.Treeview(wrap, columns=cols, show="headings", selectmode="extended")
        for col, text, width, stretch in (("time", "Time", 80, False), ("username", "Username", 190, False),
                                          ("status", "Status", 110, False), ("details", "Details", 400, True)):
            self.tree.heading(col, text=text, anchor=tk.W)
            self.tree.column(col, width=width, minwidth=60, stretch=stretch, anchor=tk.W)
        for key, (_label, color) in CATEGORY_META.items():
            self.tree.tag_configure(key, foreground=color)
        sb = ttk.Scrollbar(wrap, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.pack(fill=tk.BOTH, expand=True)
        self.empty = tk.Frame(wrap, bg=CARD)
        tk.Label(self.empty, text=WATERMARK, font=(self.font, 64, "bold"), fg="#232a40", bg=CARD).pack()
        tk.Label(self.empty, text="Pick a usernames file and press Start scan", font=(self.font, 10),
                 fg=MUTED, bg=CARD).pack(pady=(0, 4))
        tk.Label(self.empty, text="Results will appear here", font=(self.font, 9), fg="#4d5468",
                 bg=CARD).pack()
        self.empty.place(relx=0.5, rely=0.5, anchor="center")

        self.menu = tk.Menu(self.tree, tearoff=0, bg=CARD, fg=TEXT, activebackground=SELECT,
                            activeforeground=TEXT, bd=0)
        self.menu.add_command(label="Copy username", command=self.copy_selected_names)
        self.menu.add_command(label="Copy row", command=self.copy_selected_rows)
        self.menu.add_separator()
        self.menu.add_command(label="Open on NameMC", command=self.open_namemc)
        self.tree.bind("<Button-3>", self.show_menu)
        self.tree.bind("<Button-2>", self.show_menu)
        self.tree.bind("<Double-1>", lambda _e: self.open_namemc())
        self.tree.bind("<Control-c>", self.copy_selected_rows)
        self.tree.bind("<Command-c>", self.copy_selected_rows)

    def build_available_tab(self):
        tab = tk.Frame(self.nb, bg=CARD)
        self.avail_tab = tab
        self.nb.add(tab, text="Available (0)")

        bar = tk.Frame(tab, bg=CARD, padx=12, pady=10)
        bar.pack(fill=tk.X)
        tk.Label(bar, text="Names confirmed free appear here", font=(self.font, 9), fg=MUTED,
                 bg=CARD).pack(side=tk.LEFT)
        ttk.Button(bar, text="Export…", command=self.export_available, padding=(10, 5)).pack(side=tk.RIGHT)
        ttk.Button(bar, text="Copy all", command=self.copy_available, padding=(10, 5)).pack(side=tk.RIGHT, padx=6)

        wrap = tk.Frame(tab, bg=CARD)
        wrap.pack(fill=tk.BOTH, expand=True)
        self.avail_list = tk.Listbox(wrap, bg=CARD, fg=GREEN, selectbackground=SELECT, selectforeground=TEXT,
                                     font=(self.mono, 12), relief=tk.FLAT, highlightthickness=0,
                                     activestyle="none", selectmode=tk.EXTENDED, borderwidth=0)
        sb = ttk.Scrollbar(wrap, orient=tk.VERTICAL, command=self.avail_list.yview)
        self.avail_list.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.avail_list.pack(fill=tk.BOTH, expand=True, padx=(12, 0))

    def build_log_tab(self):
        tab = tk.Frame(self.nb, bg=CARD)
        self.nb.add(tab, text="Log")
        self.log_text = tk.Text(tab, wrap=tk.WORD, state=tk.DISABLED, bg=CARD, fg=TEXT, relief=tk.FLAT,
                                font=(self.mono, 10), padx=14, pady=10, highlightthickness=0,
                                insertbackground=TEXT, selectbackground=SELECT)
        sb = ttk.Scrollbar(tab, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        for tag, color in (("info", TEXT), ("ok", GREEN), ("warn", AMBER), ("error", RED), ("dim", MUTED)):
            self.log_text.tag_configure(tag, foreground=color)
        self.log_text.tag_configure("ts", foreground="#4d5468")

    # ---- small helpers ----------------------------------------------------- #
    def refresh_token_state(self):
        env_name = self.defaults.token_env
        found = bool(os.environ.get(env_name))
        if self.verify_var.get():
            note = f"✓ Found in ${env_name}" if found else f"Not found in ${env_name} - paste a token above."
            note += "  Pasted token takes priority."
        else:
            note = "Only used when verification is enabled. The token is never saved to disk."
        self.token_hint.config(text=note)

    def append_log(self, message, level="info"):
        stamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, f"{stamp}  ", "ts")
        self.log_text.insert(tk.END, message + "\n", level)
        self.log_text.config(state=tk.DISABLED)

    def trim_log(self):
        lines = int(self.log_text.index("end-1c").split(".")[0])
        if lines > 6000:
            self.log_text.config(state=tk.NORMAL)
            self.log_text.delete("1.0", f"{lines - 5000}.0")
            self.log_text.config(state=tk.DISABLED)

    def row_matches(self, row):
        _t, name, category, message = row
        wanted = self.filter_var.get()
        if wanted != "All" and CATEGORY_META[category][0] != wanted:
            return False
        q = self.search_var.get().strip().lower()
        return not q or q in name.lower() or q in message.lower()

    def tree_values(self, row):
        t, name, category, message = row
        return (t, name, CATEGORY_META[category][0], message)

    def render_rows(self):
        self.tree.delete(*self.tree.get_children())
        shown = 0
        for row in reversed(self.rows):
            if self.row_matches(row):
                self.tree.insert("", tk.END, values=self.tree_values(row), tags=(row[2],))
                shown += 1
                if shown >= 20000:
                    break

    # ---- actions ----------------------------------------------------------- #
    def select_ids_file(self):
        path = filedialog.askopenfilename(title="Select usernames file",
                                          filetypes=[("Text files", "*.txt"), ("All files", "*")])
        if path:
            self.ids_entry.delete(0, tk.END)
            self.ids_entry.insert(0, path)

    def read_config(self):
        try:
            workers = int(float(self.workers_spin.get()))
            delay = float(self.delay_spin.get())
            rpm = float(self.rpm_spin.get())
            if not (1 <= workers <= 32 and 0.1 <= delay <= 60 and 0.1 <= rpm <= 60):
                raise ValueError
        except ValueError:
            messagebox.showerror("Invalid settings",
                                 "Threads must be 1-32, delay 0.1-60 s, verify rate 0.1-60 per minute.")
            return None
        return Config(
            ids_file=self.ids_entry.get().strip() or self.defaults.ids_file,
            max_workers=workers, request_interval=delay,
            use_bulk=self.bulk_var.get(), verify_availability=self.verify_var.get(),
            verify_requests_per_minute=rpm, self_test=self.selftest_var.get(),
            skip_checked=self.skip_var.get(), token=self.token_entry.get().strip(),
        )

    def start_scan(self):
        if self.running:
            return
        cfg = self.read_config()
        if cfg is None:
            return
        if not os.path.exists(cfg.ids_file):
            messagebox.showerror("File not found", f"Usernames file not found:\n{cfg.ids_file}")
            return
        if cfg.verify_availability and not cfg.resolve_token():
            if not messagebox.askyesno(
                    "No access token",
                    f"Verification is enabled but no token was found in the field or ${cfg.token_env}.\n\n"
                    "Continue without verification?"):
                return

        self.clear_all(announce=False)
        self.running = True
        self.t0 = time.monotonic()
        self.set_state("scanning")
        self.status_var.set("Scanning...")
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.save_btn.config(state=tk.DISABLED)
        for w in self.lockable:
            w.config(state=tk.DISABLED)
        self.append_log(f"Starting scan: {cfg.ids_file}  ·  {cfg.max_workers} threads  ·  "
                        f"{'bulk' if cfg.use_bulk else 'single'} lookup", "dim")

        self.stop_event = threading.Event()
        checker = Checker(cfg, update_queue, self.stop_event)
        self.thread = threading.Thread(target=checker.run, daemon=True)
        self.thread.start()

    def stop_scan(self):
        if not self.running or not self.stop_event:
            return
        self.stop_event.set()
        self.status_var.set("Stopping - waiting for active requests...")
        self.append_log("Stop requested.", "warn")
        self.stop_btn.config(state=tk.DISABLED)

    def finish(self, state, status):
        self.running = False
        self.set_state(state)
        self.status_var.set(status)
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.save_btn.config(state=tk.NORMAL if self.rows else tk.DISABLED)
        for w in self.lockable:
            w.config(state=tk.NORMAL)
        self.refresh_metrics()

    def clear_all(self, announce=True):
        if self.running:
            return
        self.stats = {k: 0 for k in CATEGORY_META}
        self.stats.update(total=0, processed=0)
        self.rows.clear()
        self.available_names.clear()
        self.tree.delete(*self.tree.get_children())
        self.avail_list.delete(0, tk.END)
        self.nb.tab(self.avail_tab, text="Available (0)")
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete("1.0", tk.END)
        self.log_text.config(state=tk.DISABLED)
        self.rate_var.set("")
        self.save_btn.config(state=tk.DISABLED)
        self.empty.place(relx=0.5, rely=0.5, anchor="center")
        self.set_state("idle")
        self.refresh_metrics()
        if announce:
            self.status_var.set("Cleared.")

    # ---- queue / metrics --------------------------------------------------- #
    def handle(self, item):
        kind = item.get("type")
        if kind == "start":
            self.stats["total"] = item["total"]
            self.status_var.set(f"Scanning {item['total']:,} usernames...")
            self.append_log(f"Loaded {item['total']:,} unique usernames.", "dim")
        elif kind == "log":
            self.append_log(item["message"], item.get("level", "info"))
        elif kind == "result":
            category = item["category"] if item["category"] in CATEGORY_META else "unknown"
            self.stats["processed"] += 1
            self.stats[category] += 1
            row = (datetime.now().strftime("%H:%M:%S"), item["name"], category, item["message"])
            self.rows.append(row)
            self.empty.place_forget()
            if self.row_matches(row):
                self.tree.insert("", 0, values=self.tree_values(row), tags=(category,))
            if category == "available":
                self.available_names.append(item["name"])
                self.avail_list.insert(tk.END, item["name"])
            level = {"available": "ok", "taken": "dim", "skipped": "dim"}.get(category, "warn")
            self.append_log(f"{item['name']:<18} {item['message']}", level)
        elif kind == "done":
            n = self.stats["available"]
            self.append_log(f"Scan complete - {n} available.", "ok")
            self.finish("done", f"Scan complete - {n:,} available name(s) found.")
        elif kind == "stopped":
            self.append_log(item.get("message", "Scan stopped."), "warn")
            self.finish("stopped", "Scan stopped.")
        elif kind == "error":
            self.append_log(item.get("message", "Error."), "error")
            self.finish("error", item.get("message", "Error."))

    def refresh_metrics(self):
        s = self.stats
        self.cards["available"].set(s["available"])
        self.cards["taken"].set(s["taken"])
        self.cards["invalid"].set(s["invalid_pattern"])
        self.cards["blocked"].set(s["improper"] + s["rejected_policy"])
        self.cards["errors"].set(s["forbidden"] + s["unknown"] + s["failed"])
        self.cards["skipped"].set(s["skipped"])
        self.nb.tab(self.avail_tab, text=f"Available ({s['available']:,})")

        total, done = s["total"], s["processed"]
        percent = (done / total * 100) if total else 0
        self.progress.config(value=percent)
        self.progress_var.set(f"{done:,} / {total:,}   ·   {percent:.0f}%")
        if self.running and self.t0 and done:
            elapsed = max(time.monotonic() - self.t0, 0.001)
            rate = done / elapsed
            remaining = (total - done) / rate if rate else 0
            eta = time.strftime("%H:%M:%S", time.gmtime(min(remaining, 359999)))
            self.rate_var.set(f"{rate:.1f} names/s   ·   ETA {eta}")

    def process_queue(self):
        handled = 0
        try:
            while handled < 300:
                item = update_queue.get_nowait()
                handled += 1
                self.handle(item)
        except Empty:
            pass
        if handled:
            self.refresh_metrics()
            self.trim_log()
            self.log_text.see(tk.END)
        if (self.running and self.thread and not self.thread.is_alive() and update_queue.empty()):
            self.finish("error", "Scan ended unexpectedly.")
        self.root.after(30 if handled >= 300 else 100, self.process_queue)

    # ---- clipboard / export ------------------------------------------------ #
    def copy_text(self, text, what):
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        self.status_var.set(f"Copied {what} to clipboard.")

    def copy_available(self):
        if self.available_names:
            self.copy_text("\n".join(self.available_names), f"{len(self.available_names)} names")

    def show_menu(self, event):
        row = self.tree.identify_row(event.y)
        if row:
            if row not in self.tree.selection():
                self.tree.selection_set(row)
            self.menu.tk_popup(event.x_root, event.y_root)

    def selected_names(self):
        return [self.tree.item(i, "values")[1] for i in self.tree.selection()]

    def copy_selected_names(self):
        names = self.selected_names()
        if names:
            self.copy_text("\n".join(names), f"{len(names)} name(s)")

    def open_namemc(self):
        names = self.selected_names()
        if names:
            webbrowser.open(f"https://namemc.com/profile/{names[0]}")

    def copy_selected_rows(self, _event=None):
        lines = ["\t".join(self.tree.item(i, "values")) for i in self.tree.selection()]
        if lines:
            self.copy_text("\n".join(lines), f"{len(lines)} rows")
        return "break"

    def export_available(self):
        if not self.available_names:
            messagebox.showinfo("Nothing to export", "No available names yet.")
            return
        path = filedialog.asksaveasfilename(title="Export available names", defaultextension=".txt",
                                            initialfile="available_names.txt",
                                            filetypes=[("Text files", "*.txt"), ("All files", "*")])
        if path:
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"# MC Checker | {WATERMARK}\n" + "\n".join(self.available_names) + "\n")
            self.status_var.set(f"Exported {len(self.available_names)} names to {path}")

    def save_results(self):
        path = filedialog.asksaveasfilename(
            title="Save scan results", defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("CSV", "*.csv"), ("All files", "*")])
        if not path:
            return
        try:
            if path.lower().endswith(".csv"):
                import csv
                with open(path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(["time", "username", "status", "details"])
                    for row in self.rows:
                        w.writerow(self.tree_values(row))
            else:
                s = self.stats
                with open(path, "w", encoding="utf-8") as f:
                    f.write(f"Minecraft Username Scan Results\nGenerated by MC Checker  |  {WATERMARK}\n\n")
                    f.write(f"Total usernames: {s['total']}\nProcessed: {s['processed']}\n")
                    for key, (label, _c) in CATEGORY_META.items():
                        f.write(f"{label}: {s[key]}\n")
                    f.write("\nAvailable names:\n")
                    f.writelines(n + "\n" for n in self.available_names)
                    f.write("\nAll results:\n")
                    for row in self.rows:
                        f.write("  ".join(self.tree_values(row)) + "\n")
            self.append_log(f"Results saved to {path}", "ok")
            self.status_var.set(f"Results saved to {path}")
        except Exception as e:
            messagebox.showerror("Save failed", f"Unable to save results: {e}")

    def on_close(self):
        if self.running and self.stop_event:
            self.stop_event.set()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    MinecraftUsernameCheckerUI(root)
    root.mainloop()
