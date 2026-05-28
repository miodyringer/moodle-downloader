"""Headless CLI fallback (no browser).

Useful for cron / SSH:  uv run moodle-sync-cli
"""

from __future__ import annotations

import sys
from pathlib import Path

from .config import Config, get_password
from .core import LoginError, MoodleClient, Syncer


def _print(event: dict) -> None:
    kind = event.get("kind")
    msg = event.get("msg", "")
    if kind == "course":
        print(f"\n## {msg}")
    elif kind == "section":
        print(f"  § {msg}")
    elif kind == "folder":
        print("    " + "  " * event.get("depth", 0) + f"└ {msg}/")
    elif kind == "file_done":
        print(f"      ↓ {msg}")
    elif kind == "file_skip":
        print(f"      = {msg}")
    elif kind == "file_fail":
        print(f"      × {msg}")
    elif kind == "summary":
        print(f"\n✓ {msg}\n  → {event.get('dir')}")
    elif kind == "error":
        print(f"!! {msg}", file=sys.stderr)
    else:
        if msg:
            print(f"   {msg}")


def main() -> int:
    cfg = Config.load()
    if not cfg.username:
        print("No saved username. Run `uv run moodle-sync` first.", file=sys.stderr)
        return 1
    pw = get_password(cfg.username)
    if not pw:
        print("No password in keychain. Run `uv run moodle-sync` first.", file=sys.stderr)
        return 1
    if not cfg.courses:
        print("No courses selected. Run `uv run moodle-sync` first.", file=sys.stderr)
        return 1

    client = MoodleClient(cfg.moodle_url)
    try:
        client.login(cfg.username, pw)
    except LoginError as e:
        print(f"Login failed: {e}", file=sys.stderr)
        return 2

    download_dir = Path(cfg.download_dir)
    if not download_dir.is_absolute():
        download_dir = Path.cwd() / download_dir
    syncer = Syncer(client, download_dir, cfg.max_folder_depth, progress=_print)
    result = syncer.sync_courses(cfg.courses)
    return 0 if result.failed == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
