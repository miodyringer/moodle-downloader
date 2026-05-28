# moodle-sync

A tiny local web app for pulling Moodle course materials onto your disk. Login,
pick courses, watch the sync stream live in your browser. Credentials live in
the OS keychain — never on disk.

## Quick start

```bash
uv sync
uv run moodle-sync
```

That's it. A browser tab opens at `http://127.0.0.1:8765/`.

1. Enter Moodle URL, username, password → click **Sign in**.
2. Tick the courses you want, set a download directory → click **Save & start sync**.
3. Watch the live log. Files land in `./moodle_documents/<course>/<section>/…`.

Next time you run it, click **Use saved password** — no re-typing.

## Headless mode

Once you've configured at least once via the web UI, you can sync from a
terminal (cron, SSH, CI):

```bash
uv run moodle-sync-cli
```

It reads the saved config + keychain password and runs the same sync engine.

## Where things live

| What                         | Where                                                           |
| ---------------------------- | --------------------------------------------------------------- |
| Selected courses, URL, dirs  | `~/Library/Application Support/moodle-sync/config.json` (macOS) |
| Password                     | OS keychain (Keychain Access / Credential Manager / Secret Service) |
| Downloaded files             | `./moodle_documents/` (or whatever you set in the UI)           |
| Sync state (incremental hashes) | `<download_dir>/.sync_state.json`                            |

The config file never contains your password. To wipe it, click **Forget saved
password** in the UI, or:

```bash
uv run python -c "from moodle_sync.config import Config, clear_password; \
  clear_password(Config.load().username)"
```

## Options

```bash
uv run moodle-sync --port 9000 --no-browser
uv run moodle-sync --host 0.0.0.0   # expose on LAN (be careful)
```

## Project layout

```
src/moodle_sync/
  config.py   # JSON config + keychain password helpers
  core.py     # MoodleClient (login, course discovery) + Syncer (downloads)
  web.py      # FastAPI app + single-page UI + SSE progress stream
  cli.py      # Headless `moodle-sync-cli` entrypoint
```

The sync engine is UI-agnostic: it emits structured progress events
(`section`, `folder`, `file_done`, `file_skip`, `file_fail`, `summary`) that the
web UI streams over Server-Sent Events.

## Notes

- Incremental: files unchanged since the last run are MD5-checked and skipped.
- Folder recursion is depth-limited (default 5) to avoid Moodle navigation loops.
- Tested against `moodle.dhbw-mannheim.de`, but the login flow is plain Moodle —
  any Moodle without external SSO should work.
