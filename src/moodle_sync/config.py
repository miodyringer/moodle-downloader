"""Configuration + credential storage.

Credentials live in the OS keychain via `keyring`. Non-sensitive settings
(moodle URL, selected courses, download dir) live in a JSON file under the
user config dir.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

import keyring
from platformdirs import user_config_dir

APP_NAME = "moodle-sync"
KEYRING_SERVICE = "moodle-sync"


def config_dir() -> Path:
    p = Path(user_config_dir(APP_NAME, appauthor=False))
    p.mkdir(parents=True, exist_ok=True)
    return p


def config_path() -> Path:
    return config_dir() / "config.json"


@dataclass
class Course:
    id: str
    name: str
    url: str
    excluded_sections: list[str] = field(default_factory=list)
    # keys = section name, values = activity names excluded within that section
    excluded_activities: dict[str, list[str]] = field(default_factory=dict)


@dataclass
class Config:
    moodle_url: str = "https://moodle.dhbw-mannheim.de/"
    username: str = ""
    download_dir: str = "moodle_documents"
    max_folder_depth: int = 5
    courses: list[Course] = field(default_factory=list)

    @classmethod
    def load(cls) -> "Config":
        p = config_path()
        if not p.exists():
            return cls()
        try:
            data = json.loads(p.read_text())
        except (OSError, json.JSONDecodeError):
            return cls()
        courses = [
            Course(
                id=c["id"],
                name=c["name"],
                url=c["url"],
                excluded_sections=c.get("excluded_sections", []),
                excluded_activities=(
                    raw if isinstance((raw := c.get("excluded_activities", {})), dict) else {}
                ),
            )
            for c in data.pop("courses", [])
        ]
        # Drop any legacy 'password' field that might be sitting in the file
        data.pop("password", None)
        return cls(
            courses=courses,
            **{k: v for k, v in data.items() if k in cls.__annotations__},
        )

    def save(self) -> Path:
        p = config_path()
        payload = asdict(self)
        p.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        try:
            p.chmod(0o600)
        except OSError:
            pass
        return p


def get_password(username: str) -> Optional[str]:
    if not username:
        return None
    try:
        return keyring.get_password(KEYRING_SERVICE, username)
    except Exception:
        return None


def set_password(username: str, password: str) -> bool:
    if not username or not password:
        return False
    try:
        keyring.set_password(KEYRING_SERVICE, username, password)
        return True
    except Exception:
        return False


def clear_password(username: str) -> None:
    if not username:
        return
    try:
        keyring.delete_password(KEYRING_SERVICE, username)
    except Exception:
        pass
