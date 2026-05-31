"""Moodle sync engine.

Refactored from the original `moodle_sync.py` to:
  - separate session/login from sync logic
  - emit progress as structured events for any UI to consume
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Optional
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .config import Course

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

# Progress event = dict with at minimum {"kind": str, "msg": str}.
# kinds: "info" | "section" | "folder" | "file_done" | "file_skip" | "file_fail" | "error" | "summary" | "course"
ProgressFn = Callable[[dict], None]


def _noop(_: dict) -> None:
    pass


class LoginError(RuntimeError):
    pass


@dataclass
class SyncResult:
    downloaded: int = 0
    skipped: int = 0
    failed: int = 0


class MoodleClient:
    """Authenticated Moodle session."""

    def __init__(self, moodle_url: str):
        self.moodle_url = moodle_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

    def login(self, username: str, password: str) -> None:
        r = self.session.get(f"{self.moodle_url}/login/index.php")
        soup = BeautifulSoup(r.text, "html.parser")
        token_el = soup.find("input", {"name": "logintoken"})
        data = {"username": username, "password": password, "anchor": ""}
        if token_el:
            data["logintoken"] = token_el["value"]
        resp = self.session.post(
            f"{self.moodle_url}/login/index.php", data=data, allow_redirects=True
        )
        body = resp.text.lower()
        # The Moodle login page renders a "loginerrors" div on bad credentials,
        # but also renders a "logout" link in the footer regardless. Use both signals.
        if "loginerror" in body or resp.url.endswith("/login/index.php"):
            raise LoginError("Login failed — check username/password.")
        if "logout" not in body:
            raise LoginError("Login failed — Moodle did not establish a session.")

    # ----- discovery -----

    def fetch_available_courses(self) -> list[Course]:
        seen: dict[str, Course] = {}
        for page_url in (
            f"{self.moodle_url}/my/",
            f"{self.moodle_url}/my/courses.php",
            f"{self.moodle_url}/course/index.php",
        ):
            try:
                page = self.session.get(page_url, timeout=30)
            except requests.RequestException:
                continue
            soup = BeautifulSoup(page.text, "html.parser")
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "/course/view.php?id=" not in href:
                    continue
                cid = parse_qs(urlparse(href).query).get("id", [None])[0]
                name = link.get_text(strip=True)
                if not cid or not name or len(name) < 3:
                    continue
                if cid in seen:
                    continue
                seen[cid] = Course(id=cid, name=name, url=urljoin(self.moodle_url, href))

        # Improve names with the actual course page H1.
        for c in seen.values():
            try:
                page = self.session.get(c.url, timeout=30)
                soup = BeautifulSoup(page.text, "html.parser")
                h1 = soup.find("h1")
                if h1:
                    better = h1.get_text(strip=True)
                    if better and len(better) > 3:
                        c.name = better
            except requests.RequestException:
                pass
        return list(seen.values())


# ---------- sync engine ----------


_INVALID = '<>:"/\\|?*'


def _sanitize(name: str) -> str:
    for ch in _INVALID:
        name = name.replace(ch, "_")
    return name.replace("&amp;", "&").strip() or "untitled"


def _is_lang_link(text: str, href: str) -> bool:
    if "?lang=" in href or "&lang=" in href:
        return True
    if not text:
        return False
    return bool(re.search(r"‎\([a-z]{2}(_[a-z]{2})?\)‎", text))


def _file_md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()


class Syncer:
    def __init__(
        self,
        client: MoodleClient,
        download_dir: Path,
        max_folder_depth: int = 5,
        progress: ProgressFn = _noop,
    ):
        self.client = client
        self.session = client.session
        self.moodle_url = client.moodle_url
        self.download_dir = Path(download_dir)
        self.max_folder_depth = max_folder_depth
        self.progress = progress
        self.state_file = self.download_dir / ".sync_state.json"
        self.state = self._load_state()
        self._cancel = False

    def cancel(self) -> None:
        self._cancel = True

    def _load_state(self) -> dict:
        if self.state_file.exists():
            try:
                return json.loads(self.state_file.read_text())
            except (OSError, json.JSONDecodeError):
                pass
        return {"files": {}, "last_sync": None}

    def _save_state(self) -> None:
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.state["last_sync"] = datetime.now().isoformat()
        self.state_file.write_text(json.dumps(self.state, indent=2, ensure_ascii=False))

    # ----- public -----

    def sync_courses(self, courses: Iterable[Course]) -> SyncResult:
        total = SyncResult()
        for course in courses:
            if self._cancel:
                break
            r = self._sync_course(course)
            total.downloaded += r.downloaded
            total.skipped += r.skipped
            total.failed += r.failed
        self._save_state()
        self.progress(
            {
                "kind": "summary",
                "downloaded": total.downloaded,
                "skipped": total.skipped,
                "failed": total.failed,
                "dir": str(self.download_dir.absolute()),
                "msg": (
                    f"Done. {total.downloaded} new, {total.skipped} unchanged, "
                    f"{total.failed} failed."
                ),
            }
        )
        return total

    # ----- internal -----

    def _resolve_course_name(self, url: str) -> str:
        try:
            page = self.session.get(url, timeout=30)
            soup = BeautifulSoup(page.text, "html.parser")
            h1 = soup.find("h1")
            if h1:
                return h1.get_text(strip=True)
            t = soup.find("title")
            if t:
                txt = t.get_text(strip=True)
                return txt.split(": ", 1)[1] if ": " in txt else txt
        except requests.RequestException:
            pass
        cid = parse_qs(urlparse(url).query).get("id", ["unknown"])[0]
        return f"Course {cid}"

    def _sync_course(self, course: Course) -> SyncResult:
        result = SyncResult()
        name = _sanitize(self._resolve_course_name(course.url))
        course_dir = self.download_dir / name
        self.progress({"kind": "course", "msg": name})

        try:
            page = self.session.get(course.url, timeout=30)
        except requests.RequestException as e:
            self.progress({"kind": "error", "msg": f"Could not load course: {e}"})
            result.failed += 1
            return result

        soup = BeautifulSoup(page.text, "html.parser")
        sections = soup.find_all("li", {"data-sectionname": True})
        self.progress({"kind": "info", "msg": f"{len(sections)} section(s)"})

        for section in sections:
            if self._cancel:
                break
            section_name = section.get("data-sectionname", "").strip()
            if not section_name:
                continue
            if section_name in (course.excluded_sections or []):
                self.progress({"kind": "info", "msg": f"(skipped section: {section_name})"})
                continue
            section_dir = course_dir / _sanitize(section_name)
            self.progress({"kind": "section", "msg": section_name})

            for activity in section.find_all("div", class_="activity-item"):
                if self._cancel:
                    break
                activity_name = activity.get("data-activityname")
                if not activity_name:
                    a = activity.find("a", class_="aalink")
                    activity_name = a.get_text(strip=True) if a else None
                if not activity_name:
                    continue
                if activity_name in (course.excluded_activities or []):
                    self.progress({"kind": "info", "msg": f"(skipped: {activity_name})"})
                    continue
                link = activity.find("a", href=True)
                if not link:
                    continue
                href = link["href"]
                full_url = urljoin(self.moodle_url, href)

                if "/mod/folder/view.php" in href:
                    sub = self._sync_folder(full_url, activity_name, section_dir)
                    result.downloaded += sub.downloaded
                    result.skipped += sub.skipped
                    result.failed += sub.failed
                elif "/mod/resource/view.php" in href or "/pluginfile.php/" in href:
                    ok = self._download_file(full_url, activity_name, section_dir)
                    if ok is True:
                        result.downloaded += 1
                    elif ok is False:
                        result.skipped += 1
                    else:
                        result.failed += 1
                time.sleep(0.2)
        return result

    def _sync_folder(
        self,
        url: str,
        name: str,
        parent: Path,
        visited: Optional[set[str]] = None,
        depth: int = 0,
    ) -> SyncResult:
        result = SyncResult()
        visited = visited or set()
        if depth >= self.max_folder_depth or url in visited:
            return result
        visited.add(url)
        folder_dir = parent / _sanitize(name)
        self.progress({"kind": "folder", "msg": name, "depth": depth})

        try:
            page = self.session.get(url, timeout=30)
        except requests.RequestException as e:
            self.progress({"kind": "error", "msg": f"Folder {name}: {e}"})
            result.failed += 1
            return result

        soup = BeautifulSoup(page.text, "html.parser")

        # Restrict link discovery to the file-manager content area so that
        # sidebar / navigation links are not mistaken for sub-folders or files.
        content_root = (
            soup.find(class_="filemanager")
            or soup.find(id="folder_tree0")
            or soup.find(class_="fp-content")
            or soup.find(attrs={"role": "main"})
            or soup
        )

        for a in content_root.find_all("a", href=True):
            if self._cancel:
                break
            href = a["href"]
            text = a.get_text(strip=True)
            if not text or _is_lang_link(text, href) or "◀︎" in text or "▶︎" in text:
                continue
            full = urljoin(self.moodle_url, href)
            if "/pluginfile.php/" in href:
                ok = self._download_file(full, text, folder_dir)
                if ok is True:
                    result.downloaded += 1
                elif ok is False:
                    result.skipped += 1
                else:
                    result.failed += 1
            elif "/mod/folder/view.php" in href and full not in visited:
                sub = self._sync_folder(full, text, folder_dir, visited, depth + 1)
                result.downloaded += sub.downloaded
                result.skipped += sub.skipped
                result.failed += sub.failed
            time.sleep(0.15)
        return result

    def _download_file(self, url: str, name: str, target_dir: Path) -> Optional[bool]:
        """Returns True if downloaded, False if unchanged, None if failed."""
        name = _sanitize(name)
        if len(name) < 3:
            name = urlparse(url).path.rsplit("/", 1)[-1] or "file"
        if "." not in name:
            try:
                head = self.session.head(url, allow_redirects=True, timeout=10)
                ct = head.headers.get("content-type", "")
                if "pdf" in ct:
                    name += ".pdf"
                elif "zip" in ct:
                    name += ".zip"
            except requests.RequestException:
                pass

        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / name
        key = str(path.relative_to(self.download_dir))

        if path.exists():
            current = _file_md5(path)
            if self.state["files"].get(key) == current:
                self.progress({"kind": "file_skip", "msg": name})
                return False

        try:
            r = self.session.get(url, stream=True, timeout=60)
            r.raise_for_status()
            with path.open("wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            self.state["files"][key] = _file_md5(path)
            self.progress({"kind": "file_done", "msg": name})
            return True
        except (requests.RequestException, OSError) as e:
            self.progress({"kind": "file_fail", "msg": f"{name}: {e}"})
            return None
