#!/usr/bin/env python3
"""Lebanon Telegram breaking-news watcher.

Polls public t.me/s/<channel> pages on a short interval, scores new posts
with keyword rules from config.json, and (when configured) POSTs a batched
webhook to wake a Grok Bot routine. Uses no AI. Never logs secrets.
"""

from __future__ import annotations

import argparse
import fcntl
import html as htmlmod
import json
import logging
import os
import signal
import re
import sys
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    import requests
except ImportError:  # pragma: no cover - stdlib fallback
    requests = None
    import urllib.request
    import urllib.error


DIR = Path(__file__).resolve().parent
CONFIG_PATH = DIR / "config.json"
STATE_PATH = DIR / "state.json"
SHABBAT_CACHE_PATH = DIR / "shabbat-cache.json"
LOG_PATH = DIR / "watcher.log"
PID_PATH = DIR / "watcher.pid"
LOCK_PATH = DIR / "watcher.lock"

ISO = "%Y-%m-%dT%H:%M:%S%z"


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open(encoding="utf-8") as f:
        return json.load(f)


def setup_logging(verbose: bool = False) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = RotatingFileHandler(LOG_PATH, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    # Avoid adding handlers twice if --loop restarts in-process (it doesn't, but tests might).
    if not any(isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "") == str(LOG_PATH) for h in root.handlers):
        root.addHandler(fh)
    # Always stream in CI so the Actions log shows cycles; never a TTY there.
    if (
        sys.stdout.isatty()
        or verbose
        or os.environ.get("GITHUB_ACTIONS")
        or os.environ.get("LEB_LOG_STDOUT")
    ):
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        root.addHandler(sh)


def tzinfo(cfg: dict) -> ZoneInfo:
    return ZoneInfo(cfg.get("timezone") or "Asia/Jerusalem")


def now_local(cfg: dict) -> datetime:
    return datetime.now(tzinfo(cfg))


def parse_hhmm(value: str) -> tuple[int, int]:
    h, m = value.split(":")
    return int(h), int(m)


def in_active_window(cfg: dict, when: datetime | None = None) -> bool:
    when = when or now_local(cfg)
    sh, sm = parse_hhmm(cfg["active_start"])
    eh, em = parse_hhmm(cfg["active_end"])
    start = when.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end = when.replace(hour=eh, minute=em, second=0, microsecond=0)
    return start <= when <= end


def seconds_until_window(cfg: dict, when: datetime | None = None) -> int:
    when = when or now_local(cfg)
    if in_active_window(cfg, when):
        return 0
    sh, sm = parse_hhmm(cfg["active_start"])
    start_today = when.replace(hour=sh, minute=sm, second=0, microsecond=0)
    if when < start_today:
        target = start_today
    else:
        target = start_today + timedelta(days=1)
    return max(1, int((target - when).total_seconds()))


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"seen": {}, "recent": [], "pending": [], "webhook": {}, "shabbat_entry": {}}
    try:
        with STATE_PATH.open(encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("state file unreadable (%s); starting fresh", exc)
        return {"seen": {}, "recent": [], "pending": [], "webhook": {}, "shabbat_entry": {}}
    data.setdefault("seen", {})
    data.setdefault("recent", [])
    data.setdefault("pending", [])
    data.setdefault("webhook", {})
    data.setdefault("shabbat_entry", {})
    return data


def save_state(state: dict[str, Any]) -> None:
    tmp = STATE_PATH.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp.replace(STATE_PATH)


def acquire_lock() -> Any:
    fh = open(LOCK_PATH, "a+", encoding="utf-8")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fh.close()
        raise SystemExit(f"another watcher instance holds {LOCK_PATH}")
    fh.seek(0)
    fh.truncate()
    fh.write(str(os.getpid()))
    fh.flush()
    # Do not overwrite watcher.pid: start.sh stores the supervisor pid there.
    return fh


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _redact_url(url: str) -> str:
    try:
        p = urllib.parse.urlparse(url)
        return f"{p.scheme}://{p.netloc}{p.path}"
    except Exception:
        return "<url>"


def http_get(url: str, cfg: dict) -> tuple[int, str]:
    timeout = cfg.get("request_timeout_seconds", 20)
    ua = cfg.get("user_agent") or "LebanonNewsWatcher/1.0"
    headers = {"User-Agent": ua, "Accept-Language": "ar,he,en;q=0.8", "Accept": "text/html"}
    retries = int(cfg.get("request_retries", 1))
    last_err = None
    for attempt in range(retries + 1):
        try:
            if requests is not None:
                r = requests.get(url, headers=headers, timeout=timeout)
                return r.status_code, r.text
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.getcode(), resp.read().decode("utf-8", "replace")
        except Exception as exc:  # noqa: BLE001 — network, we retry then log
            last_err = exc
            time.sleep(1 + attempt)
    logging.warning("GET %s failed: %s", _redact_url(url), type(last_err).__name__)
    return 0, ""


def http_post_json(url: str, payload: dict, headers: dict, timeout: int = 20) -> tuple[int, str]:
    # headers may contain secrets; never log them.
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    if requests is not None:
        r = requests.post(url, data=body, headers=headers, timeout=timeout)
        return r.status_code, r.text[:500]
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", "replace")[:500]
    except Exception as exc:  # noqa: BLE001
        code = getattr(getattr(exc, "code", None), "real", None) or getattr(exc, "code", 0) or 0
        return int(code or 0), type(exc).__name__


# ---------------------------------------------------------------------------
# Telegram page parser
# ---------------------------------------------------------------------------

TAG_RE = re.compile(r"<[^>]+>")
BR_RE = re.compile(r"<\s*br\s*/?\s*>", re.I)
DATA_POST_RE = re.compile(
    r'<div class="tgme_widget_message\b[^>]*\bdata-post="([^"]+)"',
    re.I,
)
TIME_RE = re.compile(r'<time[^>]*\bdatetime="([^"]+)"', re.I)
TEXT_RE = re.compile(
    r'<div class="tgme_widget_message_text\b[^"]*"[^>]*>(.*?)</div>',
    re.I | re.S,
)
DATE_HREF_RE = re.compile(
    r'<a class="tgme_widget_message_date"[^>]*href="([^"]+)"',
    re.I,
)


def _strip_html(chunk: str) -> str:
    chunk = BR_RE.sub("\n", chunk)
    chunk = TAG_RE.sub(" ", chunk)
    chunk = htmlmod.unescape(chunk)
    chunk = re.sub(r"[ \t\r\f\v]+", " ", chunk)
    chunk = re.sub(r"\n+", "\n", chunk)
    return chunk.strip()


def parse_posts(html: str, channel: str) -> list[dict[str, Any]]:
    """Return posts oldest-first from a t.me/s page."""
    starts = list(DATA_POST_RE.finditer(html))
    posts: list[dict[str, Any]] = []
    for i, m in enumerate(starts):
        data_post = m.group(1)
        end = starts[i + 1].start() if i + 1 < len(starts) else len(html)
        block = html[m.start() : end]
        if "/" in data_post:
            ch, pid = data_post.split("/", 1)
        else:
            ch, pid = channel, data_post
        time_m = TIME_RE.search(block)
        dt_raw = time_m.group(1) if time_m else ""
        dt = _parse_dt(dt_raw)
        text_m = TEXT_RE.search(block)
        text = _strip_html(text_m.group(1)) if text_m else ""
        href_m = DATE_HREF_RE.search(block)
        url = htmlmod.unescape(href_m.group(1)) if href_m else f"https://t.me/{ch}/{pid}"
        posts.append(
            {
                "channel": ch or channel,
                "id": str(pid),
                "data_post": data_post,
                "time": dt,
                "time_iso": dt.isoformat() if dt else "",
                "text": text,
                "url": url,
            }
        )
    posts.sort(key=lambda p: (p["time"] or datetime.min.replace(tzinfo=timezone.utc), p["id"]))
    return posts


def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Keyword scoring
# ---------------------------------------------------------------------------

def _contains(text: str, needles: list[str]) -> list[str]:
    hits = []
    for n in needles:
        if n and n in text:
            hits.append(n)
    return hits


def _norm(text: str) -> str:
    # Light Arabic/Hebrew normalization so config lists don't need every spelling.
    trans = str.maketrans({
        "أ": "ا", "إ": "ا", "آ": "ا", "ٱ": "ا",
        "ى": "ي", "ة": "ه",
        "ـ": "",
        "\u200f": "", "\u200e": "", "\ufeff": "",
    })
    return text.translate(trans)


URL_RE = re.compile(r"https?://\S+", re.I)


def _match_text(text: str, cfg: dict) -> str:
    """Strip watermarks and URLs so channel names like 'لبنان 24' are not geo hits."""
    cleaned = text or ""
    for w in cfg.get("ignore_substrings") or []:
        if w:
            cleaned = cleaned.replace(w, " ")
            cleaned = cleaned.replace(_norm(w), " ")
    cleaned = URL_RE.sub(" ", cleaned)
    return cleaned


def classify(post: dict[str, Any], cfg: dict) -> dict[str, Any]:
    raw = post.get("text") or ""
    text = _match_text(raw, cfg)
    norm = _norm(text)
    # Match against both raw (cleaned) and normalized text.
    def hits(needles: list[str]) -> list[str]:
        found = []
        for n in needles:
            if not n:
                continue
            if n in text or _norm(n) in norm:
                found.append(n)
        return found

    lebanon = hits(cfg.get("lebanon_markers") or [])
    places = hits(cfg.get("places") or [])
    offtopic = hits(cfg.get("offtopic_markers") or [])
    relevant = bool(lebanon or places)
    # A post that only talks about Gaza/Yemen/etc. is dropped even if a
    # generic word like "العدو" appears.
    if offtopic and not relevant:
        relevant = False

    urgent = hits(cfg.get("urgent_markers") or [])
    major = hits(cfg.get("major_keywords") or [])
    standalone = hits(cfg.get("standalone_major_keywords") or [])
    exclude = hits(cfg.get("exclude_only_keywords") or [])
    overrides = hits(cfg.get("exclude_overrides") or [])
    low_level_only = bool(exclude) and not bool(overrides)

    keyword_hit = bool(relevant and not low_level_only and urgent and major)
    standalone_hit = bool(relevant and not low_level_only and standalone)
    # Dahiyeh / Beirut strike is standalone-major even without the exact phrase
    # in standalone_major_keywords, if a strike word is also present.
    dahiyeh_words = ["الضاحية", "ضاحية بيروت", "דאחייה", "הדאחייה", "Dahiyeh", "Dahieh"]
    beirut_strike_words = ["بيروت", "ביירות", "Beirut"]
    strike_words = hits(["غارة", "غارات", "اغار", "أغار", "תקיפה", "תקיפות", "airstrike", "airstrikes"])
    if relevant and not low_level_only and strike_words and hits(dahiyeh_words + beirut_strike_words):
        standalone_hit = True
        standalone = standalone or hits(dahiyeh_words + beirut_strike_words)

    cluster_keys: list[str] = []
    if relevant and not low_level_only:
        place_ctx = hits(cfg.get("cluster_place_context") or cfg.get("cluster_theater_keywords") or [])
        if place_ctx:
            for p in places:
                cluster_keys.append(f"place:{_norm(p)}")
        for ev in hits(cfg.get("cluster_event_keywords") or []):
            cluster_keys.append(f"event:{_norm(ev)}")
        theater_kws = hits(cfg.get("cluster_theater_keywords") or [])
        if theater_kws:
            cluster_keys.append(cfg.get("cluster_theater_key") or "strike_south_lebanon")

    reason_parts = []
    if keyword_hit:
        reason_parts.append("urgent+major")
    if standalone_hit and not keyword_hit:
        reason_parts.append("standalone-major")
    if low_level_only:
        reason_parts.append("excluded-low-level")
    if offtopic and not relevant:
        reason_parts.append("offtopic")

    return {
        "relevant": relevant,
        "low_level_only": low_level_only,
        "keyword_candidate": keyword_hit or standalone_hit,
        "urgent": urgent,
        "major": major,
        "standalone": standalone,
        "places": places,
        "cluster_keys": sorted(set(cluster_keys)),
        "reason": ",".join(reason_parts),
    }


def apply_cluster(posts_with_class: list[tuple[dict, dict]], cfg: dict) -> None:
    """Mark posts as cluster candidates if 3+ channels share a key in the window.

    Mutates the classification dicts in place (sets cluster_candidate / cluster_key).
    """
    window = int((cfg.get("cluster") or {}).get("window_seconds", 600))
    min_ch = int((cfg.get("cluster") or {}).get("min_channels", 3))
    # Build key -> list of (ts, channel, idx)
    index: dict[str, list[tuple[datetime, str, int]]] = {}
    for i, (post, cls) in enumerate(posts_with_class):
        if not post.get("time") or not cls.get("cluster_keys"):
            continue
        if cls.get("low_level_only"):
            continue
        if not cls.get("relevant"):
            continue
        for key in cls["cluster_keys"]:
            index.setdefault(key, []).append((post["time"], post["channel"], i))

    for key, entries in index.items():
        entries.sort()
        n = len(entries)
        for left in range(n):
            channels = {entries[left][1]}
            members = [entries[left][2]]
            t0 = entries[left][0]
            for right in range(left + 1, n):
                if (entries[right][0] - t0).total_seconds() > window:
                    break
                channels.add(entries[right][1])
                members.append(entries[right][2])
            if len(channels) >= min_ch:
                for idx in members:
                    cls = posts_with_class[idx][1]
                    cls["cluster_candidate"] = True
                    cls.setdefault("cluster_matched_keys", [])
                    if key not in cls["cluster_matched_keys"]:
                        cls["cluster_matched_keys"].append(key)


def is_candidate(cls: dict[str, Any]) -> bool:
    if cls.get("low_level_only"):
        return False
    return bool(cls.get("keyword_candidate") or cls.get("cluster_candidate"))


# ---------------------------------------------------------------------------
# Shabbat (hebcal: Tel Aviv for the news blackout, Jerusalem for the entry timer)
# ---------------------------------------------------------------------------

TEL_AVIV_HEBCAL_DEFAULT = "https://www.hebcal.com/shabbat?cfg=json&geonameid=293397&M=on"
JERUSALEM_HEBCAL_DEFAULT = "https://www.hebcal.com/shabbat?cfg=json&geonameid=281184&M=on&b=40"


def _hebcal_url(cfg: dict, city: str) -> str:
    heb = cfg.get("hebcal") or {}
    if city == "jerusalem":
        return heb.get("jerusalem_url") or JERUSALEM_HEBCAL_DEFAULT
    return heb.get("url") or TEL_AVIV_HEBCAL_DEFAULT


def _parse_hebcal_items(data: dict, cfg: dict) -> tuple[datetime | None, datetime | None, str | None]:
    candles = havdalah = None
    for item in data.get("items") or []:
        cat = item.get("category")
        raw = item.get("date")
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tzinfo(cfg))
        else:
            dt = dt.astimezone(tzinfo(cfg))
        if cat == "candles":
            candles = dt
        elif cat == "havdalah":
            havdalah = dt
    location = (data.get("location") or {}).get("title")
    return candles, havdalah, location


def _fetch_hebcal_window(url: str, cfg: dict) -> tuple[datetime, datetime, str | None] | None:
    status, body = http_get(url, cfg)
    if status != 200 or not body:
        logging.warning("hebcal fetch failed status=%s url=%s", status, _redact_url(url))
        return None
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        logging.warning("hebcal JSON parse failed url=%s", _redact_url(url))
        return None
    candles, havdalah, location = _parse_hebcal_items(data, cfg)
    if not candles or not havdalah:
        logging.warning("hebcal missing candles/havdalah url=%s", _redact_url(url))
        return None
    return candles, havdalah, location


def _windows_from_cache(cache: dict) -> dict[str, tuple[datetime, datetime]]:
    windows: dict[str, tuple[datetime, datetime]] = {}
    try:
        windows["tel_aviv"] = (
            datetime.fromisoformat(cache["candles"]),
            datetime.fromisoformat(cache["havdalah"]),
        )
    except (KeyError, TypeError, ValueError):
        pass
    try:
        windows["jerusalem"] = (
            datetime.fromisoformat(cache["jerusalem_candles"]),
            datetime.fromisoformat(cache["jerusalem_havdalah"]),
        )
    except (KeyError, TypeError, ValueError):
        pass
    return windows


def _cache_covers_when(cache: dict, when: datetime) -> bool:
    """True if the cache is for this week's Shabbat and was fetched today, or we are inside it."""
    try:
        candles = datetime.fromisoformat(cache["candles"])
        havdalah = datetime.fromisoformat(cache["havdalah"])
    except (KeyError, TypeError, ValueError):
        try:
            candles = datetime.fromisoformat(cache["jerusalem_candles"])
            havdalah = datetime.fromisoformat(cache["jerusalem_havdalah"])
        except (KeyError, TypeError, ValueError):
            return False
    fetched = cache.get("fetched_date")
    if candles <= when < havdalah:
        return True
    if fetched == when.date().isoformat() and when < havdalah + timedelta(hours=12):
        return True
    if fetched == when.date().isoformat():
        return True
    return False


def load_shabbat_times(cfg: dict, when: datetime | None = None) -> dict[str, tuple[datetime, datetime]]:
    """Return city -> (candles, havdalah) for Tel Aviv and/or Jerusalem.

    Tel Aviv times stay the source of truth for the breaking-news Shabbat blackout.
    Jerusalem times (geonameid 281184, 40 minutes before sunset) drive the
    Friday shabbat_entry timer. Both are stored in shabbat-cache.json.
    """
    when = when or now_local(cfg)
    cache = _read_json(SHABBAT_CACHE_PATH) or {}
    windows = _windows_from_cache(cache) if cache else {}
    reuse = bool(cache) and _cache_covers_when(cache, when)
    dirty = False

    if not reuse or "tel_aviv" not in windows:
        fetched = _fetch_hebcal_window(_hebcal_url(cfg, "tel_aviv"), cfg)
        if fetched:
            candles, havdalah, location = fetched
            windows["tel_aviv"] = (candles, havdalah)
            cache["candles"] = candles.isoformat()
            cache["havdalah"] = havdalah.isoformat()
            cache["location"] = location
            cache["fetched_date"] = when.date().isoformat()
            dirty = True
            logging.info("shabbat times cached (Tel Aviv): candles %s, havdalah %s", candles.isoformat(), havdalah.isoformat())
        elif "tel_aviv" not in windows and cache:
            try:
                windows["tel_aviv"] = (
                    datetime.fromisoformat(cache["candles"]),
                    datetime.fromisoformat(cache["havdalah"]),
                )
            except (KeyError, TypeError, ValueError):
                pass

    if not reuse or "jerusalem" not in windows:
        fetched = _fetch_hebcal_window(_hebcal_url(cfg, "jerusalem"), cfg)
        if fetched:
            candles, havdalah, location = fetched
            windows["jerusalem"] = (candles, havdalah)
            cache["jerusalem_candles"] = candles.isoformat()
            cache["jerusalem_havdalah"] = havdalah.isoformat()
            cache["jerusalem_location"] = location
            cache.setdefault("fetched_date", when.date().isoformat())
            dirty = True
            logging.info(
                "shabbat times cached (Jerusalem): candles %s, havdalah %s",
                candles.isoformat(),
                havdalah.isoformat(),
            )
        elif "jerusalem" not in windows and cache:
            try:
                windows["jerusalem"] = (
                    datetime.fromisoformat(cache["jerusalem_candles"]),
                    datetime.fromisoformat(cache["jerusalem_havdalah"]),
                )
            except (KeyError, TypeError, ValueError):
                pass

    if dirty:
        _write_json(SHABBAT_CACHE_PATH, cache)
    return windows


def load_shabbat_window(cfg: dict, when: datetime | None = None) -> tuple[datetime, datetime] | None:
    """Return Tel Aviv (candles, havdalah) in Asia/Jerusalem for the Shabbat near `when`."""
    times = load_shabbat_times(cfg, when)
    return times.get("tel_aviv")


def load_jerusalem_window(cfg: dict, when: datetime | None = None) -> tuple[datetime, datetime] | None:
    """Return Jerusalem (candles, havdalah) in Asia/Jerusalem for the Shabbat near `when`."""
    times = load_shabbat_times(cfg, when)
    return times.get("jerusalem")


def is_shabbat(cfg: dict, when: datetime | None = None) -> bool:
    when = when or now_local(cfg)
    window = load_shabbat_window(cfg, when)
    if not window:
        # Fail safe on Friday sunset–Saturday night if hebcal is down:
        # block from Fri 16:00 to Sat 18:00 local as a conservative fallback.
        wd = when.weekday()  # Friday=4, Saturday=5
        if wd == 4 and (when.hour > 16 or (when.hour == 16 and when.minute >= 0)):
            logging.warning("hebcal unavailable; conservative Shabbat block (Fri after 16:00)")
            return True
        if wd == 5 and when.hour < 18:
            logging.warning("hebcal unavailable; conservative Shabbat block (Sat before 18:00)")
            return True
        return False
    candles, havdalah = window
    return candles <= when < havdalah


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _write_json(path: Path, data: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Shabbat-entry timer (Friday, Jerusalem candles minus lead minutes)
# ---------------------------------------------------------------------------

SHABBAT_ENTRY_LEAD_DEFAULT = 50
SHABBAT_ENTRY_CUTOFF_DEFAULT = 30
SHABBAT_ENTRY_RETRIES_DEFAULT = 3
SHABBAT_ENTRY_CHECK_DEFAULT = 60


def shabbat_entry_cfg(cfg: dict) -> dict[str, int]:
    sec = cfg.get("shabbat_entry") or {}
    cutoff_raw = sec.get("cutoff_minutes_before_candles")
    if cutoff_raw is None:
        cutoff_raw = sec.get("cutoff_minutes")
    return {
        "lead_minutes": int(sec.get("lead_minutes") if sec.get("lead_minutes") is not None else SHABBAT_ENTRY_LEAD_DEFAULT),
        "cutoff_minutes": int(cutoff_raw if cutoff_raw is not None else SHABBAT_ENTRY_CUTOFF_DEFAULT),
        "check_interval_seconds": int(
            sec.get("check_interval_seconds") if sec.get("check_interval_seconds") is not None else SHABBAT_ENTRY_CHECK_DEFAULT
        ),
        "post_retries": int(sec.get("post_retries") if sec.get("post_retries") is not None else SHABBAT_ENTRY_RETRIES_DEFAULT),
    }


def shabbat_entry_fire_time(jerusalem_candles: datetime, lead_minutes: int) -> datetime:
    return jerusalem_candles - timedelta(minutes=lead_minutes)


def shabbat_entry_cutoff_time(jerusalem_candles: datetime, cutoff_minutes: int) -> datetime:
    return jerusalem_candles - timedelta(minutes=cutoff_minutes)


def shabbat_entry_payload(jerusalem_candles: datetime, tel_aviv_candles: datetime | None = None) -> dict[str, str]:
    payload = {
        "event": "shabbat_entry",
        "date": jerusalem_candles.date().isoformat(),
        "candle_lighting_jerusalem": jerusalem_candles.strftime("%H:%M"),
    }
    if tel_aviv_candles is not None:
        payload["candle_lighting_tel_aviv"] = tel_aviv_candles.strftime("%H:%M")
    return payload


def already_fired_shabbat_entry(state: dict, date_str: str) -> bool:
    meta = state.get("shabbat_entry") or {}
    if meta.get("last_fired_date") == date_str:
        return True
    return date_str in (meta.get("fired_dates") or [])


def shabbat_entry_decision(
    when: datetime,
    jerusalem_candles: datetime,
    lead_minutes: int = SHABBAT_ENTRY_LEAD_DEFAULT,
    cutoff_minutes: int = SHABBAT_ENTRY_CUTOFF_DEFAULT,
    already_fired: bool = False,
) -> str:
    """Return 'fire', 'wait', or 'skip'.

    fire: fire_time <= when < cutoff, and not already fired.
    wait: when < fire_time, and not already fired.
    skip: already fired, or when >= Jerusalem candles minus cutoff_minutes
          (never fire at or after that cutoff).
    """
    if already_fired:
        return "skip"
    fire_at = shabbat_entry_fire_time(jerusalem_candles, lead_minutes)
    cutoff = shabbat_entry_cutoff_time(jerusalem_candles, cutoff_minutes)
    if when >= cutoff:
        return "skip"
    if when < fire_at:
        return "wait"
    return "fire"


def describe_shabbat_entry(cfg: dict, when: datetime | None = None, state: dict | None = None) -> str:
    """Log line describing the next or current shabbat_entry fire time."""
    when = when or now_local(cfg)
    times = load_shabbat_times(cfg, when)
    jer = times.get("jerusalem")
    sec = shabbat_entry_cfg(cfg)
    lead = sec["lead_minutes"]
    cutoff_minutes = sec["cutoff_minutes"]
    tzname = cfg.get("timezone") or "Asia/Jerusalem"
    if not jer:
        return "shabbat_entry: no Jerusalem candle-lighting time available; cannot schedule"
    jerusalem_candles, _havdalah = jer
    ta_candles = times["tel_aviv"][0] if times.get("tel_aviv") else None
    fire_at = shabbat_entry_fire_time(jerusalem_candles, lead)
    cutoff = shabbat_entry_cutoff_time(jerusalem_candles, cutoff_minutes)
    date_str = jerusalem_candles.date().isoformat()
    fire_hhmm = fire_at.strftime("%H:%M")
    cutoff_hhmm = cutoff.strftime("%H:%M")
    jer_hhmm = jerusalem_candles.strftime("%H:%M")
    ta_note = f", Tel Aviv {ta_candles.strftime('%H:%M')}" if ta_candles is not None else ""
    fired = already_fired_shabbat_entry(state, date_str) if state is not None else False
    if fired:
        return (
            f"shabbat_entry already sent for {date_str} "
            f"(was due {fire_hhmm} {tzname}, Jerusalem candles {jer_hhmm}{ta_note})"
        )
    decision = shabbat_entry_decision(
        when, jerusalem_candles, lead, cutoff_minutes=cutoff_minutes, already_fired=False
    )
    if decision == "wait":
        return (
            f"shabbat_entry next fire: {date_str} {fire_hhmm} {tzname} "
            f"(Jerusalem candles {jer_hhmm}{ta_note})"
        )
    if decision == "fire":
        return (
            f"shabbat_entry fire window open: {fire_hhmm}–{cutoff_hhmm} {tzname} "
            f"on {date_str}; will fire immediately (Jerusalem candles {jer_hhmm}{ta_note})"
        )
    return (
        f"shabbat_entry will not fire for {date_str}: at or after cutoff "
        f"{cutoff_hhmm} {tzname} (Jerusalem candles {jer_hhmm}, due was {fire_hhmm})"
    )


def seconds_until_shabbat_entry(cfg: dict, when: datetime | None = None, state: dict | None = None) -> float | None:
    """Seconds until fire time, 0 if in the fire window, None if not applicable."""
    when = when or now_local(cfg)
    times = load_shabbat_times(cfg, when)
    jer = times.get("jerusalem")
    if not jer:
        return None
    jerusalem_candles, _havdalah = jer
    sec = shabbat_entry_cfg(cfg)
    lead = sec["lead_minutes"]
    cutoff_minutes = sec["cutoff_minutes"]
    date_str = jerusalem_candles.date().isoformat()
    if state is not None and already_fired_shabbat_entry(state, date_str):
        return None
    decision = shabbat_entry_decision(
        when, jerusalem_candles, lead, cutoff_minutes=cutoff_minutes, already_fired=False
    )
    if decision == "skip":
        return None
    if decision == "fire":
        return 0.0
    fire_at = shabbat_entry_fire_time(jerusalem_candles, lead)
    return max(0.0, (fire_at - when).total_seconds())


def _post_shabbat_entry(cfg: dict, payload: dict, dry_run: bool) -> bool:
    """POST the shabbat_entry event. Does not touch breaking-news rate-limit counters.

    Retries on non-2xx (or transport failure) up to post_retries times after the
    first attempt. Returns True on a 2xx (or dry-run). Never logs secrets.
    """
    retries = shabbat_entry_cfg(cfg)["post_retries"]
    url, key = webhook_configured()
    if dry_run or not url or not key:
        logging.info(
            "DRY-RUN shabbat_entry would POST %s to %s",
            json.dumps(payload, ensure_ascii=False),
            _redact_url(url) if url else "(LEB_WEBHOOK_URL unset)",
        )
        return True
    headers = build_webhook_headers(cfg, key)
    timeout = cfg.get("request_timeout_seconds", 20)
    attempts = retries + 1
    for attempt in range(1, attempts + 1):
        try:
            status, _body = http_post_json(url, payload, headers, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            status = 0
            logging.error(
                "shabbat_entry POST failed attempt %s/%s: %s",
                attempt,
                attempts,
                type(exc).__name__,
            )
        else:
            if status and 200 <= status < 300:
                logging.info(
                    "shabbat_entry webhook accepted status=%s date=%s jerusalem=%s tel_aviv=%s",
                    status,
                    payload.get("date"),
                    payload.get("candle_lighting_jerusalem"),
                    payload.get("candle_lighting_tel_aviv"),
                )
                return True
            logging.error(
                "shabbat_entry webhook rejected status=%s attempt %s/%s",
                status,
                attempt,
                attempts,
            )
        if attempt < attempts:
            time.sleep(min(8, attempt))
    return False


def mark_shabbat_entry_fired(state: dict, date_str: str, when: datetime, ok: bool) -> None:
    meta = state.setdefault("shabbat_entry", {})
    if ok:
        meta["last_fired_date"] = date_str
        meta["last_fired_at"] = when.isoformat()
        dates = list(meta.get("fired_dates") or [])
        if date_str not in dates:
            dates.append(date_str)
            meta["fired_dates"] = dates[-12:]
        meta["last_attempt_ok"] = True
    else:
        meta["last_attempt_date"] = date_str
        meta["last_attempt_at"] = when.isoformat()
        meta["last_attempt_ok"] = False


def maybe_send_shabbat_entry(cfg: dict, state: dict, dry_run: bool, when: datetime | None = None) -> str:
    """Check and possibly POST the Friday shabbat_entry event. Returns decision or 'retry'."""
    when = when or now_local(cfg)
    times = load_shabbat_times(cfg, when)
    jer = times.get("jerusalem")
    sec = shabbat_entry_cfg(cfg)
    lead = sec["lead_minutes"]
    cutoff_minutes = sec["cutoff_minutes"]
    if not jer:
        logging.warning("shabbat_entry: no Jerusalem candles time; skipping check")
        return "skip"
    jerusalem_candles, _havdalah = jer
    ta_candles = times["tel_aviv"][0] if times.get("tel_aviv") else None
    date_str = jerusalem_candles.date().isoformat()
    fired = already_fired_shabbat_entry(state, date_str)
    decision = shabbat_entry_decision(
        when, jerusalem_candles, lead, cutoff_minutes=cutoff_minutes, already_fired=fired
    )
    if decision != "fire":
        return decision
    payload = shabbat_entry_payload(jerusalem_candles, ta_candles)
    logging.info(
        "shabbat_entry firing for %s at %s (Jerusalem candles %s); payload=%s",
        date_str,
        when.strftime("%H:%M"),
        jerusalem_candles.strftime("%H:%M"),
        json.dumps(payload, ensure_ascii=False),
    )
    ok = _post_shabbat_entry(cfg, payload, dry_run=dry_run)
    mark_shabbat_entry_fired(state, date_str, when, ok=ok)
    save_state(state)
    if ok:
        logging.info("shabbat_entry recorded in state.json for %s", date_str)
        return "fire"
    logging.error(
        "shabbat_entry POST exhausted retries; will retry before the %s-minute cutoff if still time",
        cutoff_minutes,
    )
    return "retry"


def _loop_sleep_seconds(cfg: dict, check_every: int, interval: int, last_cycle_ts: float) -> float:
    """Sleep until the next poll, the next shabbat_entry check, or fire time — never more than check_every."""
    local = now_local(cfg)
    state = load_state()
    until_entry = seconds_until_shabbat_entry(cfg, local, state=state)
    sleep_for = float(check_every)
    if until_entry is not None:
        if until_entry == 0:
            # In the fire window after a failed POST: retry soon.
            sleep_for = min(sleep_for, 15.0)
        else:
            sleep_for = min(sleep_for, until_entry)
    if in_active_window(cfg, local) and last_cycle_ts:
        until_cycle = interval - (time.time() - last_cycle_ts)
        if until_cycle > 0:
            sleep_for = min(sleep_for, until_cycle)
    elif not in_active_window(cfg, local):
        wait = seconds_until_window(cfg, local)
        sleep_for = min(sleep_for, float(wait))
    sleep_for = min(sleep_for, float(check_every))
    if until_entry is not None and 0 < until_entry < 1:
        return max(0.05, sleep_for)
    return max(1.0, sleep_for)


# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------

def webhook_configured() -> tuple[str, str]:
    url = (os.environ.get("LEB_WEBHOOK_URL") or "").strip()
    key = (os.environ.get("LEB_WEBHOOK_KEY") or "").strip()
    return url, key


def build_webhook_headers(cfg: dict, key: str) -> dict[str, str]:
    wh = cfg.get("webhook") or {}
    name = wh.get("auth_header_name") or "Authorization"
    template = wh.get("auth_header_template") or "Bearer {key}"
    # Only {key} is interpolated; never log the result.
    header_value = template.replace("{key}", key)
    headers = {
        "Content-Type": "application/json",
        name: header_value,
    }
    if wh.get("also_send_x_webhook_key", True):
        headers["X-Webhook-Key"] = key
    return headers


def trim_text(text: str, limit: int) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def candidate_payload(post: dict, cls: dict, cfg: dict) -> dict[str, str]:
    limit = int((cfg.get("webhook") or {}).get("text_max_chars", 600))
    return {
        "channel": post["channel"],
        "time": post.get("time_iso") or "",
        "text": trim_text(post.get("text") or "", limit),
        "url": post.get("url") or "",
        "reason": cls.get("reason") or ("cluster" if cls.get("cluster_candidate") else ""),
    }


def maybe_send_webhook(cfg: dict, state: dict, dry_run: bool) -> None:
    pending = state.get("pending") or []
    if not pending:
        return
    wh = cfg.get("webhook") or {}
    min_interval = int(wh.get("min_interval_seconds", 600))
    max_calls = int(wh.get("max_calls_per_day", 20))
    local = now_local(cfg)
    today = local.date().isoformat()
    meta = state.setdefault("webhook", {})
    if meta.get("date") != today:
        meta["date"] = today
        meta["calls"] = 0
        # keep last_call_ts so the 10-minute gap can still apply just after midnight if needed
    calls = int(meta.get("calls") or 0)
    if calls >= max_calls:
        logging.warning("webhook daily cap reached (%s); holding %s pending", max_calls, len(pending))
        return
    last = meta.get("last_call_ts")
    if last:
        try:
            last_dt = datetime.fromisoformat(last)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=tzinfo(cfg))
            elapsed = (datetime.now(last_dt.tzinfo) - last_dt).total_seconds()
            if elapsed < min_interval:
                logging.info("webhook cooldown %.0fs remaining; holding %s pending", min_interval - elapsed, len(pending))
                return
        except ValueError:
            pass

    if is_shabbat(cfg, local):
        logging.info("Shabbat: holding %s pending candidates, not calling webhook", len(pending))
        return

    url, key = webhook_configured()
    payload = {"candidates": pending}
    if dry_run or not url or not key:
        logging.info(
            "DRY-RUN webhook would POST %s candidate(s) to %s",
            len(pending),
            _redact_url(url) if url else "(LEB_WEBHOOK_URL unset)",
        )
        for c in pending:
            logging.info(
                "  DRY-RUN candidate %s %s %s | %s",
                c.get("channel"),
                c.get("time"),
                c.get("reason"),
                (c.get("text") or "")[:180].replace("\n", " "),
            )
        # In dry-run we still clear pending and count a "call" so the cap/cooldown
        # behave like production and we don't re-log the same batch every cycle.
        meta["calls"] = calls + 1
        meta["last_call_ts"] = local.isoformat()
        meta["last_mode"] = "dry-run"
        state["pending"] = []
        return

    headers = build_webhook_headers(cfg, key)
    try:
        status, _body = http_post_json(url, payload, headers, timeout=cfg.get("request_timeout_seconds", 20))
    except Exception as exc:  # noqa: BLE001
        logging.error("webhook POST failed: %s", type(exc).__name__)
        return
    if status and 200 <= status < 300:
        logging.info("webhook accepted status=%s candidates=%s (url=%s)", status, len(pending), _redact_url(url))
        meta["calls"] = calls + 1
        meta["last_call_ts"] = local.isoformat()
        meta["last_mode"] = "live"
        state["pending"] = []
    else:
        logging.error("webhook rejected status=%s; keeping %s pending", status, len(pending))


# ---------------------------------------------------------------------------
# Cycle
# ---------------------------------------------------------------------------

def fetch_channel(channel: str, cfg: dict) -> tuple[int, list[dict[str, Any]]]:
    url = f"https://t.me/s/{channel}"
    status, body = http_get(url, cfg)
    if status != 200 or not body:
        logging.warning("channel %s fetch status=%s", channel, status)
        return status, []
    posts = parse_posts(body, channel)
    return status, posts


def run_cycle(cfg: dict, state: dict, dry_run: bool) -> dict[str, Any]:
    counts: dict[str, int] = {}
    errors: dict[str, int] = {}
    new_posts: list[tuple[dict, dict]] = []
    first_seen_channels: list[str] = []
    gap = float(cfg.get("request_gap_seconds") or 0)

    for i, channel in enumerate(cfg["channels"]):
        if i and gap:
            time.sleep(gap)
        status, posts = fetch_channel(channel, cfg)
        if status != 200:
            errors[channel] = status
            counts[channel] = 0
            continue
        counts[channel] = len(posts)
        seen = state["seen"].setdefault(channel, [])
        seen_set = set(seen)
        is_first = len(seen) == 0
        if is_first:
            first_seen_channels.append(channel)
        for post in posts:
            pid = post["id"]
            if pid in seen_set:
                continue
            seen.append(pid)
            seen_set.add(pid)
            if is_first:
                continue
            cls = classify(post, cfg)
            new_posts.append((post, cls))

        cap = int(cfg.get("seen_ids_per_channel") or 400)
        if len(seen) > cap:
            state["seen"][channel] = seen[-cap:]

    # Cluster across new posts plus the recent buffer (so a wave spanning polls still matches).
    buffer_items: list[tuple[dict, dict]] = []
    for item in state.get("recent") or []:
        # Reconstruct a minimal post/cls for clustering.
        ts = _parse_dt(item.get("time") or "")
        post = {
            "channel": item["channel"],
            "id": item["id"],
            "time": ts,
            "time_iso": item.get("time") or "",
            "text": item.get("text") or "",
            "url": item.get("url") or "",
        }
        cls = {
            "relevant": True,
            "low_level_only": False,
            "cluster_keys": item.get("keys") or [],
            "keyword_candidate": False,
        }
        buffer_items.append((post, cls))

    combined = buffer_items + new_posts
    apply_cluster(combined, cfg)

    # After clustering, only the newly seen posts (not the buffer) can become pending.
    new_start = len(buffer_items)
    added = 0
    for post, cls in combined[new_start:]:
        # Keep recent buffer entries for future cycles.
        if post.get("time") and cls.get("cluster_keys"):
            state.setdefault("recent", []).append(
                {
                    "channel": post["channel"],
                    "id": post["id"],
                    "time": post.get("time_iso") or post["time"].isoformat(),
                    "keys": cls.get("cluster_keys") or [],
                    "text": trim_text(post.get("text") or "", 200),
                    "url": post.get("url") or "",
                }
            )
        if is_candidate(cls):
            if cls.get("cluster_candidate") and "cluster" not in (cls.get("reason") or ""):
                cls["reason"] = (cls.get("reason") + ",cluster" if cls.get("reason") else "cluster")
            payload = candidate_payload(post, cls, cfg)
            # Dedupe pending by channel+id.
            existing = {(p.get("channel"), p.get("url")) for p in state["pending"]}
            key = (payload["channel"], payload["url"])
            if key not in existing:
                state["pending"].append(payload)
                added += 1
                logging.info(
                    "candidate %s %s [%s] %s",
                    post["channel"],
                    post.get("time_iso"),
                    cls.get("reason"),
                    (post.get("text") or "")[:160].replace("\n", " "),
                )

    # Trim recent buffer.
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=int(cfg.get("recent_buffer_minutes") or 30))
    kept = []
    for item in state.get("recent") or []:
        ts = _parse_dt(item.get("time") or "")
        if ts and ts >= cutoff:
            kept.append(item)
    state["recent"] = kept

    if first_seen_channels:
        logging.info("first run: marked existing posts seen (no alerts) for: %s", ", ".join(first_seen_channels))
    logging.info(
        "cycle done: fetched %s channels, new_posts=%s, new_candidates=%s, pending=%s, errors=%s",
        sum(1 for c, n in counts.items() if n),
        len(new_posts),
        added,
        len(state.get("pending") or []),
        errors or "none",
    )

    maybe_send_webhook(cfg, state, dry_run=dry_run)
    save_state(state)
    return {"counts": counts, "errors": errors, "new_posts": len(new_posts), "new_candidates": added, "first": first_seen_channels}


# ---------------------------------------------------------------------------
# Calibration (last ~24h on the current pages, no state mutation of seen)
# ---------------------------------------------------------------------------

def run_calibrate(cfg: dict) -> dict[str, Any]:
    local_tz = tzinfo(cfg)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    counts: dict[str, int] = {}
    in_window: dict[str, int] = {}
    errors: dict[str, int] = {}
    collected: list[tuple[dict, dict]] = []
    gap = float(cfg.get("request_gap_seconds") or 0)

    for i, channel in enumerate(cfg["channels"]):
        if i and gap:
            time.sleep(gap)
        status, posts = fetch_channel(channel, cfg)
        if status != 200:
            errors[channel] = status
            counts[channel] = 0
            in_window[channel] = 0
            continue
        counts[channel] = len(posts)
        recent = []
        for post in posts:
            if post["time"] is None:
                # Keep dateless posts only if they look fresh enough; otherwise skip.
                continue
            if post["time"] >= cutoff:
                recent.append(post)
        in_window[channel] = len(recent)
        for post in recent:
            cls = classify(post, cfg)
            collected.append((post, cls))
        logging.info("calibrate %s: page=%s last24h=%s status=%s", channel, len(posts), len(recent), status)

    apply_cluster(collected, cfg)
    triggers: list[dict[str, Any]] = []
    excluded = 0
    offtopic = 0
    for post, cls in collected:
        if cls.get("low_level_only"):
            excluded += 1
        if "offtopic" in (cls.get("reason") or ""):
            offtopic += 1
        if is_candidate(cls):
            if cls.get("cluster_candidate") and "cluster" not in (cls.get("reason") or ""):
                cls["reason"] = (cls.get("reason") + ",cluster" if cls.get("reason") else "cluster")
            local_time = post["time"].astimezone(local_tz).strftime("%Y-%m-%d %H:%M") if post["time"] else "?"
            triggers.append(
                {
                    "channel": post["channel"],
                    "time_local": local_time,
                    "reason": cls.get("reason"),
                    "places": cls.get("places") or [],
                    "cluster_keys": cls.get("cluster_matched_keys") or [],
                    "text": trim_text(post.get("text") or "", 240),
                    "url": post.get("url") or "",
                }
            )

    # Sort and lightly group for a readable report.
    triggers.sort(key=lambda t: t["time_local"])
    report = {
        "cutoff_utc": cutoff.isoformat(),
        "counts_on_page": counts,
        "counts_last_24h": in_window,
        "errors": errors,
        "trigger_count": len(triggers),
        "excluded_low_level": excluded,
        "offtopic_dropped_reason_count": offtopic,
        "triggers": triggers,
    }
    out = DIR / "calibration.json"
    _write_json(out, report)
    return report


def print_calibration(report: dict[str, Any]) -> None:
    print("=== Calibration: posts on public pages, last ~24 hours ===")
    print(f"cutoff UTC: {report['cutoff_utc']}")
    print()
    print(f"{'channel':<22} {'on page':>8} {'last 24h':>8}")
    for ch, n in report["counts_on_page"].items():
        w = report["counts_last_24h"].get(ch, 0)
        err = report["errors"].get(ch)
        extra = f"  ERROR {err}" if err else ""
        print(f"{ch:<22} {n:>8} {w:>8}{extra}")
    print()
    print(f"triggers: {report['trigger_count']}   (low-level excluded posts: {report['excluded_low_level']})")
    if not report["triggers"]:
        print("No posts would have triggered.")
        return
    print()
    for i, t in enumerate(report["triggers"], 1):
        places = ",".join(t["places"][:6])
        print(f"{i:2}. [{t['time_local']} Asia/Jerusalem] {t['channel']}  ({t['reason']}) {places}")
        print(f"    {t['text'].replace(chr(10), ' ')}")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_loop(cfg: dict, dry_run: bool, max_runtime: int = 0) -> None:
    logging.info(
        "watcher loop starting (dry_run=%s, pid=%s, max_runtime=%s)",
        dry_run,
        os.getpid(),
        max_runtime or "none",
    )
    started = time.monotonic()
    # Warm the Shabbat cache once at startup.
    try:
        window = load_shabbat_window(cfg)
        if window:
            logging.info("Shabbat window: %s → %s", window[0].isoformat(), window[1].isoformat())
    except Exception as exc:  # noqa: BLE001
        logging.warning("shabbat warmup failed: %s", type(exc).__name__)

    try:
        logging.info("%s", describe_shabbat_entry(cfg, state=load_state()))
    except Exception as exc:  # noqa: BLE001
        logging.warning("shabbat_entry schedule log failed: %s", type(exc).__name__)

    interval = int(cfg.get("poll_interval_seconds") or 180)
    check_every = int(shabbat_entry_cfg(cfg)["check_interval_seconds"])
    last_cycle_ts = 0.0
    last_offwindow_log = 0.0

    while True:
        try:
            local = now_local(cfg)
            state = load_state()
            maybe_send_shabbat_entry(cfg, state, dry_run=dry_run, when=local)

            # Re-read: a fire may have just updated state.
            state = load_state()
            until_entry = seconds_until_shabbat_entry(cfg, now_local(cfg), state=state)
            imminent = until_entry is not None and until_entry <= 90

            local = now_local(cfg)
            if not imminent and in_active_window(cfg, local):
                now_ts = time.time()
                if last_cycle_ts == 0.0 or (now_ts - last_cycle_ts) >= interval:
                    run_cycle(cfg, load_state(), dry_run=dry_run)
                    last_cycle_ts = time.time()
            elif not in_active_window(cfg, local):
                now_ts = time.time()
                if now_ts - last_offwindow_log >= 600:
                    wait = seconds_until_window(cfg, local)
                    logging.info(
                        "outside 07:00–23:45 window; next poll in %ss (shabbat_entry check continues every %ss)",
                        wait,
                        check_every,
                    )
                    last_offwindow_log = now_ts

            if max_runtime and (time.monotonic() - started) >= max_runtime:
                logging.info("max runtime %ss reached; exiting loop", max_runtime)
                break
            sleep_for = _loop_sleep_seconds(cfg, check_every, interval, last_cycle_ts)
            if max_runtime:
                remain = max_runtime - (time.monotonic() - started)
                if remain <= 0:
                    logging.info("max runtime %ss reached; exiting loop", max_runtime)
                    break
                sleep_for = min(sleep_for, max(0.05, remain))
            time.sleep(sleep_for)
        except Exception:
            logging.exception("loop iteration failed")
            if max_runtime and (time.monotonic() - started) >= max_runtime:
                logging.info("max runtime %ss reached after error; exiting loop", max_runtime)
                break
            time.sleep(check_every)


def main() -> int:
    parser = argparse.ArgumentParser(description="Lebanon Telegram breaking-news watcher")
    parser.add_argument("--loop", action="store_true", help="run forever (used by start.sh)")
    parser.add_argument("--once", action="store_true", help="run a single fetch/score cycle and exit")
    parser.add_argument("--calibrate", action="store_true", help="score last ~24h of posts on the pages; do not update seen state or webhook")
    parser.add_argument("--dry-run", action="store_true", help="force dry-run even if webhook env is set")
    parser.add_argument(
        "--max-runtime",
        type=int,
        default=0,
        metavar="SECONDS",
        help="in --loop mode, exit cleanly after this many seconds (0 = run forever)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    setup_logging(verbose=args.verbose)

    def _on_signal(signum, _frame):
        logging.info("received signal %s; exiting", signum)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    cfg = load_config()
    url, key = webhook_configured()
    dry_run = args.dry_run or not (url and key)

    if args.calibrate:
        report = run_calibrate(cfg)
        print_calibration(report)
        return 0

    lock_fh = acquire_lock()
    try:
        if args.once:
            state = load_state()
            summary = run_cycle(cfg, state, dry_run=dry_run)
            print(json.dumps({k: summary[k] for k in ("counts", "errors", "new_posts", "new_candidates", "first")}, ensure_ascii=False, indent=2))
        else:
            run_loop(cfg, dry_run=dry_run, max_runtime=max(0, int(args.max_runtime or 0)))
        return 0
    finally:
        try:
            fcntl.flock(lock_fh, fcntl.LOCK_UN)
            lock_fh.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
