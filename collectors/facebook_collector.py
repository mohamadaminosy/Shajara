# collectors/facebook_collector.py
# Enhanced Facebook collector
# - ensures utils import works when run from subfolder
# - uses cookies when provided and passes account/group correctly
# - fallback to unauthenticated attempt and prints useful hints

# --- BEGIN: ensure repo root in PYTHONPATH (add this at top of file) ---
import os, sys
HERE = os.path.dirname(__file__)
CANDIDATE = os.path.abspath(os.path.join(HERE, ".."))
if os.path.isdir(os.path.join(CANDIDATE, "utils")) and CANDIDATE not in sys.path:
    sys.path.insert(0, CANDIDATE)
else:
    REPO_ROOT = os.path.abspath(os.getcwd())
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)
# --- END ---

import re
import math
import hashlib
import json
import time
from datetime import datetime, timezone
import pandas as pd

from facebook_scraper import get_posts, set_user_agent

# Supabase helper
from utils.supabase_client import upsert_posts

# ---------------- Settings (from env) ----------------
PAGE_URLS = [u.strip() for u in os.environ.get("FB_PAGES", "https://www.facebook.com/Suwayda24,https://www.facebook.com/groups/zero0nine9").split(",") if u.strip()]
POSTS_LIMIT = int(os.environ.get("FB_LIMIT", "200") or 200)
USER_AGENT = os.environ.get("FB_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
FB_COOKIES_JSON = os.environ.get("FB_COOKIES_JSON", "").strip()

# ---------------- Helpers ----------------
def _identify(url: str):
    """Return (kind, ident, canonical_source_url)
       kind = 'group' or 'account' or 'post' or 'unknown'
       ident = id or username (string) or empty
    """
    if not url:
        return "unknown", "", url
    from urllib.parse import urlparse, parse_qs
    # normalize mobile/mbasic to www
    url = re.sub(r"^(https?://)(m\.|mbasic\.)?facebook\.com", r"\1www.facebook.com", url.strip())
    p = urlparse(url)
    path = (p.path or "").strip("/")
    if not path:
        return "account", "", url
    # groups/{id}
    if path.startswith("groups/"):
        parts = path.split("/")
        ident = parts[1] if len(parts) > 1 else ""
        return "group", ident, f"https://www.facebook.com/groups/{ident}"
    # profile.php?id=123
    if "profile.php" in (p.path or ""):
        q = parse_qs(p.query or "")
        pid = q.get("id", [""])[0]
        return "account", pid, f"https://www.facebook.com/profile.php?id={pid}"
    # permalink with id param
    if "permalink.php" in (p.path or ""):
        q = parse_qs(p.query or "")
        pid = q.get("id", [""])[0]
        if pid:
            return "account", pid, f"https://www.facebook.com/profile.php?id={pid}"
    # people/.../{id}
    m = re.search(r"/people/[^/]+/(\d+)", p.path or "")
    if m:
        pid = m.group(1)
        return "account", pid, f"https://www.facebook.com/people/x/{pid}"
    # otherwise take first path segment as username
    ident = path.split("/")[0]
    return "account", ident, f"https://www.facebook.com/{ident}"
