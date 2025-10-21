# utils/supabase_client.py
import os, requests, json, re, urllib.parse, socket

def _clean_env_url(s):
    if not s:
        return s
    # trim whitespace/newlines
    s = s.strip()
    # remove obvious URL-encoded newlines and CRLF and common zero-width chars
    s = re.sub(r'(%0A|%0a|\\r|\\n)', '', s)
    s = re.sub(r'[\x00-\x1f\x7f-\x9f\u200b\u200c\u200e\u200f]', '', s)
    s = s.strip()
    # ensure no trailing slash
    return s.rstrip('/')

SUPABASE_URL_RAW = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "").strip()

SUPABASE_URL = _clean_env_url(SUPABASE_URL_RAW)

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    raise RuntimeError("Supabase environment variables SUPABASE_URL and SUPABASE_ANON_KEY are required and must not be empty.")

# parse host and verify DNS resolves (fail fast with clear message)
try:
    parsed = urllib.parse.urlparse(SUPABASE_URL if SUPABASE_URL.startswith("http") else ("https://" + SUPABASE_URL))
    host = parsed.hostname
    if not host:
        raise RuntimeError(f"Could not parse hostname from SUPABASE_URL: {SUPABASE_URL!r}")
    # attempt DNS resolution (raises socket.gaierror on failure)
    try:
        socket.gethostbyname(host)
    except Exception as e:
        raise RuntimeError(f"DNS resolution failed for host {host!r}: {e}")
except Exception as e:
    raise RuntimeError(f"SUPABASE_URL parse/resolve error: {e}")

HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

def upsert_posts(rows, batch_size=50):
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/posts"
    clean_rows = []
    for r in rows:
        clean = {}
        for k,v in r.items():
            if v is None:
                clean[k] = None
            else:
                try:
                    json.dumps(v)
                    clean[k] = v
                except Exception:
                    clean[k] = str(v)
        clean_rows.append(clean)

    for i in range(0, len(clean_rows), batch_size):
        batch = clean_rows[i:i+batch_size]
        params = {"on_conflict": "hash"}
        resp = requests.post(url, params=params, headers=HEADERS, data=json.dumps(batch), timeout=20)
        if resp.status_code in (200,201):
            continue
        if resp.status_code == 409:
            for row in batch:
                r1 = requests.post(url, params=params, headers=HEADERS, data=json.dumps([row]), timeout=20)
                if r1.status_code not in (200,201):
                    raise Exception(f"Row insert failed: {r1.status_code} {r1.text}")
            continue
        raise Exception(f"Batch insert failed after inserting 0 rows: {resp.status_code} {resp.text}")
