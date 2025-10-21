# utils/supabase_client.py
# Simple Supabase REST helper for inserting/upserting posts.
import os, requests, json

def _clean_env_url(s):
    if not s:
        return s
    # remove leading/trailing whitespace and newlines
    s = s.strip()
    # remove accidental URL-encoded newlines or carriage returns
    s = s.replace("%0A", "").replace("%0a", "")
    s = s.replace("\\n", "").replace("\\r", "")
    # finally ensure no trailing slash (we add path later)
    return s.rstrip("/")

SUPABASE_URL_RAW = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY_RAW = os.environ.get("SUPABASE_ANON_KEY", "")

SUPABASE_URL = _clean_env_url(SUPABASE_URL_RAW)
SUPABASE_ANON_KEY = SUPABASE_ANON_KEY_RAW.strip() if isinstance(SUPABASE_ANON_KEY_RAW, str) else SUPABASE_ANON_KEY_RAW

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    raise RuntimeError("Supabase environment variables SUPABASE_URL and SUPABASE_ANON_KEY are required and must not be empty.")

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
    # ensure rows are JSON-serializable (e.g. None instead of Python None for some fields)
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
        resp = requests.post(url, params=params, headers=HEADERS, data=json.dumps(batch))
        if resp.status_code in (200,201):
            continue
        if resp.status_code == 409:
            for row in batch:
                r1 = requests.post(url, params=params, headers=HEADERS, data=json.dumps([row]))
                if r1.status_code not in (200,201):
                    raise Exception(f"Row insert failed: {r1.status_code} {r1.text}")
            continue
        raise Exception(f"Batch insert failed after inserting 0 rows: {resp.status_code} {resp.text}")
