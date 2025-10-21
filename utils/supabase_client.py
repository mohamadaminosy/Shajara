# utils/supabase_client.py
# Simple Supabase REST helper for inserting/upserting posts.
import os, requests, json, math

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")

HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

def upsert_posts(rows, batch_size=50):
    if not rows:
        return
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        raise RuntimeError("Supabase environment variables SUPABASE_URL and SUPABASE_ANON_KEY are required.")
    url = f"{SUPABASE_URL}/rest/v1/posts"
    # ensure rows are JSON-serializable (e.g. None instead of Python None for some fields)
    clean_rows = []
    for r in rows:
        # supabase expects JSON null for missing; ensure no unsupported types
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

    # post in batches
    for i in range(0, len(clean_rows), batch_size):
        batch = clean_rows[i:i+batch_size]
        params = {"on_conflict": "hash"}
        resp = requests.post(url, params=params, headers=HEADERS, data=json.dumps(batch))
        if resp.status_code in (200,201):
            # success
            continue
        # handle conflict errors or server errors
        if resp.status_code == 409:
            # conflict — try inserting rows one by one to skip duplicates
            for row in batch:
                r1 = requests.post(url, params=params, headers=HEADERS, data=json.dumps([row]))
                if r1.status_code not in (200,201):
                    raise Exception(f"Row insert failed: {r1.status_code} {r1.text}")
            continue
        raise Exception(f"Batch insert failed after inserting 0 rows: {resp.status_code} {resp.text}")
