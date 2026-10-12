#!/usr/bin/env python3
"""Collect autocomplete keywords and save a full keyword research run to Supabase.

Usage:
  python3 atlas_save.py --seed "medicare 2027" --browser-json ~/atlas_run.json
  python3 atlas_save.py --seed "medicare 2027" --dry-run

--browser-json is the file Atlas writes after browsing (paa, rel, pasf, reddit, quora).
Autocomplete (Google, Bing, YouTube) is fetched here, so Atlas doesn't need to.
Needs SUPABASE_URL and SUPABASE_SERVICE_KEY, set in the environment or in
~/.keyword_tool.env (KEY=VALUE lines). Standard library only.
"""
import argparse, json, os, string, sys, time, urllib.parse, urllib.request

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}
LIST_MODULES = ["ac", "bing", "youtube", "paa", "rel", "pasf", "reddit", "quora"]


def load_env():
    path = os.path.expanduser("~/.keyword_tool.env")
    if os.path.exists(path):
        for line in open(path):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))


def get(url, params):
    req = urllib.request.Request(url + "?" + urllib.parse.urlencode(params), headers=UA)
    return urllib.request.urlopen(req, timeout=10).read().decode("utf-8", "replace")


def google(q):
    return [s for s in json.loads(get("https://suggestqueries.google.com/complete/search",
            {"client": "firefox", "q": q, "hl": "en", "gl": "us"}))[1] if isinstance(s, str)]


def bing(q):
    d = json.loads(get("https://api.bing.com/osjson.aspx", {"query": q, "language": "en-US"}))
    return d[1] if len(d) > 1 else []


def youtube(q):
    t = get("https://suggestqueries.google.com/complete/search",
            {"client": "youtube", "q": q, "hl": "en", "gl": "us"})
    d = json.loads(t[t.index("(") + 1:t.rindex(")")])  # JSONP wrapper
    return [i[0] for i in d[1] if isinstance(i, list) and i and isinstance(i[0], str)]


def autocomplete(seed):
    out = {"ac": [], "bing": [], "youtube": []}
    queries = [seed] + [f"{seed} {c}" for c in string.ascii_lowercase]
    for name, fn in (("ac", google), ("bing", bing), ("youtube", youtube)):
        for q in queries:
            try:
                out[name] += fn(q)
            except Exception:
                pass
            time.sleep(0.25)
    return out


def build_items(data):
    rows, seen = [], set()

    def add(module, keyword, extra=None):
        keyword, extra = (keyword or "").strip(), extra or {}
        key = (module, keyword.lower(), extra.get("url", ""))
        if keyword and key not in seen:
            seen.add(key)
            rows.append({"module": module, "keyword": keyword, "extra": extra})

    for m in LIST_MODULES:
        for item in data.get(m) or []:
            if isinstance(item, dict):
                add(m, item.get("title") or item.get("keyword") or item.get("question"),
                    {k: v for k, v in item.items() if k not in ("title", "keyword", "question")})
            else:
                add(m, str(item))
    return rows


def sb(method, table, body=None, params=None, prefer="return=representation"):
    url = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1/" + table
    if params:
        url += "?" + urllib.parse.urlencode(params)
    key = os.environ["SUPABASE_SERVICE_KEY"]
    req = urllib.request.Request(url, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"apikey": key, "Authorization": "Bearer " + key,
                 "Content-Type": "application/json", "Prefer": prefer})
    raw = urllib.request.urlopen(req, timeout=30).read().decode()
    return json.loads(raw) if raw else []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", required=True)
    ap.add_argument("--browser-json", help="JSON file written by Atlas after browsing")
    ap.add_argument("--no-autocomplete", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="count items, don't save")
    a = ap.parse_args()
    load_env()

    data = {}
    if a.browser_json:
        data.update(json.load(open(os.path.expanduser(a.browser_json))))
    if not a.no_autocomplete:
        data.update(autocomplete(a.seed))
    rows = build_items(data)

    counts = {}
    for r in rows:
        counts[r["module"]] = counts.get(r["module"], 0) + 1
    if a.dry_run or not rows:
        print("DRY RUN" if a.dry_run else "NOTHING TO SAVE", json.dumps(counts))
        return
    if not (os.environ.get("SUPABASE_URL") and os.environ.get("SUPABASE_SERVICE_KEY")):
        sys.exit("ERROR: set SUPABASE_URL and SUPABASE_SERVICE_KEY (env or ~/.keyword_tool.env)")
    run = sb("POST", "kw_runs", {"seed": a.seed, "country": "us", "source": "atlas"})[0]
    for i in range(0, len(rows), 500):
        sb("POST", "kw_items", [dict(r, run_id=run["id"]) for r in rows[i:i + 500]],
           prefer="return=minimal")
    print(f"SAVED run {run['id']} seed={a.seed!r} items={len(rows)} {json.dumps(counts)}")


if __name__ == "__main__":
    main()
