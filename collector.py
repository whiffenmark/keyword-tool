"""Keyword collector worker: runs the research modules for a seed and writes the results
into the SERPVault Playbooks `keywords` table (source = 'keyword_research').

Runs from the command line or cron on a server (Lyra/Orion), not on Vercel:

    python collector.py "water damage restoration" --alphabet
    python collector.py --from-json atlas-export.json          # push a saved UI / Atlas payload
    python collector.py "mold removal" --dry-run --json out.json

Writing needs three environment variables:

    PLAYBOOKS_SUPABASE_URL   https://<project-ref>.supabase.co
    PLAYBOOKS_SERVICE_KEY    the project's service-role (secret) key; server only, never in a browser
    PLAYBOOKS_OWNER_ID       the owner's auth user id; every row is written with this user_id so the
                             owner-only RLS policies in the app show it

Without them the collector runs as a dry run and only prints what it would write.
"""
import argparse, datetime, json, os, sys, time

import requests

import server

# Modules that produce keyword-like rows. Business names (local), SERP URLs and page n-grams
# are not keywords, so they stay in the research UI and are not written to the keywords table.
KEYWORD_MODULES = ["ac", "bing", "youtube", "paa", "rel", "pasf", "reddit", "quora"]
# Google's results page now needs JavaScript, so paa/rel/pasf get nothing over plain HTTP, and
# Reddit's JSON search answers 403 without OAuth (both checked 2026-10-15). Those modules stay
# available but are off by default; feed browser (Atlas) results in with --from-json instead.
DEFAULT_MODULES = ["ac", "bing", "youtube"]
BATCH_SIZE = 500
REQUEST_PAUSE = 0.5


def _ac_queries(seed, alphabet):
    if not alphabet:
        return [seed]
    return [seed] + server.build_queries(seed, "alpha", server.ALPHABET, [])


def _run_autocomplete(fetch, seed, alphabet, pause):
    out, seen = [], set()
    for query in _ac_queries(seed, alphabet):
        for s in fetch(query):
            if not isinstance(s, str):
                continue
            s = s.strip().lower()
            if s and s not in seen:
                seen.add(s); out.append(s)
        time.sleep(pause)
    return out


def collect(seed, modules=None, gl="us", location="", alphabet=False, pause=REQUEST_PAUSE):
    """Run the chosen modules for one seed. Returns the same payload shape the research UI
    saves (`{"seed":..., "gl":..., "<module>": [...]}`) so server.build_items can flatten it."""
    modules = modules or DEFAULT_MODULES
    unknown = [m for m in modules if m not in KEYWORD_MODULES]
    if unknown:
        raise ValueError(f"Unknown module(s): {', '.join(unknown)}. Choose from {', '.join(KEYWORD_MODULES)}.")
    hl = server.GL_MAP.get(gl, "en")
    query = f"{seed} {location}".strip() if location else seed
    server.clear_google_cache()
    data = {"seed": seed, "gl": gl, "location": location}
    fetchers = {
        "ac":      lambda: _run_autocomplete(lambda q: server.get_autocomplete(q, hl, gl), query, alphabet, pause),
        "bing":    lambda: _run_autocomplete(server.get_bing_autocomplete, query, alphabet, pause),
        "youtube": lambda: _run_autocomplete(server.get_youtube_autocomplete, query, alphabet, pause),
        "paa":     lambda: server.get_people_also_ask(query, hl, gl),
        "rel":     lambda: server.get_related_searches(query, hl, gl),
        "pasf":    lambda: server.get_people_also_search(query, hl, gl),
        "reddit":  lambda: [{"title": r["text"], "source": r["source"], "type": r["type"]} for r in server.get_reddit_results(query)],
        "quora":   lambda: [{"title": r["text"], "source": r["source"], "type": r["type"]} for r in server.get_quora_results(query)],
    }
    for m in modules:
        data[m] = fetchers[m]()
        if not data[m]:
            print(f"Warning: module '{m}' returned nothing for '{query}'.", file=sys.stderr)
        time.sleep(pause)
    return data


def to_playbooks_rows(payload, owner_id, run_ref, modules=None):
    """Map a research payload to rows for the Playbooks `keywords` table."""
    seed = (payload.get("seed") or "").strip()
    if not seed:
        raise ValueError("Payload has no seed")
    country = (payload.get("gl") or payload.get("country") or "").strip()
    location = (payload.get("location") or "").strip()
    allowed = set(modules or KEYWORD_MODULES)
    rows, seen = [], set()
    for item in server.build_items(payload):
        module, keyword = item["module"], item["keyword"].strip()
        if module not in allowed or not keyword:
            continue
        key = (module, keyword.lower())
        if key in seen:
            continue
        seen.add(key)
        row = {
            "source": "keyword_research",
            "module": module,
            "seed": seed,
            "source_ref": run_ref,
            "keyword": keyword,
            "country": country,
            "location": location,
            "raw": item["extra"] if isinstance(item["extra"], dict) else {},
        }
        if owner_id:
            row["user_id"] = owner_id
        rows.append(row)
    return rows


class PlaybooksWriter:
    """Inserts rows into the Playbooks `keywords` table through the Supabase REST API."""

    def __init__(self, url, service_key, session=None):
        self.url = url.rstrip("/")
        self.key = service_key
        self.session = session or requests.Session()

    def insert_keywords(self, rows):
        written = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            r = self.session.post(
                f"{self.url}/rest/v1/keywords",
                json=batch,
                headers={"apikey": self.key, "Authorization": f"Bearer {self.key}",
                         "Content-Type": "application/json", "Prefer": "return=minimal"},
                timeout=30,
            )
            r.raise_for_status()
            written += len(batch)
        return written


def writer_from_env():
    url, key, owner = (os.environ.get(k, "").strip() for k in
                       ("PLAYBOOKS_SUPABASE_URL", "PLAYBOOKS_SERVICE_KEY", "PLAYBOOKS_OWNER_ID"))
    if url and key and owner:
        return PlaybooksWriter(url, key), owner
    return None, owner


def run_ref_now(prefix="collector"):
    return f"{prefix}:{datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"


def main(argv=None):
    p = argparse.ArgumentParser(description="Collect keywords for a seed and save them to SERPVault Playbooks.")
    p.add_argument("seed", nargs="?", help="Seed keyword (omit with --from-json)")
    p.add_argument("--modules", default=",".join(DEFAULT_MODULES),
                   help=f"Comma list from: {','.join(KEYWORD_MODULES)} (default {','.join(DEFAULT_MODULES)})")
    p.add_argument("--gl", default="us", help="Country code (default us)")
    p.add_argument("--location", default="", help="Appended to the seed, e.g. 'Malibu CA'")
    p.add_argument("--alphabet", action="store_true", help="Also query seed + a..z for autocomplete modules")
    p.add_argument("--from-json", help="Push a saved research payload (UI export or Atlas JSON) instead of collecting")
    p.add_argument("--json", help="Also write the rows to this file")
    p.add_argument("--dry-run", action="store_true", help="Print what would be written; write nothing")
    args = p.parse_args(argv)

    modules = [m.strip() for m in args.modules.split(",") if m.strip()]
    if args.from_json:
        with open(args.from_json) as f:
            payload = json.load(f)
        run_ref = run_ref_now("import")
    else:
        if not args.seed:
            p.error("a seed is required unless --from-json is given")
        payload = collect(args.seed, modules, args.gl, args.location, args.alphabet)
        run_ref = run_ref_now()

    writer, owner = writer_from_env()
    rows = to_playbooks_rows(payload, owner or None, run_ref, None if args.from_json else modules)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(rows, f, indent=2)

    counts = {}
    for r in rows:
        counts[r["module"]] = counts.get(r["module"], 0) + 1
    summary = ", ".join(f"{m} {n}" for m, n in sorted(counts.items())) or "none"
    print(f"{len(rows)} keyword(s) for '{payload.get('seed')}' ({summary}); ref {run_ref}")

    if args.dry_run or writer is None:
        if writer is None and not args.dry_run:
            print("Dry run: set PLAYBOOKS_SUPABASE_URL, PLAYBOOKS_SERVICE_KEY and PLAYBOOKS_OWNER_ID to write.")
        for r in rows[:20]:
            print(f"  [{r['module']}] {r['keyword']}")
        if len(rows) > 20:
            print(f"  ... {len(rows) - 20} more")
        return 0

    try:
        written = writer.insert_keywords(rows)
    except requests.RequestException as e:
        print(f"Write failed: {e}", file=sys.stderr)
        return 2
    print(f"Wrote {written} row(s) to Playbooks keywords.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
