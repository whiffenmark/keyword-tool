"""One-off copy of saved research from the old `keyword_research` Supabase project
(kw_runs / kw_items) into the SERPVault Playbooks `keywords` table.

    python migrate_kw_items.py            # dry run: reads the old project, prints what would be copied
    python migrate_kw_items.py --apply    # writes to Playbooks

Reads   OLD_SUPABASE_URL, OLD_SUPABASE_SERVICE_KEY            (the old keyword_research project)
Writes  PLAYBOOKS_SUPABASE_URL, PLAYBOOKS_SERVICE_KEY, PLAYBOOKS_OWNER_ID

Only keyword modules are copied (see collector.KEYWORD_MODULES). Local businesses, SERP URLs and
page n-grams are counted and skipped. Each row keeps its run as source_ref = "kw_runs:<id>", so a
second --apply would duplicate rows: the script refuses if Playbooks already has rows with those refs.
"""
import argparse, os, sys

import requests

import collector


def rest_get(url, key, table, params, session=None):
    s = session or requests
    r = s.get(f"{url.rstrip('/')}/rest/v1/{table}", params=params,
              headers={"apikey": key, "Authorization": f"Bearer {key}"}, timeout=30)
    r.raise_for_status()
    return r.json()


def in_filter(values):
    """PostgREST `in` filter; values are double-quoted because refs contain ':'."""
    return "in.(" + ",".join('"' + v.replace('"', '') + '"' for v in values) + ")"


def map_rows(runs, items, owner_id):
    """Turn old kw_runs + kw_items into Playbooks keyword rows. Returns (rows, skipped_by_module)."""
    by_id = {r["id"]: r for r in runs}
    rows, skipped, seen = [], {}, set()
    for it in items:
        run = by_id.get(it["run_id"])
        module = it.get("module", "")
        keyword = (it.get("keyword") or "").strip()
        if run is None or module not in collector.KEYWORD_MODULES or not keyword:
            skipped[module or "?"] = skipped.get(module or "?", 0) + 1
            continue
        key = (run["id"], module, keyword.lower())
        if key in seen:
            continue
        seen.add(key)
        extra = it.get("extra") if isinstance(it.get("extra"), dict) else {}
        row = {
            "source": "keyword_research",
            "module": module,
            "seed": run.get("seed") or "",
            "source_ref": f"kw_runs:{run['id']}",
            "keyword": keyword,
            "country": run.get("country") or "",
            "location": "",
            "raw": {**extra, "migrated_from": "keyword_research", "original_created_at": it.get("created_at"),
                    "run_source": run.get("source")},
        }
        if owner_id:
            row["user_id"] = owner_id
        rows.append(row)
    return rows, skipped


def main(argv=None):
    p = argparse.ArgumentParser(description="Copy kw_runs/kw_items into Playbooks keywords.")
    p.add_argument("--apply", action="store_true", help="Write to Playbooks (default is a dry run)")
    args = p.parse_args(argv)

    old_url, old_key = os.environ.get("OLD_SUPABASE_URL", ""), os.environ.get("OLD_SUPABASE_SERVICE_KEY", "")
    if not (old_url and old_key):
        print("Set OLD_SUPABASE_URL and OLD_SUPABASE_SERVICE_KEY.", file=sys.stderr)
        return 1
    runs = rest_get(old_url, old_key, "kw_runs", {"select": "id,seed,country,source,created_at", "order": "id.asc"})
    items = rest_get(old_url, old_key, "kw_items",
                     {"select": "run_id,module,keyword,extra,created_at", "order": "id.asc", "limit": "100000"})

    writer, owner = collector.writer_from_env()
    rows, skipped = map_rows(runs, items, owner or None)
    print(f"Old project: {len(runs)} run(s), {len(items)} item(s). To copy: {len(rows)} keyword row(s).")
    if skipped:
        print("Skipped (not keyword modules): " + ", ".join(f"{m} {n}" for m, n in sorted(skipped.items())))

    if not args.apply:
        print("Dry run. Re-run with --apply to write.")
        return 0
    if writer is None:
        print("Set PLAYBOOKS_SUPABASE_URL, PLAYBOOKS_SERVICE_KEY and PLAYBOOKS_OWNER_ID to write.", file=sys.stderr)
        return 1
    refs = sorted({r["source_ref"] for r in rows})
    if refs:
        existing = rest_get(writer.url, writer.key, "keywords",
                            {"select": "id", "source_ref": in_filter(refs), "limit": "1"})
        if existing:
            print("Playbooks already has rows from these runs; refusing to copy twice.", file=sys.stderr)
            return 1
    written = writer.insert_keywords(rows)
    print(f"Wrote {written} row(s) to Playbooks keywords.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
