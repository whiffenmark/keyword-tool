# Keyword Tool

Keyword research collectors (Google, Bing and YouTube autocomplete, People Also Ask, related
searches, Reddit, Quora, local pack, SERP URLs, page keywords) with a small Flask UI, plus a
command-line collector that saves results into the **SERPVault Playbooks** `keywords` table.

## Collector (writes to SERPVault Playbooks)

`collector.py` runs on a server or laptop, not on Vercel. Each run writes rows with
`source = 'keyword_research'`, the module name, the seed, the country and a `source_ref`
(`collector:<UTC time>` or `import:<UTC time>`).

```bash
pip install -r requirements.txt
python collector.py "water damage restoration"                 # Google + Bing + YouTube autocomplete
python collector.py "water damage restoration" --alphabet      # also seed + a..z (27 queries per module)
python collector.py "mold removal" --modules ac,bing --gl gb
python collector.py --from-json atlas-results.json             # push a saved UI / Atlas payload
python collector.py "flood cleanup" --dry-run --json rows.json # print and save rows, write nothing
```

To write, set these (without them every run is a dry run):

| Variable | Value |
|---|---|
| `PLAYBOOKS_SUPABASE_URL` | `https://<project-ref>.supabase.co` |
| `PLAYBOOKS_SERVICE_KEY` | the Playbooks service-role key (server only, never in a browser or the repo) |
| `PLAYBOOKS_OWNER_ID` | the owner's auth user id; rows get this `user_id` so the app's owner-only RLS shows them |

### What works over plain HTTP (checked 2026-10-15)

| Module | Status |
|---|---|
| `ac` Google autocomplete | works; long local phrases often return nothing |
| `bing` Bing autocomplete | works |
| `youtube` YouTube autocomplete | works (uses `client=firefox&ds=yt`; the old `client=youtube` answer is JSONP) |
| `paa`, `rel`, `pasf`, local pack, SERP URLs | **empty**: Google's results page now needs JavaScript |
| `reddit` | **403** without OAuth |
| `quora` | **empty** (no questions parsed from the search page) |

The collector prints a warning for any module that returns nothing. For PAA, related searches and
SERPs use a real browser (Atlas) and push its JSON with `--from-json`.

### Copy the old `keyword_research` project

`migrate_kw_items.py` copies `kw_runs` / `kw_items` from the old Supabase project into Playbooks.
It is a dry run unless `--apply` is given, skips non-keyword modules, and refuses to copy the same
runs twice.

```bash
OLD_SUPABASE_URL=... OLD_SUPABASE_SERVICE_KEY=... python migrate_kw_items.py
OLD_SUPABASE_URL=... OLD_SUPABASE_SERVICE_KEY=... PLAYBOOKS_SUPABASE_URL=... \
  PLAYBOOKS_SERVICE_KEY=... PLAYBOOKS_OWNER_ID=... python migrate_kw_items.py --apply
```

## Research UI

```bash
python server.py        # http://localhost:5000
```

The UI's "Database" card saves to the old `kw_runs` / `kw_items` tables (`SUPABASE_URL`,
`SUPABASE_SERVICE_KEY`). The Vercel deploy only serves the static page: the Flask routes return 404
there.

## Tests

```bash
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest
```

Tests use fake fetchers and a fake HTTP session; they make no network calls.
