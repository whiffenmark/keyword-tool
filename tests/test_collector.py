import json

import pytest

import collector
import migrate_kw_items
import server

# Constraints of the Playbooks `keywords` table (supabase/migrations/20261014120000_add_serpvault_tables.sql
# in serpvault-playbooks) that rows from this repo must meet.
ALLOWED_SOURCES = {"upload", "hermes", "keyword_research", "manual"}


def assert_valid_keyword_row(row):
    assert row["source"] == "keyword_research" and row["source"] in ALLOWED_SOURCES
    assert "upload_id" not in row and "hermes_import_id" not in row
    assert row["keyword"].strip()
    assert isinstance(row["raw"], dict)
    for col in ("module", "seed", "source_ref", "country", "location"):
        assert isinstance(row[col], str)


@pytest.fixture
def fake_fetchers(monkeypatch):
    calls = []
    monkeypatch.setattr(collector, "REQUEST_PAUSE", 0)
    monkeypatch.setattr(collector.time, "sleep", lambda s: None)
    monkeypatch.setattr(server, "get_autocomplete", lambda q, hl="en", gl="us": calls.append(("ac", q)) or [f"{q} cost", f"{q} near me", "  "])
    monkeypatch.setattr(server, "get_people_also_ask", lambda q, hl="en", gl="us": ["How much does water damage repair cost?"])
    monkeypatch.setattr(server, "get_related_searches", lambda q, hl="en", gl="us": ["water damage insurance", "Water Damage Insurance"])
    monkeypatch.setattr(server, "get_people_also_search", lambda q, hl="en", gl="us": [])
    monkeypatch.setattr(server, "get_reddit_results", lambda q: [{"text": "Ceiling leak help", "source": "r/HomeImprovement", "type": "post"}])
    return calls


def test_collect_builds_ui_payload(fake_fetchers):
    data = collector.collect("water damage", ["ac", "paa", "rel", "pasf", "reddit"], gl="us", location="Malibu CA")
    assert data["seed"] == "water damage" and data["location"] == "Malibu CA"
    assert data["ac"] == ["water damage malibu ca cost", "water damage malibu ca near me"]
    assert data["paa"] == ["How much does water damage repair cost?"]
    assert data["reddit"][0]["title"] == "Ceiling leak help"
    assert fake_fetchers == [("ac", "water damage Malibu CA")]


def test_collect_alphabet_expands_autocomplete(fake_fetchers):
    collector.collect("mold", ["ac"], alphabet=True)
    assert len(fake_fetchers) == 27
    assert fake_fetchers[1] == ("ac", "mold a")


def test_collect_rejects_unknown_module():
    with pytest.raises(ValueError, match="local"):
        collector.collect("x", ["local"])


def test_rows_match_keywords_table(fake_fetchers):
    data = collector.collect("water damage", ["ac", "paa", "rel", "reddit"])
    rows = collector.to_playbooks_rows(data, "11111111-1111-1111-1111-111111111111", "collector:test")
    assert rows
    for r in rows:
        assert_valid_keyword_row(r)
        assert r["user_id"] == "11111111-1111-1111-1111-111111111111"
        assert r["seed"] == "water damage" and r["source_ref"] == "collector:test" and r["country"] == "us"
    rel = [r["keyword"] for r in rows if r["module"] == "rel"]
    assert rel == ["water damage insurance"], "same keyword in one module is kept once (case-insensitive)"
    reddit = [r for r in rows if r["module"] == "reddit"][0]
    assert reddit["raw"] == {"source": "r/HomeImprovement", "type": "post"}


def test_rows_skip_non_keyword_modules():
    payload = {"seed": "roofing", "gl": "us",
               "paa": ["Is a roof leak an emergency?"],
               "local": [{"name": "Acme Roofing", "rating": "4.8"}],
               "serp_google": [{"keyword": "roofing", "urls": [{"rank": 1, "url": "https://a.com", "domain": "a.com"}]}]}
    rows = collector.to_playbooks_rows(payload, None, "import:x")
    assert [r["module"] for r in rows] == ["paa"]
    assert "user_id" not in rows[0]


def test_rows_require_seed():
    with pytest.raises(ValueError):
        collector.to_playbooks_rows({"paa": ["x?"]}, None, "r")


class FakeResponse:
    def __init__(self, status=201):
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise collector.requests.HTTPError(f"{self.status_code}")


class FakeSession:
    def __init__(self, status=201):
        self.posts, self.status = [], status

    def post(self, url, json=None, headers=None, timeout=None):
        self.posts.append({"url": url, "json": json, "headers": headers})
        return FakeResponse(self.status)


def test_writer_batches_and_sends_service_key(monkeypatch):
    monkeypatch.setattr(collector, "BATCH_SIZE", 2)
    s = FakeSession()
    w = collector.PlaybooksWriter("https://abc.supabase.co/", "svc", session=s)
    assert w.insert_keywords([{"keyword": str(i)} for i in range(5)]) == 5
    assert [len(p["json"]) for p in s.posts] == [2, 2, 1]
    assert s.posts[0]["url"] == "https://abc.supabase.co/rest/v1/keywords"
    assert s.posts[0]["headers"]["apikey"] == "svc" and s.posts[0]["headers"]["Prefer"] == "return=minimal"


def test_writer_raises_on_http_error():
    w = collector.PlaybooksWriter("https://abc.supabase.co", "svc", session=FakeSession(status=401))
    with pytest.raises(collector.requests.HTTPError):
        w.insert_keywords([{"keyword": "x"}])


def test_writer_from_env_needs_all_three(monkeypatch):
    for k in ("PLAYBOOKS_SUPABASE_URL", "PLAYBOOKS_SERVICE_KEY", "PLAYBOOKS_OWNER_ID"):
        monkeypatch.delenv(k, raising=False)
    assert collector.writer_from_env() == (None, "")
    monkeypatch.setenv("PLAYBOOKS_SUPABASE_URL", "https://abc.supabase.co")
    monkeypatch.setenv("PLAYBOOKS_SERVICE_KEY", "svc")
    assert collector.writer_from_env()[0] is None
    monkeypatch.setenv("PLAYBOOKS_OWNER_ID", "owner")
    w, owner = collector.writer_from_env()
    assert isinstance(w, collector.PlaybooksWriter) and owner == "owner"


def test_cli_from_json_without_env_is_a_dry_run(tmp_path, monkeypatch, capsys):
    for k in ("PLAYBOOKS_SUPABASE_URL", "PLAYBOOKS_SERVICE_KEY", "PLAYBOOKS_OWNER_ID"):
        monkeypatch.delenv(k, raising=False)
    src = tmp_path / "atlas.json"
    src.write_text(json.dumps({"seed": "flood cleanup", "gl": "us", "paa": ["What is flood cleanup?"], "rel": ["flood cleanup cost"]}))
    out = tmp_path / "rows.json"
    assert collector.main(["--from-json", str(src), "--json", str(out)]) == 0
    printed = capsys.readouterr().out
    assert "2 keyword(s) for 'flood cleanup'" in printed and "Dry run" in printed
    rows = json.loads(out.read_text())
    assert {r["module"] for r in rows} == {"paa", "rel"}
    assert all(r["source_ref"].startswith("import:") for r in rows)


def test_cli_writes_when_env_set(fake_fetchers, monkeypatch, capsys):
    s = FakeSession()
    monkeypatch.setattr(collector, "writer_from_env", lambda: (collector.PlaybooksWriter("https://abc.supabase.co", "svc", session=s), "owner-1"))
    assert collector.main(["water damage", "--modules", "paa,rel"]) == 0
    sent = s.posts[0]["json"]
    assert {r["module"] for r in sent} == {"paa", "rel"} and all(r["user_id"] == "owner-1" for r in sent)
    assert "Wrote 2 row(s)" in capsys.readouterr().out


def test_cli_seed_required():
    with pytest.raises(SystemExit):
        collector.main([])


def test_migration_maps_old_rows():
    runs = [{"id": 1, "seed": "water damage", "country": "us", "source": "atlas", "created_at": "2026-10-12"}]
    items = [
        {"run_id": 1, "module": "paa", "keyword": "Is water damage covered?", "extra": {}, "created_at": "t1"},
        {"run_id": 1, "module": "paa", "keyword": "is water damage covered?", "extra": {}, "created_at": "t2"},
        {"run_id": 1, "module": "local", "keyword": "Acme Restoration", "extra": {"rating": "4.9"}, "created_at": "t3"},
        {"run_id": 1, "module": "serp_google", "keyword": "water damage", "extra": {"url": "https://a.com"}, "created_at": "t4"},
        {"run_id": 9, "module": "ac", "keyword": "orphan", "extra": {}, "created_at": "t5"},
    ]
    rows, skipped = migrate_kw_items.map_rows(runs, items, "owner-1")
    assert len(rows) == 1
    assert_valid_keyword_row(rows[0])
    assert rows[0]["source_ref"] == "kw_runs:1" and rows[0]["user_id"] == "owner-1"
    assert rows[0]["raw"]["migrated_from"] == "keyword_research" and rows[0]["raw"]["run_source"] == "atlas"
    assert skipped == {"local": 1, "serp_google": 1, "ac": 1}


def test_migration_in_filter_quotes_refs():
    assert migrate_kw_items.in_filter(["kw_runs:1", "kw_runs:2"]) == 'in.("kw_runs:1","kw_runs:2")'


def test_migration_dry_run_needs_old_env(monkeypatch, capsys):
    monkeypatch.delenv("OLD_SUPABASE_URL", raising=False)
    monkeypatch.delenv("OLD_SUPABASE_SERVICE_KEY", raising=False)
    assert migrate_kw_items.main([]) == 1


class FakeJsonResponse:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


def test_youtube_autocomplete_uses_json_dataset(monkeypatch):
    seen = {}

    def fake_get(url, params=None, timeout=None, headers=None):
        seen.update(params)
        return FakeJsonResponse(["water damage", ["water damage sound", ["water damage iphone", 0], 7, []]])

    monkeypatch.setattr(server.requests, "get", fake_get)
    assert server.get_youtube_autocomplete("water damage") == ["water damage sound", "water damage iphone"]
    assert seen["client"] == "firefox" and seen["ds"] == "yt"


def test_collect_warns_on_empty_module(monkeypatch, capsys):
    monkeypatch.setattr(collector.time, "sleep", lambda s: None)
    monkeypatch.setattr(server, "get_people_also_ask", lambda q, hl="en", gl="us": [])
    collector.collect("roof leak", ["paa"])
    assert "module 'paa' returned nothing" in capsys.readouterr().err
