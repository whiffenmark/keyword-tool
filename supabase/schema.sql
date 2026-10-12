-- Keyword research storage. One "run" per saved research session,
-- many "items" (keywords, questions, URLs, businesses) per run.

create table if not exists kw_runs (
  id          bigint generated always as identity primary key,
  seed        text not null,
  country     text default 'us',
  source      text not null default 'tool',   -- 'tool' or 'atlas'
  created_at  timestamptz not null default now()
);

create table if not exists kw_items (
  id          bigint generated always as identity primary key,
  run_id      bigint not null references kw_runs(id) on delete cascade,
  module      text not null,                  -- ac, bing, youtube, paa, rel, pasf, reddit, quora, local, serp_google, serp_bing, page, schema_paa
  keyword     text not null,
  extra       jsonb not null default '{}'::jsonb,  -- url, rank, rating, subreddit, etc.
  created_at  timestamptz not null default now()
);

create index if not exists kw_items_run_idx     on kw_items(run_id);
create index if not exists kw_items_keyword_idx on kw_items(lower(keyword));
create index if not exists kw_runs_seed_idx     on kw_runs(lower(seed));

-- Locked down: only the server (service key) can read or write.
alter table kw_runs  enable row level security;
alter table kw_items enable row level security;

-- Let the server (service_role key) use the tables. Public/anon roles get nothing.
grant usage on schema public to service_role;
grant all on kw_runs, kw_items to service_role;
grant usage, select on all sequences in schema public to service_role;
