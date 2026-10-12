# Atlas keyword research prompt

One-time setup on the laptop:

    git clone https://github.com/whiffenmark/keyword-tool ~/keyword-tool
    nano ~/.keyword_tool.env      # two lines, then: chmod 600 ~/.keyword_tool.env
        SUPABASE_URL=https://nlosvzvhyyttmgjqdetc.supabase.co
        SUPABASE_SERVICE_KEY=<secret key from Supabase > Project Settings > API Keys>

Per run: start the signed-in Chrome (see README steps), run `/browser connect` in a NEW Hermes
session, then paste the prompt below with the seed changed.

---

Keyword research for: medicare 2027 (US, English). Use my signed-in Chrome (already connected).
Hard limit: 6 page loads. This needs 3.

1. google.com/search?q=<seed with + for spaces>. On this ONE page copy:
   - every "People also ask" question (expand if allowed, max 3 rounds, no new page loads)
   - every "Related searches" phrase at the bottom
   - "People also search for" phrases if present (keep these separate from related)
2. reddit.com/search/?q=<seed> : top 15 thread titles, subreddit, link if visible.
3. quora.com/search?q=<seed> : top 15 question titles, link if visible.

Rules: retry a failed page at most ONCE. Do not troubleshoot, edit config, kill processes or read
code. On a CAPTCHA, stop that page and note it. Wait 3-5 seconds between pages. Record only what
you actually saw. No invention. Remove duplicates.

Then:
a) Write what you found to ~/atlas_run.json in exactly this shape (empty list if not found):
   {"paa":[],"rel":[],"pasf":[],"reddit":[{"title":"","subreddit":"","url":""}],"quora":[{"title":"","url":""}]}
b) Run:  python3 ~/keyword-tool/scripts/atlas_save.py --seed "medicare 2027" --browser-json ~/atlas_run.json
   (It adds Google, Bing and YouTube autocomplete itself and saves everything to the database.
   It takes about 40 seconds. Do not read or print any key or env file.)

Reply with ONLY: the script's final line (starts with SAVED or ERROR), then one line listing
anything that failed or was blocked.
