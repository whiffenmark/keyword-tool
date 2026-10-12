#!/usr/bin/env python3
"""Expand Google "People also ask" boxes in your signed-in Chrome and collect the questions.

Atlas's own browser tool can't click (safety guard). This script is the safe alternative: it only
clicks the accordion buttons inside the "People also ask" block of one Google results page.

Needs Chrome running with remote debugging (see atlas/PROMPT.md) and:  pip install playwright
(No browser download needed: it attaches to your running Chrome.)

Usage:
  python3 paa_expand.py --seed "medicare 2027" --merge-into ~/atlas_run.json
"""
import argparse, json, os, random, sys, time
from urllib.parse import urlencode, urlparse

ROOT_JS = """() => {
  const hs = [...document.querySelectorAll('h2,h3,[role=heading],span,div')]
    .filter(e => e.children.length === 0 && /^people also ask$/i.test((e.textContent || '').trim()));
  for (const h of hs) {
    let el = h;
    for (let i = 0; i < 8 && el; i++) {
      el = el.parentElement;
      if (el && el.querySelectorAll('[aria-expanded]').length >= 2) return el;
    }
  }
  return null;
}"""


def question_text(el):
    t = (el.inner_text() or "").strip().split("\n")[0].strip()
    return t if 8 <= len(t) <= 200 else ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", required=True)
    ap.add_argument("--cdp", default="http://127.0.0.1:9222", help="Chrome remote debugging URL")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--max", type=int, default=30, help="stop after this many questions")
    ap.add_argument("--merge-into", help="JSON file to add the questions to (key 'paa')")
    ap.add_argument("--base-url", default="https://www.google.com/search", help=argparse.SUPPRESS)
    a = ap.parse_args()

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        sys.exit("ERROR: run  pip install playwright   (no browser download needed)")

    questions, clicked = [], set()
    with sync_playwright() as p:
        try:
            browser = p.chromium.connect_over_cdp(a.cdp)
        except Exception as e:
            sys.exit(f"ERROR: cannot reach Chrome at {a.cdp} ({e.__class__.__name__}). Is it running?")
        ctx = browser.contexts[0] if browser.contexts else browser.new_context()
        page = ctx.new_page()
        try:
            page.goto(a.base_url + "?" + urlencode({"q": a.seed, "hl": "en", "gl": "us"}),
                      wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)
            url = page.url
            if "/sorry/" in url or "consent." in urlparse(url).netloc:
                sys.exit(f"ERROR: Google blocked or asked for consent ({url[:60]}). Solve it in Chrome, then retry.")
            for rnd in range(a.rounds):
                root = page.evaluate_handle(ROOT_JS).as_element()
                if root is None:
                    if rnd == 0:
                        print("NOTE: no 'People also ask' block found on this page.", file=sys.stderr)
                    break
                toggles = root.query_selector_all("[aria-expanded]")
                progressed = False
                for t in toggles:
                    q = question_text(t)
                    if not q:
                        continue
                    if q not in questions:
                        questions.append(q)
                    if q in clicked or t.get_attribute("aria-expanded") != "false":
                        continue
                    if len(questions) >= a.max:
                        break
                    clicked.add(q)
                    try:
                        t.scroll_into_view_if_needed(timeout=3000)
                        t.click(timeout=3000)
                        progressed = True
                    except Exception:
                        continue
                    time.sleep(random.uniform(0.8, 1.6))
                if not progressed or len(questions) >= a.max:
                    break
            # final sweep for questions revealed by the last round
            root = page.evaluate_handle(ROOT_JS).as_element()
            if root:
                for t in root.query_selector_all("[aria-expanded]"):
                    q = question_text(t)
                    if q and q not in questions:
                        questions.append(q)
        finally:
            page.close()

    if a.merge_into:
        path = os.path.expanduser(a.merge_into)
        data = json.load(open(path)) if os.path.exists(path) else {}
        have = data.get("paa") or []
        data["paa"] = have + [q for q in questions if q not in have]
        json.dump(data, open(path, "w"), ensure_ascii=False)
    print(f"PAA {len(questions)} questions" + (f" merged into {a.merge_into}" if a.merge_into else ""))
    if not a.merge_into:
        print(json.dumps({"paa": questions}, ensure_ascii=False))


if __name__ == "__main__":
    main()
