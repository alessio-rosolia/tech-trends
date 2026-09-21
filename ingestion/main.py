import os
import json
import time
import re
import threading
import requests
import feedparser
from fluent import sender
import websocket

# Configurazione Fluentd
FLUENTD_HOST = os.getenv("FLUENTD_HOST", "fluentd")
FLUENTD_PORT = int(os.getenv("FLUENTD_PORT", 24224))
logger = sender.FluentSender('social_stream', host=FLUENTD_HOST, port=FLUENTD_PORT)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) TechPipeline/2.0"}

def emit_event(tag: str, text: str, author: str, url: str, source: str):
    if not text or len(text.strip()) < 10:
        return
    data = {
        "text": text.strip()[:1000],
        "author": author or "unknown",
        "url": url or "",
        "source": source,
        "timestamp": int(time.time())
    }
    logger.emit(tag, data)

# HACKER NEWS
def fetch_hacker_news():
    seen_ids = set()
    base_url = "https://hacker-news.firebaseio.com/v0"
    print("[HN Fetcher] Avviato monitoraggio live stories e commenti...")

    while True:
        try:
            res_stories = requests.get(f"{base_url}/newstories.json", headers=HEADERS, timeout=10)
            res_updates = requests.get(f"{base_url}/updates.json", headers=HEADERS, timeout=10)

            item_ids = []
            if res_stories.status_code == 200:
                item_ids.extend(res_stories.json()[:30])
            if res_updates.status_code == 200:
                item_ids.extend(res_updates.json().get("items", [])[:30])

            for item_id in item_ids:
                if item_id in seen_ids:
                    continue
                seen_ids.add(item_id)
                if len(seen_ids) > 10000:
                    seen_ids.pop()

                item_res = requests.get(f"{base_url}/item/{item_id}.json", headers=HEADERS, timeout=5)
                if item_res.status_code == 200 and item_res.json():
                    item = item_res.json()
                    item_type = item.get("type", "")
                    author = item.get("by", "unknown")
                    item_url = item.get("url") or f"https://news.ycombinator.com/item?id={item_id}"

                    if item_type == "story":
                        text = item.get("title", "")
                        emit_event("hackernews", text, author, item_url, "hackernews")
                    elif item_type == "comment":
                        raw_comment = item.get("text", "")
                        clean_text = raw_comment.replace("<p>", " ").replace("</p>", "").replace("<i>", "").replace("</i>", "")
                        emit_event("hackernews", clean_text, author, item_url, "hackernews")

        except Exception as e:
            print(f"[HN Fetcher] Errore: {e}")

        time.sleep(10)

# REDDIT
def fetch_reddit():
    seen_urls = set()
    subreddits = [
        "programming", "technology", "devops", "datascience",
        "locallama", "rust", "golang", "machinelearning",
        "selfhosted", "artificial", "netsec", "webdev",
        "sysadmin", "cloudcomputing"
    ]
    chunks = [subreddits[i:i+5] for i in range(0, len(subreddits), 5)]
    print(f"[Reddit Fetcher] Avviato monitoraggio su {len(subreddits)} subreddit...")

    while True:
        for chunk in chunks:
            multi_sub = "+".join(chunk)
            rss_url = f"https://www.reddit.com/r/{multi_sub}/new/.rss"
            try:
                feed = feedparser.parse(rss_url, request_headers=HEADERS)
                for entry in feed.entries:
                    link = entry.get("link", "")
                    if link in seen_urls:
                        continue
                    seen_urls.add(link)
                    if len(seen_urls) > 5000:
                        seen_urls.pop()

                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    clean_summary = ""
                    if summary:
                        clean_summary = summary.split("<!-- SC_OFF -->")[-1].split("<!-- SC_ON -->")[0]
                        clean_summary = clean_summary.replace("<p>", " ").replace("</p>", "")[:500]

                    full_text = f"{title}. {clean_summary}" if clean_summary else title
                    author = entry.get("author", "unknown")
                    emit_event("reddit", full_text, author, link, "reddit")

            except Exception as e:
                print(f"[Reddit Fetcher] Errore r/{multi_sub}: {e}")

            time.sleep(3)
        time.sleep(15)

# GITHUB TRENDING & ARXIV RSS
def fetch_github_and_arxiv():
    seen_ids = set()
    print("[ArXiv/GitHub Fetcher] Avviato monitoraggio repository e paper...")

    arxiv_feeds = [
        "http://export.arxiv.org/rss/cs.AI",
        "http://export.arxiv.org/rss/cs.SE",
        "http://export.arxiv.org/rss/cs.CR"
    ]

    while True:
        # 1. ArXiv Papers
        for feed_url in arxiv_feeds:
            try:
                feed = feedparser.parse(feed_url)
                for entry in feed.entries:
                    link = entry.get("link", "")
                    if link in seen_ids:
                        continue
                    seen_ids.add(link)
                    text = f"{entry.get('title', '')}: {entry.get('summary', '')[:400]}"
                    author = entry.get("author", "arxiv")
                    emit_event("arxiv", text, author, link, "arxiv")
            except Exception as e:
                print(f"[ArXiv Fetcher] Errore: {e}")

        # 2. GitHub Repositories
        try:
            gh_url = "https://api.github.com/search/repositories?q=stars:>10+pushed:>2026-01-01&sort=updated&order=desc&per_page=25"
            res = requests.get(gh_url, headers=HEADERS, timeout=10)
            if res.status_code == 200:
                for repo in res.json().get("items", []):
                    r_id = repo.get("html_url")
                    if r_id in seen_ids:
                        continue
                    seen_ids.add(r_id)
                    text = f"GitHub Project {repo.get('name')}: {repo.get('description', '')}. Language: {repo.get('language', '')}"
                    emit_event("github", text, repo.get("owner", {}).get("login", "github"), r_id, "github")
        except Exception as e:
            print(f"[GitHub Fetcher] Errore: {e}")

        if len(seen_ids) > 5000:
            seen_ids.clear()

        time.sleep(60)

# BLUESKY 

TECH_KEYWORDS = [
    r"\bpython\b", r"\brust\b", r"\bgolang\b", r"\bjavascript\b", r"\btypescript\b",
    r"\bc\+\+\b", r"\bc#\b", r"\bdocker\b", r"\bkubernetes\b", r"\bk8s\b",
    r"\blinux\b", r"\bpostgresql\b", r"\bpostgres\b", r"\bclickhouse\b",
    r"\bkafka\b", r"\bredis\b", r"\bpytorch\b", r"\btensorflow\b",
    r"\bhuggingface\b", r"\bfastapi\b", r"\blangchain\b", r"\bollama\b",
    r"\bvllm\b", r"\bnextjs\b", r"\breact\b", r"\bvue\b", r"\bsvelte\b"
]
KEYWORD_PATTERN = re.compile("|".join(TECH_KEYWORDS), re.IGNORECASE)

def on_message(ws, message):
    try:
        data = json.loads(message)
        commit = data.get("commit", {})
        if commit.get("operation") == "create" and commit.get("collection") == "app.bsky.feed.post":
            record = commit.get("record", {})
            text = record.get("text", "")
            if not text:
                return

            if KEYWORD_PATTERN.search(text):
                author = data.get("did", "unknown")
                post_rkey = commit.get("rkey", "")
                url = f"https://bsky.app/profile/{author}/post/{post_rkey}"
                emit_event("bluesky", text, author, url, "bluesky")
    except Exception:
        pass

def on_error(ws, error):
    print(f"[Bluesky] Websocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("[Bluesky] Connessione chiusa. Riconnessione in 5s...")
    time.sleep(5)
    start_bluesky()

def start_bluesky():
    ws_url = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
    ws = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()


# ENTRYPOINT
if __name__ == "__main__":
    t_hn = threading.Thread(target=fetch_hacker_news, daemon=True)
    t_reddit = threading.Thread(target=fetch_reddit, daemon=True)
    t_gh_arxiv = threading.Thread(target=fetch_github_and_arxiv, daemon=True)
    t_bsky = threading.Thread(target=start_bluesky, daemon=True)

    t_hn.start()
    t_reddit.start()
    t_gh_arxiv.start()
    t_bsky.start()

    print("[Ingestion] Tutti i canali di ingestione attivi con filtri tecnici rigorosi.")
    while True:
        time.sleep(1)