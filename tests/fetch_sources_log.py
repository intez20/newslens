"""Fetch articles from all 4 sources and log to file.

Run this to verify all clients still work and see sample data.
DELETE this file once stage-02c testing is complete.

Usage:
    python tests/fetch_sources_log.py
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from producer.guardian_client import GuardianClient
from producer.rss_client import RSSClient
from producer.hackernews_client import HackerNewsClient
from producer.bluesky_client import BlueskyClient

LOG_FILE = Path(__file__).parent / "fetch_sources_output.log"


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    clients = [
        ("Guardian", GuardianClient(sections=["technology"], page_size=3)),
        ("RSS", RSSClient()),
        ("HackerNews", HackerNewsClient(limit=5)),
        ("Bluesky", BlueskyClient()),
    ]

    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"=== NewsLens Source Fetch Log — {datetime.now(timezone.utc).isoformat()} ===\n\n")

        total = 0
        for name, client in clients:
            f.write(f"{'='*60}\n")
            f.write(f"SOURCE: {name} (client.source_name = {client.source_name})\n")
            f.write(f"{'='*60}\n\n")

            try:
                articles = client.fetch()
                f.write(f"Fetched {len(articles)} articles\n\n")

                for i, art in enumerate(articles[:5], 1):  # log max 5 per source
                    f.write(f"  [{i}] article_id: {art.article_id}\n")
                    f.write(f"      headline:   {art.headline[:80]}{'...' if len(art.headline) > 80 else ''}\n")
                    f.write(f"      section:    {art.section}\n")
                    f.write(f"      published:  {art.published_at.isoformat()}\n")
                    f.write(f"      source_url: {art.source_url}\n")
                    f.write(f"      body_len:   {len(art.body)} chars, {len(art.body.split())} words\n")
                    f.write("\n")

                if len(articles) > 5:
                    f.write(f"  ... and {len(articles) - 5} more articles\n\n")

                total += len(articles)
            except Exception as e:
                f.write(f"  ERROR: {type(e).__name__}: {e}\n\n")

        f.write(f"{'='*60}\n")
        f.write(f"TOTAL: {total} articles fetched across all sources\n")
        f.write(f"{'='*60}\n")

    print(f"Log written to {LOG_FILE}")
    print(f"Total articles: {total}")


if __name__ == "__main__":
    main()
