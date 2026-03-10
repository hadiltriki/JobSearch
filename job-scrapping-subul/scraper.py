"""
scraper.py — Scraper facade
============================
Central import point for all job scrapers.
Import the functions you need in scraping_pipeline.py.

Available scrapers:
    scrape_aijobs(query, session)     → list[dict]   aijobs.ai
    scrape_remoteok(query, session)   → list[dict]   remoteok.com
    scrape_emploitic(query, session)  → list[dict]   emploitic.com
    scrape_tanitjobs(query, session)  → list[dict]   tanitjobs.com
    scrape_greenhouse(query, session) → list[dict]   greenhouse.io
    scrape_eluta(query, session)      → list[dict]   eluta.ca (Canada)
    scrape_whatjobs(query, session)   → list[dict]   uk.whatjobs.com (UK)

Internal helpers (used by scraping_pipeline.py):
    _scrape_emploitic_fetch_one(url, session) → dict | None
"""

from scraper_aijobs       import scrape_aijobs
from scraper_emploitic    import scrape_emploitic, _scrape_emploitic_fetch_one
from scraper_remoteok     import scrape_remoteok
from scraper_tanitjobs    import scrape_tanitjobs
from scraper_greenhouse   import scrape_greenhouse
from scraper_eluta        import scrape_eluta
from scraper_whatjobs     import scrape_whatjobs
from scraper_indeed       import scrape_indeed
from scraper_linkedin     import scrape_linkedin
from scraper_lever        import scrape_lever
from scraper_wttj         import scrape_wttj

__all__ = [
    "scrape_aijobs",
    "scrape_emploitic",
    "_scrape_emploitic_fetch_one",
    "scrape_remoteok",
    "scrape_tanitjobs",
    "scrape_greenhouse",
    "scrape_eluta",
    "scrape_whatjobs",
    "scrape_indeed",
    "scrape_linkedin",
    "scrape_lever",
    "scrape_wttj",
]