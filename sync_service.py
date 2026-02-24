"""
sync_service.py - Orchestration layer between PinpointAPI and Database.

Caching strategy (same as PHP version):
  T, T-1, T-2  → Always refresh from Pinpointe API
  Older         → Serve from DB; if missing, fetch once then cache forever

Filtering:
  - Seed campaigns (name contains "seed"/"wseed"/"iaseed") → flagged is_seed=1
  - Campaigns with sends < MIN_SENDS → discarded entirely

Performance:
  - All domains are fetched concurrently via asyncio.gather
  - Within each domain, campaign summaries are fetched concurrently (see pinpoint_api.py)
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Any

import pytz

from config import DOMAINS, TIMEZONE, LIVE_DAYS, MIN_SENDS, LEADPIER_CACHE_MINUTES
from pinpoint_api import PinpointAPI
from leadpier_api import LeadpierAPI
from database import (
    upsert_domain,
    get_domain_by_code,
    upsert_campaign,
    update_campaign_stats,
    get_campaign_by_statid,
    get_campaign_count_by_date_range,
    get_campaigns_by_date_range,
    upsert_leadpier_sources,
    get_leadpier_sources_by_date,
    get_leadpier_last_sync,
)

logger = logging.getLogger("sync_service")


def _is_seed(name: str) -> bool:
    low = name.lower()
    return "seed" in low or "wseed" in low or "iaseed" in low


def _cutoff_date() -> str:
    tz = pytz.timezone(TIMEZONE)
    cutoff = datetime.now(tz) - timedelta(days=LIVE_DAYS + 1)
    return cutoff.strftime("%Y-%m-%d")


def _is_live(date_str: str) -> bool:
    return date_str > _cutoff_date()


def _group_campaigns(
    rows: list[dict], revenue_map: dict[str, dict] | None = None
) -> list[dict]:
    """Group flat campaign rows by domain, compute totals, and merge revenue."""
    domains: dict[str, dict] = {}

    for row in rows:
        code = row["domain_code"]
        if code not in domains:
            domains[code] = {
                "code": code,
                "name": row["domain_name"],
                "le_domain": row["le_domain"],
                "campaigns": [],
                "totals": {
                    "sends": 0,
                    "opens": 0,
                    "clicks": 0,
                    "bounces": 0,
                    "unsubs": 0,
                    "revenue": 0.0,
                    "conversions": 0,
                    "visitors": 0,
                    "total_leads": 0,
                },
            }

        sends = int(row.get("sends") or 0)
        clicks = int(row.get("clicks") or 0)
        campaign_name = row["campaign_name"]

        # Revenue from Leadpier match
        rev_data = (revenue_map or {}).get(campaign_name)
        revenue = rev_data["revenue"] if rev_data else 0.0
        conversions = rev_data["sold_leads"] if rev_data else 0
        visitors = rev_data["visitors"] if rev_data else 0
        total_leads = rev_data["leads"] if rev_data else 0
        epc = round(revenue / clicks, 2) if clicks > 0 and revenue > 0 else 0.0
        ecpm = round((revenue / sends) * 1000, 2) if sends > 0 and revenue > 0 else 0.0

        campaign = {
            "statid": row.get("statid", ""),
            "campaign_id": row["campaign_id"],
            "campaign_name": campaign_name,
            "date": row["date"],
            "time": row["time"],
            "sends": sends,
            "opens": int(row.get("opens") or 0),
            "open_percent": float(row.get("open_percent") or 0),
            "clicks": clicks,
            "click_percent": float(row.get("click_percent") or 0),
            "bounces": int(row.get("bounces") or 0),
            "bounce_percent": float(row.get("bounce_percent") or 0),
            "unsubs": int(row.get("unsubs") or 0),
            "is_seed": int(row.get("is_seed") or 0),
            "last_fetched_at": row.get("last_fetched_at"),
            # Revenue fields
            "revenue": revenue,
            "conversions": conversions,
            "visitors": visitors,
            "total_leads": total_leads,
            "epc": epc,
            "ecpm": ecpm,
        }

        domains[code]["campaigns"].append(campaign)
        for k in ("sends", "opens", "clicks", "bounces", "unsubs"):
            domains[code]["totals"][k] += campaign[k]
        domains[code]["totals"]["revenue"] += revenue
        domains[code]["totals"]["conversions"] += conversions
        domains[code]["totals"]["visitors"] += visitors
        domains[code]["totals"]["total_leads"] += total_leads

    for d in domains.values():
        s = d["totals"]["sends"]
        c = d["totals"]["clicks"]
        r = d["totals"]["revenue"]
        d["totals"]["open_percent"] = (
            round((d["totals"]["opens"] / s) * 100, 2) if s > 0 else 0
        )
        d["totals"]["click_percent"] = (
            round((d["totals"]["clicks"] / s) * 100, 2) if s > 0 else 0
        )
        d["totals"]["bounce_percent"] = (
            round((d["totals"]["bounces"] / s) * 100, 2) if s > 0 else 0
        )
        d["totals"]["epc"] = round(r / c, 2) if c > 0 and r > 0 else 0.0
        d["totals"]["ecpm"] = round((r / s) * 1000, 2) if s > 0 and r > 0 else 0.0

    return list(domains.values())


# ------------------------------------------------------------------
# Public helpers
# ------------------------------------------------------------------


async def sync_campaigns(
    start_date: str, end_date: str, force_fresh: bool = False
) -> dict[str, Any]:
    tz = pytz.timezone(TIMEZONE)
    cutoff = _cutoff_date()
    today = datetime.now(tz).strftime("%Y-%m-%d")

    needs_api = end_date > cutoff
    api_start = max(start_date, cutoff) if needs_api else None
    api_end = end_date if needs_api else None

    old_needs_fetch = False
    if start_date <= cutoff:
        if get_campaign_count_by_date_range(start_date, min(end_date, cutoff)) == 0:
            old_needs_fetch = True

    results: dict[str, Any] = {
        "success": True,
        "domains": [],
        "totalCampaigns": 0,
        "seedCampaigns": 0,
        "skippedLowSends": 0,
        "errors": [],
        "syncTime": datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S"),
    }

    if not needs_api and not old_needs_fetch:
        logger.info("All dates finalized and cached. No API calls needed.")
        return results

    fetch_start = start_date if old_needs_fetch else (api_start or start_date)
    fetch_end = end_date

    now = datetime.now(tz)
    # Use tz.localize() instead of .replace(tzinfo=tz) to avoid pytz offset bugs
    range_start = tz.localize(datetime.strptime(fetch_start, "%Y-%m-%d"))
    days_back = (now - range_start).days + 1
    # Ensure at least 3 days of lookback for the API call
    interval_count = max(min(days_back, 30), 3)

    logger.info(
        "Sync %s → %s  (API fetch %s → %s, interval=%d days)",
        start_date,
        end_date,
        fetch_start,
        fetch_end,
        interval_count,
    )

    api = PinpointAPI()
    t0 = time.perf_counter()

    # ---- Helper: process a single domain (runs concurrently) ----
    async def _process_domain(code: str, domain: dict) -> dict | None:
        try:
            db_domain = get_domain_by_code(code)
            if not db_domain:
                upsert_domain(code, domain)
                db_domain = get_domain_by_code(code)

            campaigns = await api.get_full_campaign_stats(
                domain, interval_count, "days"
            )
            logger.info(
                "Domain %s: %d campaigns from API", domain["name"], len(campaigns)
            )

            updated = 0
            skipped = 0
            seeds = 0
            low_sends = 0

            for c in campaigns:
                c_date = c["date"]
                if c_date < start_date or c_date > end_date:
                    skipped += 1
                    continue

                if c.get("sends", 0) < MIN_SENDS:
                    low_sends += 1
                    continue

                is_seed_flag = _is_seed(c["campaign_name"])
                if is_seed_flag:
                    seeds += 1
                c["is_seed"] = 1 if is_seed_flag else 0

                live = _is_live(c_date)

                try:
                    if live:
                        cid = upsert_campaign(db_domain["id"], c)
                        update_campaign_stats(cid, c)
                        updated += 1
                    else:
                        existing = get_campaign_by_statid(db_domain["id"], c["statid"])
                        if not existing:
                            cid = upsert_campaign(db_domain["id"], c)
                            update_campaign_stats(cid, c)
                            updated += 1
                        else:
                            skipped += 1
                except Exception as e:
                    logger.error(
                        "DB upsert error for %s/%s: %s",
                        domain["name"],
                        c.get("campaign_id", "?"),
                        e,
                    )
                    results["errors"].append(
                        {
                            "domain": domain["name"],
                            "campaign": c.get("campaign_id", "unknown"),
                            "error": str(e),
                        }
                    )

            logger.info(
                "Domain %s: updated=%d  skipped=%d  seeds=%d  lowSends=%d",
                domain["name"],
                updated,
                skipped,
                seeds,
                low_sends,
            )

            return {
                "name": domain["name"],
                "campaigns": updated,
                "skipped": skipped,
                "seeds": seeds,
                "lowSends": low_sends,
            }

        except Exception as e:
            logger.error("Domain %s ERROR: %s", domain["name"], e, exc_info=True)
            results["errors"].append({"domain": domain["name"], "error": str(e)})
            return None

    # ---- Launch all enabled domains concurrently ----
    enabled = {k: v for k, v in DOMAINS.items() if v.get("enabled")}
    logger.info("Syncing %d domains concurrently...", len(enabled))

    domain_results = await asyncio.gather(
        *[_process_domain(code, domain) for code, domain in enabled.items()]
    )

    for dr in domain_results:
        if dr:
            results["domains"].append(dr)
            results["totalCampaigns"] += dr["campaigns"]
            results["seedCampaigns"] += dr["seeds"]
            results["skippedLowSends"] += dr["lowSends"]

    elapsed = time.perf_counter() - t0
    logger.info("Sync complete in %.1fs (all domains concurrent)", elapsed)

    return results


async def sync_today() -> dict:
    today = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")
    return await sync_campaigns(today, today)


async def sync_live_days() -> dict:
    tz = pytz.timezone(TIMEZONE)
    today = datetime.now(tz).strftime("%Y-%m-%d")
    start = (datetime.now(tz) - timedelta(days=LIVE_DAYS)).strftime("%Y-%m-%d")
    return await sync_campaigns(start, today)


# ------------------------------------------------------------------
# Leadpier Revenue Sync
# ------------------------------------------------------------------
async def sync_revenue(report_date: str, force: bool = False) -> dict[str, Any]:
    """
    Fetch Leadpier revenue data for a given date, cache it, return summary.
    Skips API call if cached data is < LEADPIER_CACHE_MINUTES old.
    """
    # Check cache freshness
    if not force:
        last_sync = get_leadpier_last_sync(report_date)
        if last_sync:
            try:
                last_dt = datetime.fromisoformat(last_sync)
                age_min = (datetime.utcnow() - last_dt).total_seconds() / 60
                if age_min < LEADPIER_CACHE_MINUTES:
                    cached = get_leadpier_sources_by_date(report_date)
                    logger.info(
                        "Revenue cache fresh (%.0f min old), %d sources",
                        age_min,
                        len(cached),
                    )
                    return {
                        "success": True,
                        "cached": True,
                        "sources": len(cached),
                        "date": report_date,
                    }
            except Exception:
                pass

    lp = LeadpierAPI()
    try:
        sources = await lp.get_sources(report_date, report_date)
        count = upsert_leadpier_sources(report_date, sources)
        logger.info("Revenue sync: %d sources stored for %s", count, report_date)
        return {
            "success": True,
            "cached": False,
            "sources": count,
            "date": report_date,
        }
    except Exception as exc:
        logger.error("Revenue sync failed for %s: %s", report_date, exc)
        return {"success": False, "error": str(exc), "date": report_date}


def _get_revenue_map_for_dates(
    start_date: str, end_date: str, campaign_names: list[str]
) -> dict[str, dict]:
    """
    Load cached Leadpier sources for each date in range, then match
    against campaign names.  Returns {campaign_name: {revenue, ...}}.
    """
    # Collect all sources across dates
    all_sources: list[dict] = []
    d = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    while d <= end:
        day_str = d.strftime("%Y-%m-%d")
        all_sources.extend(get_leadpier_sources_by_date(day_str))
        d += timedelta(days=1)

    if not all_sources:
        return {}

    return LeadpierAPI.match_all_campaigns(all_sources, campaign_names)


# ------------------------------------------------------------------
# Public read helpers (with revenue)
# ------------------------------------------------------------------
def get_campaigns_grouped(
    start_date: str, end_date: str, seed_only: bool = False
) -> list[dict]:
    rows = get_campaigns_by_date_range(start_date, end_date, seed_only)
    # Gather campaign names and match against cached Leadpier data
    campaign_names = [r["campaign_name"] for r in rows]
    revenue_map = _get_revenue_map_for_dates(start_date, end_date, campaign_names)
    return _group_campaigns(rows, revenue_map)


def get_today_campaigns() -> list[dict]:
    today = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")
    return get_campaigns_grouped(today, today)


def get_today_seed_campaigns() -> list[dict]:
    today = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")
    return get_campaigns_grouped(today, today, seed_only=True)
