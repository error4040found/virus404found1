"""
pinpoint_api.py - Pinpointe XML API client (Python port)

Mirrors the PHP PinpointAPI class logic:
  STEP 1: GetNewslettersSent   → list campaigns in a time window
  STEP 2: GetNewsletterSummary → detailed stats per campaign (by statid)

Uses regex-based XML parsing (same approach as PHP version) because
Pinpointe responses sometimes have inconsistent nesting that breaks
standard XML parsers.

Performance: Uses asyncio.Semaphore to fetch up to MAX_CONCURRENT
campaign summaries in parallel (default 10), reducing total sync
time from ~5 min to ~30-40 sec.
"""

import asyncio
import re
import logging
import time
from datetime import datetime
from html import escape as html_escape
from typing import Any

import httpx
import pytz

from config import TIMEZONE

logger = logging.getLogger("pinpoint_api")

# Max concurrent GetNewsletterSummary requests per domain
MAX_CONCURRENT = 10


class PinpointAPIError(Exception):
    """Raised when the Pinpointe API returns an error."""

    pass


class PinpointAPI:
    """Async client for the Pinpointe XML API."""

    def __init__(self, timeout: int = 120):
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Build raw XML request string
    # ------------------------------------------------------------------
    def _build_xml(
        self,
        username: str,
        usertoken: str,
        request_type: str,
        request_method: str,
        details: dict[str, Any] | None = None,
    ) -> str:
        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            "<xmlrequest>",
            f"<username>{html_escape(username)}</username>",
            f"<usertoken>{html_escape(usertoken)}</usertoken>",
            f"<requesttype>{request_type}</requesttype>",
            f"<requestmethod>{request_method}</requestmethod>",
            "<details>",
        ]
        if details:
            for key, value in details.items():
                parts.append(f"<{key}>{html_escape(str(value))}</{key}>")
        parts.append("</details>")
        parts.append("</xmlrequest>")
        return "".join(parts)

    # ------------------------------------------------------------------
    # POST XML to Pinpointe, return raw response text
    # Accepts an optional shared httpx.AsyncClient to avoid creating
    # a new connection per request (much faster for batch calls).
    # ------------------------------------------------------------------
    async def _make_request(
        self, api_url: str, xml_body: str, client: httpx.AsyncClient | None = None
    ) -> str:
        body_bytes = xml_body.encode("utf-8")
        logger.debug("POST %s  (%d bytes)", api_url, len(body_bytes))

        async def _do_post(c: httpx.AsyncClient) -> httpx.Response:
            return await c.post(
                api_url,
                content=body_bytes,
                headers={"Content-Type": "application/xml"},
            )

        if client:
            response = await _do_post(client)
        else:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                verify=True,
                follow_redirects=True,
            ) as c:
                response = await _do_post(c)

        if response.status_code != 200:
            logger.error(
                "HTTP %d from Pinpointe: %s",
                response.status_code,
                response.text[:500],
            )
            raise PinpointAPIError(f"HTTP {response.status_code} from Pinpointe")

        logger.debug("Response length: %d chars", len(response.text))
        return response.text

    # ------------------------------------------------------------------
    # Check <status> in response
    # ------------------------------------------------------------------
    @staticmethod
    def _check_status(raw_xml: str, context: str = "") -> None:
        m = re.search(r"<status>(.*?)</status>", raw_xml, re.IGNORECASE)
        if m and m.group(1).strip().upper() == "FAILED":
            err_msg = "Unknown error"
            em = re.search(
                r"<errormessage>(.*?)</errormessage>", raw_xml, re.IGNORECASE
            )
            if em:
                err_msg = em.group(1)
            raise PinpointAPIError(f"[{context}] {err_msg}")

    # ------------------------------------------------------------------
    # Extract a single XML field using regex
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_field(xml: str, field: str, default: str = "") -> str:
        m = re.search(rf"<{field}>(.*?)</{field}>", xml, re.IGNORECASE | re.DOTALL)
        return m.group(1).strip() if m else default

    # ==================================================================
    # STEP 1 — GetNewslettersSent
    # ==================================================================
    async def get_campaigns_sent(
        self,
        domain: dict,
        interval_count: int = 30,
        interval_units: str = "days",
    ) -> list[dict]:
        logger.info(
            "[%s] GetNewslettersSent  interval=%d %s",
            domain["name"],
            interval_count,
            interval_units,
        )
        xml_body = self._build_xml(
            domain["username"],
            domain["usertoken"],
            "Newsletters",
            "GetNewslettersSent",
            {
                "intervalcount": interval_count,
                "intervalunits": interval_units,
            },
        )

        raw = await self._make_request(domain["api_url"], xml_body)
        self._check_status(raw, f"GetNewslettersSent/{domain['name']}")

        campaigns: list[dict] = []
        items = re.findall(r"<item>(.*?)</item>", raw, re.DOTALL)
        logger.info("[%s] Found %d <item> blocks", domain["name"], len(items))

        for item_xml in items:
            campaigns.append(
                {
                    "newsletterid": self._extract_field(item_xml, "newsletterid"),
                    "name": self._extract_field(item_xml, "name", "Unnamed"),
                    "subject": self._extract_field(item_xml, "subject"),
                    "statid": self._extract_field(item_xml, "statid"),
                    "starttime": self._extract_field(item_xml, "starttime"),
                    "finishtime": self._extract_field(item_xml, "finishtime"),
                    "sentto": int(self._extract_field(item_xml, "sentto", "0") or "0"),
                }
            )
        return campaigns

    # ==================================================================
    # STEP 2 — GetNewsletterSummary
    # ==================================================================
    async def get_campaign_summary(
        self, domain: dict, statid: str, client: httpx.AsyncClient | None = None
    ) -> dict:
        xml_body = self._build_xml(
            domain["username"],
            domain["usertoken"],
            "Newsletters",
            "GetNewsletterSummary",
            {"statid": statid, "summaryonly": "1", "resultlimit": "0"},
        )

        raw = await self._make_request(domain["api_url"], xml_body, client)
        self._check_status(raw, f"GetNewsletterSummary/{domain['name']}/{statid}")

        sends = int(self._extract_field(raw, "sendsize", "0") or "0")
        opens_unique = int(self._extract_field(raw, "emailopens_unique", "0") or "0")
        opens_total = int(self._extract_field(raw, "emailopens", "0") or "0")
        clicks = int(self._extract_field(raw, "linkclicks", "0") or "0")
        bounce_soft = int(self._extract_field(raw, "bouncecount_soft", "0") or "0")
        bounce_hard = int(self._extract_field(raw, "bouncecount_hard", "0") or "0")
        bounce_unknown = int(
            self._extract_field(raw, "bouncecount_unknown", "0") or "0"
        )
        bounces = bounce_soft + bounce_hard + bounce_unknown
        unsubs = int(self._extract_field(raw, "unsubscribecount", "0") or "0")
        newsletter_name = self._extract_field(raw, "newslettername", "")

        opens = opens_unique if opens_unique > 0 else opens_total
        open_pct = round((opens / sends) * 100, 2) if sends > 0 else 0.0
        click_pct = round((clicks / sends) * 100, 2) if sends > 0 else 0.0
        bounce_pct = round((bounces / sends) * 100, 2) if sends > 0 else 0.0

        return {
            "statid": str(statid),
            "newslettername": newsletter_name,
            "sends": sends,
            "opens": opens,
            "open_percent": open_pct,
            "clicks": clicks,
            "click_percent": click_pct,
            "bounces": bounces,
            "bounce_percent": bounce_pct,
            "unsubs": unsubs,
        }

    # ==================================================================
    # COMBINED — Step 1 + Step 2  (CONCURRENT summaries)
    # ==================================================================
    async def get_full_campaign_stats(
        self,
        domain: dict,
        interval_count: int = 30,
        interval_units: str = "days",
    ) -> list[dict]:
        t0 = time.perf_counter()

        campaigns_list = await self.get_campaigns_sent(
            domain, interval_count, interval_units
        )

        tz = pytz.timezone(TIMEZONE)
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        # Filter out campaigns without statid upfront
        valid_campaigns = [c for c in campaigns_list if c.get("statid")]
        logger.info(
            "[%s] Fetching summaries for %d campaigns (%d concurrent)...",
            domain["name"],
            len(valid_campaigns),
            MAX_CONCURRENT,
        )

        async def _fetch_one(c: dict, client: httpx.AsyncClient) -> dict | None:
            async with semaphore:
                try:
                    summary = await self.get_campaign_summary(
                        domain, c["statid"], client
                    )
                    campaign_date, campaign_time = self._parse_starttime(
                        c.get("starttime", ""), tz
                    )
                    return {
                        "campaign_id": c["newsletterid"],
                        "statid": c["statid"],
                        "campaign_name": summary["newslettername"] or c["name"],
                        "date": campaign_date,
                        "time": campaign_time,
                        "sends": summary["sends"],
                        "opens": summary["opens"],
                        "open_percent": summary["open_percent"],
                        "clicks": summary["clicks"],
                        "click_percent": summary["click_percent"],
                        "bounces": summary["bounces"],
                        "bounce_percent": summary["bounce_percent"],
                        "unsubs": summary["unsubs"],
                    }
                except Exception as exc:
                    logger.warning(
                        "[%s] Summary failed for statid %s: %s",
                        domain["name"],
                        c["statid"],
                        exc,
                    )
                    return None

        # Use a single shared httpx client for all concurrent requests
        async with httpx.AsyncClient(
            timeout=self.timeout,
            verify=True,
            follow_redirects=True,
        ) as client:
            raw_results = await asyncio.gather(
                *[_fetch_one(c, client) for c in valid_campaigns]
            )

        results = [r for r in raw_results if r is not None]

        elapsed = time.perf_counter() - t0
        logger.info(
            "[%s] Full stats: %d campaigns with data  (%.1fs — was ~%ds sequential)",
            domain["name"],
            len(results),
            elapsed,
            len(valid_campaigns),
        )
        return results

    # ------------------------------------------------------------------
    # Parse Pinpointe starttime string → (date, time) in local tz
    # Handles ISO 8601, Unix timestamps, and various date formats
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_starttime(raw: str, tz) -> tuple[str, str]:
        if not raw:
            now = datetime.now(tz)
            return now.strftime("%Y-%m-%d"), "00:00:00"

        # Try ISO 8601 first (e.g. "2026-02-03T12:40:02.000Z")
        try:
            cleaned = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(cleaned)
            if dt.tzinfo is None:
                dt = pytz.UTC.localize(dt)
            dt_local = dt.astimezone(tz)
            return dt_local.strftime("%Y-%m-%d"), dt_local.strftime("%H:%M:%S")
        except (ValueError, TypeError):
            pass

        # Try Unix timestamp
        try:
            ts = int(raw)
            dt = datetime.fromtimestamp(ts, tz=pytz.UTC).astimezone(tz)
            return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")
        except (ValueError, TypeError, OSError):
            pass

        # Fallback
        now = datetime.now(tz)
        logger.warning("Could not parse starttime '%s', using today", raw)
        return now.strftime("%Y-%m-%d"), "00:00:00"
