"""
main.py - FastAPI application for Pinpointe Email Campaign Dashboard

Endpoints:
  GET  /login               → Login page
  POST /login               → Authenticate
  GET  /logout              → Logout
  GET  /                    → Dashboard UI (auth required)
  GET  /api/today           → Today's campaigns (excl. seeds) + revenue
  GET  /api/range           → Campaigns for date range + revenue
  GET  /api/seeds/today     → Today's seed campaigns + revenue
  GET  /api/seeds/range     → Seed campaigns for date range + revenue
  POST /api/sync/today      → Sync today from Pinpointe + Leadpier
  POST /api/sync/range      → Sync date range
  POST /api/sync/live       → Sync live days (T, T-1, T-2)
  POST /api/sync/revenue    → Sync Leadpier revenue only
  GET  /api/domains         → List configured domains
  GET  /api/health          → Health check
"""

import os
import logging
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path

import pytz
from fastapi import FastAPI, Query, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from config import TIMEZONE, DOMAINS, USERS, SESSION_SECRET_KEY

BASE_DIR = Path(__file__).resolve().parent
from database import (
    init_schema,
    upsert_domain,
    get_all_domains,
    get_all_domains_admin,
    get_domain_by_id,
    create_domain,
    update_domain,
    delete_domain,
    cleanup_old_data,
)
from sync_service import (
    sync_today,
    sync_live_days,
    sync_campaigns,
    sync_revenue,
    get_today_campaigns,
    get_campaigns_grouped,
    get_today_seed_campaigns,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("main")


# ── Scheduled cleanup job ─────────────────────────────────────────
def scheduled_cleanup():
    """Run daily cleanup of data older than 30 days."""
    logger.info("Scheduler: starting daily data cleanup...")
    try:
        result = cleanup_old_data(days=30)
        logger.info(
            "Scheduler: cleanup complete — %d campaigns, %d stats, %d leadpier sources removed (cutoff: %s)",
            result["campaigns"],
            result["campaign_stats"],
            result["leadpier_sources"],
            result["cutoff_date"],
        )
    except Exception as e:
        logger.error("Scheduler: cleanup failed — %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: initialize DB, seed domains, start scheduler."""
    logger.info("Initializing database...")
    init_schema()
    for code, domain in DOMAINS.items():
        upsert_domain(code, domain)
    logger.info("Database ready. %d domains loaded.", len(DOMAINS))

    # Start scheduler — runs cleanup daily at 2:00 AM EST
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(
        scheduled_cleanup,
        trigger=CronTrigger(hour=2, minute=0),
        id="daily_cleanup",
        name="Remove data older than 30 days",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started — daily cleanup at 2:00 AM %s", TIMEZONE)

    # Also run cleanup once on startup
    scheduled_cleanup()

    yield

    # Shutdown
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")


app = FastAPI(
    title="Pinpointe Campaign Dashboard",
    version="1.0.0",
    lifespan=lifespan,
)

# Static files & templates
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# ── Public paths that don't require login ─────────────────────────
PUBLIC_PATHS = {"/login", "/api/health"}
PUBLIC_PREFIXES = ("/static/",)
# Paths that require super role
SUPER_PATHS = {"/domains"}
SUPER_API_PREFIX = "/api/admin/"


class AuthGuardMiddleware(BaseHTTPMiddleware):
    """Redirect unauthenticated users to login page. Enforce role for super paths."""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        # Allow public paths and static files through
        if path in PUBLIC_PATHS or any(path.startswith(p) for p in PUBLIC_PREFIXES):
            return await call_next(request)
        # Check session
        if not request.session.get("authenticated"):
            if path.startswith("/api/"):
                return JSONResponse(
                    status_code=401,
                    content={"success": False, "error": "Not authenticated"},
                )
            return RedirectResponse(url="/login", status_code=302)
        # Check super role for admin paths
        role = request.session.get("role", "user")
        if path in SUPER_PATHS or path.startswith(SUPER_API_PREFIX):
            if role != "super":
                if path.startswith("/api/"):
                    return JSONResponse(
                        status_code=403,
                        content={
                            "success": False,
                            "error": "Super admin access required",
                        },
                    )
                return RedirectResponse(url="/", status_code=302)
        return await call_next(request)


# Order matters: SessionMiddleware wraps the app first,
# then AuthGuardMiddleware runs inside it (has access to request.session)
app.add_middleware(AuthGuardMiddleware)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET_KEY)


# ──────────────────────────────────────────────────────────────────────
# Authentication
# ──────────────────────────────────────────────────────────────────────
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    # If already logged in, go to dashboard
    if request.session.get("authenticated"):
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request, username: str = Form(...), password: str = Form(...)
):
    user = USERS.get(username)
    if user and user["password"] == password:
        request.session["authenticated"] = True
        request.session["username"] = username
        request.session["role"] = user["role"]
        logger.info("User '%s' (%s) logged in", username, user["role"])
        return RedirectResponse(url="/", status_code=302)
    logger.warning("Failed login attempt for user '%s'", username)
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": "Invalid username or password"},
        status_code=401,
    )


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=302)


# ──────────────────────────────────────────────────────────────────────
# Dashboard pages
# ──────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "role": request.session.get("role", "user"),
            "username": request.session.get("username", ""),
        },
    )


@app.get("/domains", response_class=HTMLResponse)
async def domains_page(request: Request):
    """Domain management dashboard (super users only — enforced by middleware)."""
    return templates.TemplateResponse(
        "domains.html",
        {
            "request": request,
            "role": request.session.get("role", "user"),
            "username": request.session.get("username", ""),
        },
    )


# ──────────────────────────────────────────────────────────────────────
# API: Read endpoints (from database)
# ──────────────────────────────────────────────────────────────────────
@app.get("/api/today")
async def api_today():
    domains = get_today_campaigns()
    return {
        "success": True,
        "date": datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d"),
        "timezone": TIMEZONE,
        "domains": domains,
    }


@app.get("/api/range")
async def api_range(
    startDate: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    endDate: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
):
    domains = get_campaigns_grouped(startDate, endDate)
    return {
        "success": True,
        "startDate": startDate,
        "endDate": endDate,
        "timezone": TIMEZONE,
        "domains": domains,
    }


@app.get("/api/seeds/today")
async def api_seeds_today():
    domains = get_today_seed_campaigns()
    return {
        "success": True,
        "date": datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d"),
        "timezone": TIMEZONE,
        "domains": domains,
    }


@app.get("/api/seeds/range")
async def api_seeds_range(
    startDate: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    endDate: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
):
    domains = get_campaigns_grouped(startDate, endDate, seed_only=True)
    return {
        "success": True,
        "startDate": startDate,
        "endDate": endDate,
        "timezone": TIMEZONE,
        "domains": domains,
    }


# ──────────────────────────────────────────────────────────────────────
# API: Sync endpoints (hit Pinpointe API → write to DB)
# ──────────────────────────────────────────────────────────────────────
@app.post("/api/sync/today")
async def api_sync_today():
    try:
        result = await sync_today()
        # Also sync Leadpier revenue for today
        today = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")
        rev = await sync_revenue(today, force=True)
        result["revenue_sync"] = rev
        return result
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)},
        )


@app.post("/api/sync/range")
async def api_sync_range(
    startDate: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    endDate: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
):
    try:
        result = await sync_campaigns(startDate, endDate)
        # Sync revenue for each date in range
        from datetime import timedelta as td

        rev_results = []
        d = datetime.strptime(startDate, "%Y-%m-%d")
        end = datetime.strptime(endDate, "%Y-%m-%d")
        while d <= end:
            day_str = d.strftime("%Y-%m-%d")
            rev = await sync_revenue(day_str, force=True)
            rev_results.append(rev)
            d += td(days=1)
        result["revenue_sync"] = rev_results
        return result
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)},
        )


@app.post("/api/sync/live")
async def api_sync_live():
    try:
        result = await sync_live_days()
        # Sync revenue for live days
        from datetime import timedelta as td
        from config import LIVE_DAYS

        tz = pytz.timezone(TIMEZONE)
        rev_results = []
        for i in range(LIVE_DAYS + 1):
            day_str = (datetime.now(tz) - td(days=i)).strftime("%Y-%m-%d")
            rev = await sync_revenue(day_str, force=True)
            rev_results.append(rev)
        result["revenue_sync"] = rev_results
        return result
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)},
        )


@app.post("/api/sync/revenue")
async def api_sync_revenue(
    date: str = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
):
    """Sync Leadpier revenue data only (no Pinpointe sync)."""
    try:
        if not date:
            date = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")
        result = await sync_revenue(date, force=True)
        return result
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)},
        )


# ──────────────────────────────────────────────────────────────────────
# API: Utility endpoints
# ──────────────────────────────────────────────────────────────────────
@app.get("/api/domains")
async def api_domains():
    domains = get_all_domains()
    return {"success": True, "domains": domains}


@app.get("/api/health")
async def api_health():
    return {
        "success": True,
        "status": "running",
        "timestamp": datetime.now(pytz.timezone(TIMEZONE)).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "timezone": TIMEZONE,
    }


# ──────────────────────────────────────────────────────────────────────
# API: Domain management (super admin only)
# ──────────────────────────────────────────────────────────────────────
@app.get("/api/admin/domains")
async def api_admin_domains(
    search: str = Query(""),
    page: int = Query(1, ge=1),
):
    """Paginated domain list with search."""
    result = get_all_domains_admin(search=search, page=page, per_page=15)
    return {"success": True, **result}


@app.get("/api/admin/domains/{domain_id}")
async def api_admin_domain_detail(domain_id: int):
    """Get a single domain."""
    domain = get_domain_by_id(domain_id)
    if not domain:
        return JSONResponse(
            status_code=404, content={"success": False, "error": "Domain not found"}
        )
    return {"success": True, "domain": domain}


@app.post("/api/admin/domains")
async def api_admin_domain_create(request: Request):
    """Create a new domain."""
    try:
        data = await request.json()
        required = ["code", "name", "api_url", "username", "usertoken", "le_domain"]
        missing = [f for f in required if not data.get(f)]
        if missing:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": f"Missing fields: {', '.join(missing)}",
                },
            )
        domain = create_domain(data)
        return {"success": True, "domain": domain}
    except ValueError as e:
        return JSONResponse(
            status_code=400, content={"success": False, "error": str(e)}
        )
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"success": False, "error": str(e)}
        )


@app.put("/api/admin/domains/{domain_id}")
async def api_admin_domain_update(domain_id: int, request: Request):
    """Update an existing domain."""
    try:
        data = await request.json()
        domain = update_domain(domain_id, data)
        if not domain:
            return JSONResponse(
                status_code=404, content={"success": False, "error": "Domain not found"}
            )
        return {"success": True, "domain": domain}
    except ValueError as e:
        return JSONResponse(
            status_code=400, content={"success": False, "error": str(e)}
        )
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"success": False, "error": str(e)}
        )


@app.delete("/api/admin/domains/{domain_id}")
async def api_admin_domain_delete(domain_id: int):
    """Delete a domain and all its campaigns."""
    try:
        deleted = delete_domain(domain_id)
        if not deleted:
            return JSONResponse(
                status_code=404, content={"success": False, "error": "Domain not found"}
            )
        return {"success": True, "message": "Domain deleted"}
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"success": False, "error": str(e)}
        )


@app.get("/api/me")
async def api_me(request: Request):
    """Return current user info."""
    return {
        "success": True,
        "username": request.session.get("username", ""),
        "role": request.session.get("role", "user"),
    }


@app.post("/api/cleanup")
async def api_cleanup():
    """Manually trigger cleanup of data older than 30 days."""
    try:
        result = cleanup_old_data(days=30)
        return {"success": True, **result}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e)},
        )


@app.get("/api/debug/test-pinpointe")
async def api_debug_test():
    """Quick diagnostic: call GetNewslettersSent for first domain and report."""
    from pinpoint_api import PinpointAPI

    first_code = next(iter(DOMAINS))
    domain = DOMAINS[first_code]
    api = PinpointAPI()
    try:
        campaigns = await api.get_campaigns_sent(domain, 3, "days")
        return {
            "success": True,
            "domain": domain["name"],
            "campaigns_found": len(campaigns),
            "sample": campaigns[:3] if campaigns else [],
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "domain": domain["name"],
                "error": str(e),
            },
        )


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8001))
    uvicorn.run(app, host="0.0.0.0", port=port)
