"""
config.py - Configuration for Pinpointe Email Campaign Dashboard
"""

import os

# Timezone
TIMEZONE = "America/New_York"

# Database
DB_PATH = os.path.join(os.path.dirname(__file__), "database", "campaigns.db")

# Minimum sends threshold - campaigns below this are ignored
MIN_SENDS = 50

# Number of "live" days (today + N days back) that always refresh from API
LIVE_DAYS = 2

# ─── Dashboard Authentication ─────────────────────────────────────
# Users: role "super" can manage domains, role "user" can only view dashboard
USERS = {
    "admin": {"password": "Shakta@123", "role": "super"},
    "manager": {"password": "Manager@123", "role": "super"},
    "viewer1": {"password": "Viewer@123", "role": "user"},
    "viewer2": {"password": "Viewer@456", "role": "user"},
}
SESSION_SECRET_KEY = "pnpt-dash-v2-s3cr3t-k3y-2026-zR7nP8wM"  # Change in production

# ─── Leadpier Revenue API ─────────────────────────────────────────
LEADPIER_AUTH_URL = "https://webapi.leadpier.com/v1/frontend/authenticate"
LEADPIER_DATA_URL = "https://webapi.leadpier.com/v1/api/stats/user/org/sources"
LEADPIER_USERNAME = "info@insightbridgeusa.com"
LEADPIER_PASSWORD = "Shakta123!"
LEADPIER_TOKEN_FILE = os.path.join(
    os.path.dirname(__file__), "database", "leadpier_token.json"
)
LEADPIER_TOKEN_EXPIRY_HOURS = 2
LEADPIER_CACHE_MINUTES = 30  # Don't re-fetch revenue if synced < 30 min ago

# Pinpointe API Domains Configuration
DOMAINS = {
    "P3_CFL": {
        "name": "CapitalFastLane",
        "api_url": "https://ib5131.pptsend.com/apixml.php",
        "username": "CapitalFastLane",
        "usertoken": "37f3b27c0c77a9fa711ef41403e8838fd49616d6",
        "le_domain": "capitalfastlane.com",
        "phase": 3,
        "enabled": True,
    },
    "P3_CAP": {
        "name": "CashAccessPro",
        "api_url": "https://ib5131.pptsend.com/apixml.php",
        "username": "CashAccessPro",
        "usertoken": "43bc2a163d0eb8adcf32dee81fa94c857fe965ce",
        "le_domain": "cashaccesspro.com",
        "phase": 3,
        "enabled": True,
    },
    "P3_EFP": {
        "name": "EasyFundPath",
        "api_url": "https://ib5131.pptsend.com/apixml.php",
        "username": "EasyFundPath",
        "usertoken": "f1327adfc1c28e6bd95f717edc2dd4bfd6a321c7",
        "le_domain": "easyfundpath.com",
        "phase": 3,
        "enabled": True,
    },
    "P3_FEP": {
        "name": "FundEdgePro",
        "api_url": "https://ib5131.pptsend.com/apixml.php",
        "username": "FundEdgePro",
        "usertoken": "101ae20bec205af50215db7a09a7833f6fd01a7e",
        "le_domain": "fundedgepro.com",
        "phase": 3,
        "enabled": True,
    },
    "P3_RFB": {
        "name": "RapidFundBridge",
        "api_url": "https://ib5131.pptsend.com/apixml.php",
        "username": "RapidFundBridge",
        "usertoken": "5d9fb077f532b7386730542b27beeb09ded46169",
        "le_domain": "rapidfundbridge.com",
        "phase": 3,
        "enabled": True,
    },
}
