"""
database.py - SQLAlchemy ORM database layer for campaign data.

Models:
  Domain          — Pinpointe account credentials
  Campaign        — Individual email campaigns (keyed by domain_id + statid)
  CampaignStat    — Latest performance stats for each campaign
  LeadpierSource  — Cached Leadpier revenue data per source per date
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
    UniqueConstraint,
    Index,
    event,
)
from sqlalchemy.orm import (
    declarative_base,
    relationship,
    sessionmaker,
    Session,
)

from config import DB_PATH

logger = logging.getLogger("database")

# ---------------------------------------------------------------------------
# Engine & Session factory
# ---------------------------------------------------------------------------
_db_dir = os.path.dirname(DB_PATH)
if _db_dir and not os.path.isdir(_db_dir):
    os.makedirs(_db_dir, exist_ok=True)

DATABASE_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False},
)


@event.listens_for(engine, "connect")
def _set_sqlite_pragma(dbapi_conn, connection_record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


# ---------------------------------------------------------------------------
# ORM Models
# ---------------------------------------------------------------------------
class Domain(Base):
    __tablename__ = "domains"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    api_url = Column(String, nullable=False)
    username = Column(String, nullable=False)
    usertoken = Column(String, nullable=False)
    le_domain = Column(String, nullable=False)
    phase = Column(Integer, nullable=False)
    enabled = Column(Integer, default=1)
    created_at = Column(String, default=lambda: datetime.utcnow().isoformat())
    updated_at = Column(
        String,
        default=lambda: datetime.utcnow().isoformat(),
        onupdate=lambda: datetime.utcnow().isoformat(),
    )

    campaigns = relationship(
        "Campaign", back_populates="domain", cascade="all, delete-orphan"
    )


class Campaign(Base):
    __tablename__ = "campaigns"
    __table_args__ = (
        UniqueConstraint("domain_id", "statid", name="uq_domain_statid"),
        Index("idx_campaigns_domain", "domain_id"),
        Index("idx_campaigns_date", "date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    domain_id = Column(Integer, ForeignKey("domains.id"), nullable=False)
    statid = Column(String, nullable=False)
    campaign_id = Column(String, nullable=False)
    campaign_name = Column(String, nullable=False)
    date = Column(String, nullable=False)
    time = Column(String, nullable=False)
    is_seed = Column(Integer, default=0)
    created_at = Column(String, default=lambda: datetime.utcnow().isoformat())
    updated_at = Column(String, default=lambda: datetime.utcnow().isoformat())

    domain = relationship("Domain", back_populates="campaigns")
    stats = relationship(
        "CampaignStat",
        back_populates="campaign",
        uselist=False,
        cascade="all, delete-orphan",
    )


class CampaignStat(Base):
    __tablename__ = "campaign_stats"
    __table_args__ = (Index("idx_stats_campaign", "campaign_id"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    campaign_id = Column(
        Integer, ForeignKey("campaigns.id"), unique=True, nullable=False
    )
    sends = Column(Integer, default=0)
    opens = Column(Integer, default=0)
    open_percent = Column(Float, default=0.0)
    clicks = Column(Integer, default=0)
    click_percent = Column(Float, default=0.0)
    bounces = Column(Integer, default=0)
    bounce_percent = Column(Float, default=0.0)
    unsubs = Column(Integer, default=0)
    last_fetched_at = Column(String, default=lambda: datetime.utcnow().isoformat())

    campaign = relationship("Campaign", back_populates="stats")


class LeadpierSource(Base):
    """Cached Leadpier revenue data — one row per source per date."""

    __tablename__ = "leadpier_sources"
    __table_args__ = (
        UniqueConstraint("source_name", "report_date", name="uq_source_date"),
        Index("idx_lp_report_date", "report_date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String, nullable=False)
    report_date = Column(String, nullable=False)  # YYYY-MM-DD
    visitors = Column(Integer, default=0)
    total_leads = Column(Integer, default=0)
    sold_leads = Column(Integer, default=0)
    total_revenue = Column(Float, default=0.0)
    epl = Column(Float, default=0.0)  # Earnings per lead
    epv = Column(Float, default=0.0)  # Earnings per visitor
    fetched_at = Column(String, default=lambda: datetime.utcnow().isoformat())


# ---------------------------------------------------------------------------
# Schema init
# ---------------------------------------------------------------------------
def init_schema() -> None:
    """Create all tables if they don't exist."""
    Base.metadata.create_all(bind=engine)
    logger.info("Database schema initialized at %s", DB_PATH)


def get_session() -> Session:
    """Return a new database session."""
    return SessionLocal()


# ---------------------------------------------------------------------------
# Domain CRUD
# ---------------------------------------------------------------------------
def upsert_domain(code: str, data: dict) -> None:
    session = get_session()
    try:
        domain = session.query(Domain).filter(Domain.code == code).first()
        if domain:
            domain.name = data["name"]
            domain.api_url = data["api_url"]
            domain.username = data["username"]
            domain.usertoken = data["usertoken"]
            domain.le_domain = data["le_domain"]
            domain.phase = data["phase"]
            domain.enabled = 1 if data.get("enabled", True) else 0
            domain.updated_at = datetime.utcnow().isoformat()
        else:
            domain = Domain(
                code=code,
                name=data["name"],
                api_url=data["api_url"],
                username=data["username"],
                usertoken=data["usertoken"],
                le_domain=data["le_domain"],
                phase=data["phase"],
                enabled=1 if data.get("enabled", True) else 0,
            )
            session.add(domain)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_domain_by_code(code: str) -> Optional[dict]:
    session = get_session()
    try:
        d = session.query(Domain).filter(Domain.code == code).first()
        if not d:
            return None
        return {
            "id": d.id,
            "code": d.code,
            "name": d.name,
            "api_url": d.api_url,
            "username": d.username,
            "usertoken": d.usertoken,
            "le_domain": d.le_domain,
            "phase": d.phase,
            "enabled": d.enabled,
        }
    finally:
        session.close()


def get_all_domains() -> list[dict]:
    session = get_session()
    try:
        rows = (
            session.query(Domain)
            .filter(Domain.enabled == 1)
            .order_by(Domain.name)
            .all()
        )
        return [
            {
                "id": d.id,
                "code": d.code,
                "name": d.name,
                "api_url": d.api_url,
                "username": d.username,
                "usertoken": d.usertoken,
                "le_domain": d.le_domain,
                "phase": d.phase,
                "enabled": d.enabled,
            }
            for d in rows
        ]
    finally:
        session.close()


def get_all_domains_admin(
    search: str = "",
    page: int = 1,
    per_page: int = 15,
    include_disabled: bool = True,
) -> dict:
    """
    Return paginated domain list for admin dashboard.
    Supports search by name, code, le_domain, username.
    """
    session = get_session()
    try:
        query = session.query(Domain)
        if not include_disabled:
            query = query.filter(Domain.enabled == 1)
        if search:
            like = f"%{search}%"
            query = query.filter(
                (Domain.name.ilike(like))
                | (Domain.code.ilike(like))
                | (Domain.le_domain.ilike(like))
                | (Domain.username.ilike(like))
            )
        total = query.count()
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        rows = (
            query.order_by(Domain.name)
            .offset((page - 1) * per_page)
            .limit(per_page)
            .all()
        )
        domains = [
            {
                "id": d.id,
                "code": d.code,
                "name": d.name,
                "api_url": d.api_url,
                "username": d.username,
                "usertoken": d.usertoken,
                "le_domain": d.le_domain,
                "phase": d.phase,
                "enabled": bool(d.enabled),
                "created_at": d.created_at,
                "updated_at": d.updated_at,
            }
            for d in rows
        ]
        return {
            "domains": domains,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
        }
    finally:
        session.close()


def get_domain_by_id(domain_id: int) -> Optional[dict]:
    """Get a single domain by its primary key."""
    session = get_session()
    try:
        d = session.query(Domain).filter(Domain.id == domain_id).first()
        if not d:
            return None
        return {
            "id": d.id,
            "code": d.code,
            "name": d.name,
            "api_url": d.api_url,
            "username": d.username,
            "usertoken": d.usertoken,
            "le_domain": d.le_domain,
            "phase": d.phase,
            "enabled": bool(d.enabled),
            "created_at": d.created_at,
            "updated_at": d.updated_at,
        }
    finally:
        session.close()


def create_domain(data: dict) -> dict:
    """Create a new domain. Returns the created domain dict."""
    session = get_session()
    try:
        # Check for duplicate code
        existing = session.query(Domain).filter(Domain.code == data["code"]).first()
        if existing:
            raise ValueError(f"Domain with code '{data['code']}' already exists")
        domain = Domain(
            code=data["code"],
            name=data["name"],
            api_url=data["api_url"],
            username=data["username"],
            usertoken=data["usertoken"],
            le_domain=data["le_domain"],
            phase=int(data.get("phase", 2)),
            enabled=1 if data.get("enabled", True) else 0,
        )
        session.add(domain)
        session.commit()
        session.refresh(domain)
        logger.info("Created domain: %s (%s)", domain.name, domain.code)
        return {
            "id": domain.id,
            "code": domain.code,
            "name": domain.name,
            "api_url": domain.api_url,
            "username": domain.username,
            "usertoken": domain.usertoken,
            "le_domain": domain.le_domain,
            "phase": domain.phase,
            "enabled": bool(domain.enabled),
            "created_at": domain.created_at,
            "updated_at": domain.updated_at,
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def update_domain(domain_id: int, data: dict) -> Optional[dict]:
    """Update an existing domain by ID. Returns updated domain dict or None."""
    session = get_session()
    try:
        domain = session.query(Domain).filter(Domain.id == domain_id).first()
        if not domain:
            return None
        # Check code uniqueness if code changed
        if "code" in data and data["code"] != domain.code:
            existing = session.query(Domain).filter(Domain.code == data["code"]).first()
            if existing:
                raise ValueError(f"Domain with code '{data['code']}' already exists")
            domain.code = data["code"]
        if "name" in data:
            domain.name = data["name"]
        if "api_url" in data:
            domain.api_url = data["api_url"]
        if "username" in data:
            domain.username = data["username"]
        if "usertoken" in data:
            domain.usertoken = data["usertoken"]
        if "le_domain" in data:
            domain.le_domain = data["le_domain"]
        if "phase" in data:
            domain.phase = int(data["phase"])
        if "enabled" in data:
            domain.enabled = 1 if data["enabled"] else 0
        domain.updated_at = datetime.utcnow().isoformat()
        session.commit()
        session.refresh(domain)
        logger.info("Updated domain: %s (%s)", domain.name, domain.code)
        return {
            "id": domain.id,
            "code": domain.code,
            "name": domain.name,
            "api_url": domain.api_url,
            "username": domain.username,
            "usertoken": domain.usertoken,
            "le_domain": domain.le_domain,
            "phase": domain.phase,
            "enabled": bool(domain.enabled),
            "created_at": domain.created_at,
            "updated_at": domain.updated_at,
        }
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def delete_domain(domain_id: int) -> bool:
    """
    Delete a domain by ID (cascades to campaigns & stats).
    Returns True if deleted, False if not found.
    """
    session = get_session()
    try:
        domain = session.query(Domain).filter(Domain.id == domain_id).first()
        if not domain:
            return False
        logger.info(
            "Deleting domain: %s (%s) and all its campaigns", domain.name, domain.code
        )
        session.delete(domain)
        session.commit()
        return True
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Campaign CRUD
# ---------------------------------------------------------------------------
def upsert_campaign(domain_id: int, data: dict) -> int:
    statid = data.get("statid") or data.get("campaign_id")
    is_seed = data.get("is_seed", 0)
    session = get_session()
    try:
        campaign = (
            session.query(Campaign)
            .filter(Campaign.domain_id == domain_id, Campaign.statid == statid)
            .first()
        )
        if campaign:
            campaign.campaign_id = data["campaign_id"]
            campaign.campaign_name = data["campaign_name"]
            campaign.date = data["date"]
            campaign.time = data["time"]
            campaign.is_seed = is_seed
            campaign.updated_at = datetime.utcnow().isoformat()
        else:
            campaign = Campaign(
                domain_id=domain_id,
                statid=statid,
                campaign_id=data["campaign_id"],
                campaign_name=data["campaign_name"],
                date=data["date"],
                time=data["time"],
                is_seed=is_seed,
            )
            session.add(campaign)
        session.commit()
        session.refresh(campaign)
        return campaign.id
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def update_campaign_stats(campaign_id: int, stats: dict) -> None:
    session = get_session()
    try:
        stat = (
            session.query(CampaignStat)
            .filter(CampaignStat.campaign_id == campaign_id)
            .first()
        )
        now = datetime.utcnow().isoformat()
        if stat:
            stat.sends = stats.get("sends", 0)
            stat.opens = stats.get("opens", 0)
            stat.open_percent = stats.get("open_percent", 0.0)
            stat.clicks = stats.get("clicks", 0)
            stat.click_percent = stats.get("click_percent", 0.0)
            stat.bounces = stats.get("bounces", 0)
            stat.bounce_percent = stats.get("bounce_percent", 0.0)
            stat.unsubs = stats.get("unsubs", 0)
            stat.last_fetched_at = now
        else:
            stat = CampaignStat(
                campaign_id=campaign_id,
                sends=stats.get("sends", 0),
                opens=stats.get("opens", 0),
                open_percent=stats.get("open_percent", 0.0),
                clicks=stats.get("clicks", 0),
                click_percent=stats.get("click_percent", 0.0),
                bounces=stats.get("bounces", 0),
                bounce_percent=stats.get("bounce_percent", 0.0),
                unsubs=stats.get("unsubs", 0),
                last_fetched_at=now,
            )
            session.add(stat)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_campaign_by_statid(domain_id: int, statid: str) -> Optional[dict]:
    session = get_session()
    try:
        c = (
            session.query(Campaign)
            .filter(Campaign.domain_id == domain_id, Campaign.statid == statid)
            .first()
        )
        if not c:
            return None
        return {
            "id": c.id,
            "domain_id": c.domain_id,
            "statid": c.statid,
            "campaign_id": c.campaign_id,
            "campaign_name": c.campaign_name,
            "date": c.date,
            "time": c.time,
            "is_seed": c.is_seed,
        }
    finally:
        session.close()


def get_campaign_count_by_date_range(start: str, end: str) -> int:
    session = get_session()
    try:
        return session.query(Campaign).filter(Campaign.date.between(start, end)).count()
    finally:
        session.close()


def get_campaigns_by_date_range(
    start: str, end: str, seed_only: bool = False
) -> list[dict]:
    session = get_session()
    try:
        query = (
            session.query(
                Domain.code.label("domain_code"),
                Domain.name.label("domain_name"),
                Domain.le_domain,
                Campaign.statid,
                Campaign.campaign_id,
                Campaign.campaign_name,
                Campaign.date,
                Campaign.time,
                Campaign.is_seed,
                CampaignStat.sends,
                CampaignStat.opens,
                CampaignStat.open_percent,
                CampaignStat.clicks,
                CampaignStat.click_percent,
                CampaignStat.bounces,
                CampaignStat.bounce_percent,
                CampaignStat.unsubs,
                CampaignStat.last_fetched_at,
            )
            .join(Campaign, Domain.id == Campaign.domain_id)
            .outerjoin(CampaignStat, Campaign.id == CampaignStat.campaign_id)
            .filter(Domain.enabled == 1)
            .filter(Campaign.date.between(start, end))
        )

        if seed_only is True:
            query = query.filter(Campaign.is_seed == 1)
        elif seed_only is False:
            query = query.filter((Campaign.is_seed == 0) | (Campaign.is_seed.is_(None)))

        query = query.order_by(Domain.name, Campaign.date.desc(), Campaign.time.desc())

        rows = query.all()
        return [
            {
                "domain_code": r.domain_code,
                "domain_name": r.domain_name,
                "le_domain": r.le_domain,
                "statid": r.statid,
                "campaign_id": r.campaign_id,
                "campaign_name": r.campaign_name,
                "date": r.date,
                "time": r.time,
                "is_seed": r.is_seed,
                "sends": r.sends,
                "opens": r.opens,
                "open_percent": r.open_percent,
                "clicks": r.clicks,
                "click_percent": r.click_percent,
                "bounces": r.bounces,
                "bounce_percent": r.bounce_percent,
                "unsubs": r.unsubs,
                "last_fetched_at": r.last_fetched_at,
            }
            for r in rows
        ]
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Leadpier Revenue CRUD
# ---------------------------------------------------------------------------
def upsert_leadpier_sources(report_date: str, sources: list[dict]) -> int:
    """
    Bulk upsert Leadpier source records for a given date.
    Returns the number of records upserted.
    """
    session = get_session()
    count = 0
    try:
        for src in sources:
            source_name = src.get("source", "")
            if not source_name:
                continue
            existing = (
                session.query(LeadpierSource)
                .filter(
                    LeadpierSource.source_name == source_name,
                    LeadpierSource.report_date == report_date,
                )
                .first()
            )
            now = datetime.utcnow().isoformat()
            if existing:
                existing.visitors = int(src.get("visitors", 0) or 0)
                existing.total_leads = int(src.get("totalLeads", 0) or 0)
                existing.sold_leads = int(src.get("soldLeads", 0) or 0)
                existing.total_revenue = float(src.get("totalRevenue", 0) or 0)
                existing.epl = float(src.get("EPL", 0) or 0)
                existing.epv = float(src.get("EPV", 0) or 0)
                existing.fetched_at = now
            else:
                session.add(
                    LeadpierSource(
                        source_name=source_name,
                        report_date=report_date,
                        visitors=int(src.get("visitors", 0) or 0),
                        total_leads=int(src.get("totalLeads", 0) or 0),
                        sold_leads=int(src.get("soldLeads", 0) or 0),
                        total_revenue=float(src.get("totalRevenue", 0) or 0),
                        epl=float(src.get("EPL", 0) or 0),
                        epv=float(src.get("EPV", 0) or 0),
                        fetched_at=now,
                    )
                )
            count += 1
        session.commit()
        logger.info("Upserted %d Leadpier sources for %s", count, report_date)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
    return count


def get_leadpier_sources_by_date(report_date: str) -> list[dict]:
    """Return all cached Leadpier source records for a given date."""
    session = get_session()
    try:
        rows = (
            session.query(LeadpierSource)
            .filter(LeadpierSource.report_date == report_date)
            .all()
        )
        return [
            {
                "source": r.source_name,
                "visitors": r.visitors,
                "totalLeads": r.total_leads,
                "soldLeads": r.sold_leads,
                "totalRevenue": r.total_revenue,
                "EPL": r.epl,
                "EPV": r.epv,
                "fetched_at": r.fetched_at,
            }
            for r in rows
        ]
    finally:
        session.close()


def get_leadpier_last_sync(report_date: str) -> Optional[str]:
    """Return the most recent fetched_at timestamp for a given date, or None."""
    session = get_session()
    try:
        row = (
            session.query(LeadpierSource.fetched_at)
            .filter(LeadpierSource.report_date == report_date)
            .order_by(LeadpierSource.fetched_at.desc())
            .first()
        )
        return row.fetched_at if row else None
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Data cleanup — remove records older than N days
# ---------------------------------------------------------------------------
def cleanup_old_data(days: int = 30) -> dict[str, int]:
    """
    Delete campaigns (+ cascade stats) and leadpier_sources
    where the date is older than `days` days ago.
    Returns counts of deleted rows per table.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    session = get_session()
    try:
        # Campaign rows older than cutoff (cascade deletes CampaignStat)
        old_campaigns = session.query(Campaign).filter(Campaign.date < cutoff).all()
        campaigns_deleted = len(old_campaigns)
        stats_deleted = 0
        for c in old_campaigns:
            if c.stats:
                stats_deleted += 1
            session.delete(c)

        # Leadpier source rows older than cutoff
        lp_deleted = (
            session.query(LeadpierSource)
            .filter(LeadpierSource.report_date < cutoff)
            .delete(synchronize_session="fetch")
        )

        session.commit()
        logger.info(
            "Cleanup: removed %d campaigns, %d stats, %d leadpier sources "
            "(older than %s, %d days)",
            campaigns_deleted,
            stats_deleted,
            lp_deleted,
            cutoff,
            days,
        )
        return {
            "campaigns": campaigns_deleted,
            "campaign_stats": stats_deleted,
            "leadpier_sources": lp_deleted,
            "cutoff_date": cutoff,
        }
    except Exception as e:
        session.rollback()
        logger.error("Cleanup failed: %s", e)
        raise
    finally:
        session.close()
