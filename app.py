import os
import logging
import threading
from datetime import datetime, timedelta, date
from collections import defaultdict

from flask import Flask, render_template, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from apscheduler.schedulers.background import BackgroundScheduler
import stripe
import pytz

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

database_url = os.environ.get("DATABASE_URL", "")
if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = database_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"pool_pre_ping": True}

db = SQLAlchemy(app)
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")

CHICAGO_TZ = pytz.timezone("America/Chicago")

# Sync state (in-process, single worker)
_sync_lock = threading.Lock()
_sync_state = {"running": False, "count": 0, "error": None, "last_completed": None}

# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
class Charge(db.Model):
    __tablename__ = "charges"

    id = db.Column(db.String, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    created = db.Column(db.DateTime(timezone=True), nullable=False, index=True)
    currency = db.Column(db.String(8), nullable=False)
    status = db.Column(db.String(32), nullable=False)
    has_invoice = db.Column(db.Boolean, nullable=False, default=False)
    subscription_id = db.Column(db.String, nullable=True)
    billing_reason = db.Column(db.String(64), nullable=True)
    refunded = db.Column(db.Boolean, nullable=False, default=False)
    amount_refunded = db.Column(db.BigInteger, nullable=False, default=0)
    charge_type = db.Column(db.String(16), nullable=False)


# ---------------------------------------------------------------------------
# Stripe sync helpers
# ---------------------------------------------------------------------------
def _classify(has_invoice: bool, subscription_id: str | None) -> str:
    """Return 'recurring' if tied to a subscription invoice, else 'new_hardware'."""
    if has_invoice and subscription_id:
        return "recurring"
    return "new_hardware"


def _extract_invoice_fields(invoice_obj):
    """Pull subscription_id and billing_reason from an invoice object or ID."""
    subscription_id = None
    billing_reason = None

    if invoice_obj is None:
        return subscription_id, billing_reason

    # Expanded Invoice object
    if hasattr(invoice_obj, "id"):
        sub = invoice_obj.subscription
        if sub is not None:
            subscription_id = sub if isinstance(sub, str) else getattr(sub, "id", str(sub))
        billing_reason = getattr(invoice_obj, "billing_reason", None)
    # Unexpanded string ID — fetch it
    elif isinstance(invoice_obj, str):
        try:
            inv = stripe.Invoice.retrieve(invoice_obj)
            sub = inv.subscription
            if sub is not None:
                subscription_id = sub if isinstance(sub, str) else getattr(sub, "id", str(sub))
            billing_reason = inv.billing_reason
        except Exception:
            logger.warning("Could not fetch invoice %s", invoice_obj)

    return subscription_id, billing_reason


def sync_charges(full: bool = False) -> int:
    """Fetch charges from Stripe and upsert into Postgres.

    full=True  → backfill all historical charges
    full=False → last 90 days only (catches recent refunds)
    """
    params: dict = {"limit": 100, "expand": ["data.invoice"]}
    if not full:
        cutoff = datetime.utcnow() - timedelta(days=90)
        params["created"] = {"gte": int(cutoff.timestamp())}

    count = 0
    for ch in stripe.Charge.list(**params).auto_paging_iter():
        invoice_obj = ch.invoice
        has_invoice = invoice_obj is not None
        subscription_id, billing_reason = _extract_invoice_fields(invoice_obj)
        charge_type = _classify(has_invoice, subscription_id)
        created_dt = datetime.utcfromtimestamp(ch.created).replace(tzinfo=pytz.utc)

        existing = db.session.get(Charge, ch.id)
        if existing:
            existing.amount = ch.amount
            existing.status = ch.status
            existing.refunded = ch.refunded
            existing.amount_refunded = ch.amount_refunded
            existing.has_invoice = has_invoice
            existing.subscription_id = subscription_id
            existing.billing_reason = billing_reason
            existing.charge_type = charge_type
        else:
            db.session.add(
                Charge(
                    id=ch.id,
                    amount=ch.amount,
                    created=created_dt,
                    currency=ch.currency,
                    status=ch.status,
                    has_invoice=has_invoice,
                    subscription_id=subscription_id,
                    billing_reason=billing_reason,
                    refunded=ch.refunded,
                    amount_refunded=ch.amount_refunded,
                    charge_type=charge_type,
                )
            )

        count += 1
        _sync_state["count"] = count
        if count % 500 == 0:
            db.session.commit()
            logger.info("Synced %d charges so far …", count)

    db.session.commit()
    logger.info("Sync complete — %d charges processed.", count)
    return count


def _run_sync_background(full: bool):
    """Run sync in a background thread with state tracking."""
    with app.app_context():
        try:
            _sync_state["running"] = True
            _sync_state["count"] = 0
            _sync_state["error"] = None
            count = sync_charges(full=full)
            _sync_state["last_completed"] = datetime.utcnow().isoformat() + "Z"
        except Exception as e:
            logger.exception("Background sync failed")
            _sync_state["error"] = str(e)
        finally:
            _sync_state["running"] = False


# ---------------------------------------------------------------------------
# Scheduled background sync
# ---------------------------------------------------------------------------
def _scheduled_sync():
    with app.app_context():
        try:
            sync_charges(full=False)
        except Exception:
            logger.exception("Scheduled sync failed")


scheduler = BackgroundScheduler(daemon=True)
sync_interval = int(os.environ.get("SYNC_INTERVAL_MINUTES", "60"))
scheduler.add_job(_scheduled_sync, "interval", minutes=sync_interval)
scheduler.start()

# ---------------------------------------------------------------------------
# Database init
# ---------------------------------------------------------------------------
with app.app_context():
    db.create_all()

# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
_DATE_EXPR = "(created AT TIME ZONE 'America/Chicago')::date"
_SUCCEEDED = "status = 'succeeded'"


def _parse_range(args, default_days=90):
    """Return (start_date, end_date) from request args."""
    s = args.get("start")
    e = args.get("end")
    if s and e:
        return date.fromisoformat(s), date.fromisoformat(e)
    chicago_now = datetime.now(CHICAGO_TZ)
    end_d = chicago_now.date()
    start_d = end_d - timedelta(days=default_days)
    return start_d, end_d


def _day_summary(target_date):
    rows = db.session.execute(
        text(f"""
            SELECT charge_type, COALESCE(SUM(amount), 0) AS total
            FROM charges
            WHERE {_SUCCEEDED}
              AND {_DATE_EXPR} = :d
            GROUP BY charge_type
        """),
        {"d": target_date},
    )
    out = {"total": 0, "new_hardware": 0, "recurring": 0}
    for r in rows:
        out[r.charge_type] = r.total / 100
        out["total"] += r.total / 100
    return out


def _range_summary(start_d, end_d):
    rows = db.session.execute(
        text(f"""
            SELECT charge_type, COALESCE(SUM(amount), 0) AS total
            FROM charges
            WHERE {_SUCCEEDED}
              AND {_DATE_EXPR} BETWEEN :s AND :e
            GROUP BY charge_type
        """),
        {"s": start_d, "e": end_d},
    )
    out = {"total": 0, "new_hardware": 0, "recurring": 0}
    for r in rows:
        out[r.charge_type] = r.total / 100
        out["total"] += r.total / 100
    return out


# ---------------------------------------------------------------------------
# Routes — pages
# ---------------------------------------------------------------------------
@app.route("/")
def dashboard():
    return render_template("dashboard.html")


# ---------------------------------------------------------------------------
# Routes — sync
# ---------------------------------------------------------------------------
@app.route("/sync", methods=["GET", "POST"])
def sync_endpoint():
    full = request.args.get("full", "false").lower() == "true"
    if _sync_state["running"]:
        return jsonify({"status": "already_running", "charges_so_far": _sync_state["count"]}), 409
    if not _sync_lock.acquire(blocking=False):
        return jsonify({"status": "already_running"}), 409
    try:
        t = threading.Thread(target=_run_sync_background, args=(full,), daemon=True)
        t.start()
    finally:
        _sync_lock.release()
    return jsonify({"status": "started", "full": full})


@app.route("/sync/status")
def sync_status():
    return jsonify(_sync_state)


# ---------------------------------------------------------------------------
# Routes — API
# ---------------------------------------------------------------------------
@app.route("/api/daily-revenue")
def api_daily_revenue():
    start_d, end_d = _parse_range(request.args, default_days=90)

    rows = db.session.execute(
        text(f"""
            SELECT {_DATE_EXPR} AS day, charge_type, SUM(amount) AS total
            FROM charges
            WHERE {_SUCCEEDED}
              AND {_DATE_EXPR} BETWEEN :s AND :e
            GROUP BY day, charge_type
            ORDER BY day
        """),
        {"s": start_d, "e": end_d},
    )

    data = defaultdict(lambda: {"new_hardware": 0, "recurring": 0})
    for r in rows:
        data[r.day][r.charge_type] = r.total

    labels, new_hw, recurring, total = [], [], [], []
    current = start_d
    while current <= end_d:
        labels.append(current.isoformat())
        nh = data[current]["new_hardware"] / 100
        rec = data[current]["recurring"] / 100
        new_hw.append(round(nh, 2))
        recurring.append(round(rec, 2))
        total.append(round(nh + rec, 2))
        current += timedelta(days=1)

    return jsonify(
        labels=labels, total=total, new_hardware=new_hw, recurring=recurring
    )


@app.route("/api/summary")
def api_summary():
    chicago_now = datetime.now(CHICAGO_TZ)
    today_chi = chicago_now.date()
    yesterday_chi = today_chi - timedelta(days=1)
    month_start = today_chi.replace(day=1)

    resp = {
        "today": _day_summary(today_chi),
        "yesterday": _day_summary(yesterday_chi),
        "mtd": _range_summary(month_start, today_chi),
    }

    s = request.args.get("start")
    e = request.args.get("end")
    if s and e:
        resp["period"] = _range_summary(date.fromisoformat(s), date.fromisoformat(e))

    return jsonify(resp)


@app.route("/api/revenue-by-dow")
def api_revenue_by_dow():
    start_d, end_d = _parse_range(request.args, default_days=90)

    rows = db.session.execute(
        text(f"""
            SELECT
                EXTRACT(DOW FROM created AT TIME ZONE 'America/Chicago')::int AS dow,
                charge_type,
                SUM(amount) AS total
            FROM charges
            WHERE {_SUCCEEDED}
              AND {_DATE_EXPR} BETWEEN :s AND :e
            GROUP BY dow, charge_type
            ORDER BY dow
        """),
        {"s": start_d, "e": end_d},
    )

    data = defaultdict(lambda: {"new_hardware": 0, "recurring": 0})
    for r in rows:
        data[r.dow][r.charge_type] = r.total / 100

    day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    labels = day_names
    new_hw = [round(data[i]["new_hardware"], 2) for i in range(7)]
    recurring = [round(data[i]["recurring"], 2) for i in range(7)]
    total = [round(new_hw[i] + recurring[i], 2) for i in range(7)]

    return jsonify(labels=labels, total=total, new_hardware=new_hw, recurring=recurring)


@app.route("/api/revenue-by-dow-half")
def api_revenue_by_dow_half():
    start_d, end_d = _parse_range(request.args, default_days=90)

    rows = db.session.execute(
        text(f"""
            SELECT
                EXTRACT(DOW FROM created AT TIME ZONE 'America/Chicago')::int AS dow,
                CASE WHEN EXTRACT(HOUR FROM created AT TIME ZONE 'America/Chicago') < 12
                     THEN 'first' ELSE 'second' END AS half,
                SUM(amount) AS total
            FROM charges
            WHERE {_SUCCEEDED}
              AND {_DATE_EXPR} BETWEEN :s AND :e
            GROUP BY dow, half
            ORDER BY dow
        """),
        {"s": start_d, "e": end_d},
    )

    data = defaultdict(lambda: {"first": 0, "second": 0})
    for r in rows:
        data[r.dow][r.half] = r.total / 100

    day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    first = [round(data[i]["first"], 2) for i in range(7)]
    second = [round(data[i]["second"], 2) for i in range(7)]

    return jsonify(labels=day_names, first_half=first, second_half=second)


@app.route("/api/revenue-by-hour")
def api_revenue_by_hour():
    start_d, end_d = _parse_range(request.args, default_days=90)

    rows = db.session.execute(
        text(f"""
            SELECT
                EXTRACT(HOUR FROM created AT TIME ZONE 'America/Chicago')::int AS hr,
                charge_type,
                SUM(amount) AS total
            FROM charges
            WHERE {_SUCCEEDED}
              AND {_DATE_EXPR} BETWEEN :s AND :e
            GROUP BY hr, charge_type
            ORDER BY hr
        """),
        {"s": start_d, "e": end_d},
    )

    data = defaultdict(lambda: {"new_hardware": 0, "recurring": 0})
    for r in rows:
        data[r.hr][r.charge_type] = r.total / 100

    labels = [f"{h:02d}:00" for h in range(24)]
    new_hw = [round(data[h]["new_hardware"], 2) for h in range(24)]
    recurring = [round(data[h]["recurring"], 2) for h in range(24)]
    total = [round(new_hw[h] + recurring[h], 2) for h in range(24)]

    return jsonify(labels=labels, total=total, new_hardware=new_hw, recurring=recurring)


@app.route("/api/refund-rate")
def api_refund_rate():
    rows = db.session.execute(
        text(f"""
            SELECT
                TO_CHAR(created AT TIME ZONE 'America/Chicago', 'YYYY-MM') AS month,
                COUNT(*)::int AS total_orders,
                SUM(CASE WHEN refunded OR amount_refunded > 0 THEN 1 ELSE 0 END)::int AS refunded_orders
            FROM charges
            WHERE {_SUCCEEDED}
            GROUP BY month
            ORDER BY month
        """)
    )

    labels, rates, totals, refundeds = [], [], [], []
    for r in rows:
        labels.append(r.month)
        pct = round((r.refunded_orders / r.total_orders) * 100, 2) if r.total_orders else 0
        rates.append(pct)
        totals.append(r.total_orders)
        refundeds.append(r.refunded_orders)

    return jsonify(
        labels=labels, rates=rates, totals=totals, refundeds=refundeds
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5000)
