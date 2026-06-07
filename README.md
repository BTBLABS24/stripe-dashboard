# Stripe Revenue Dashboard

Single-service Flask app that syncs Stripe charges into Postgres and renders a Chart.js dashboard.

## Deploy to Railway

1. Create a new project on [Railway](https://railway.com).
2. Add a **Postgres** plugin (Railway add-on).
3. Connect this repo (or push via Railway CLI).
4. Set the environment variables below in the service settings.
5. Deploy. Railway will auto-detect the `Procfile`.

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `STRIPE_SECRET_KEY` | **Yes** | Your Stripe secret key (`sk_live_…` or `sk_test_…`) |
| `DATABASE_URL` | **Yes** | Provided automatically by the Railway Postgres plugin |
| `SYNC_INTERVAL_MINUTES` | No | Background sync interval (default: `60`) |

## Initial Data Load

After the first deploy, trigger a full historical backfill:

```bash
curl -X POST https://<your-app>.up.railway.app/sync?full=true
```

Subsequent syncs (the background job and plain `/sync` calls) only pull the last 90 days to stay fast while still catching recent refunds.

## Endpoints

| Path | Method | Purpose |
|---|---|---|
| `/` | GET | Dashboard UI |
| `/sync` | GET/POST | Incremental sync (last 90 days) |
| `/sync?full=true` | GET/POST | Full historical backfill |
| `/api/daily-revenue` | GET | Daily revenue JSON (supports `start` / `end` query params) |
| `/api/summary` | GET | Today / yesterday / MTD summary |
| `/api/revenue-by-dow` | GET | Revenue grouped by day of week |
| `/api/revenue-by-hour` | GET | Revenue grouped by hour (Chicago time) |
| `/api/refund-rate` | GET | Monthly refund-rate percentages |

## Local Development

```bash
pip install -r requirements.txt
export STRIPE_SECRET_KEY=sk_test_…
export DATABASE_URL=postgresql://user:pass@localhost:5432/stripe_dash
python app.py
```

The dev server runs on `http://localhost:5000`.
