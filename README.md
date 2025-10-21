"""
# 🚀 Voxmill Deployment Guide - Render

## Prerequisites

1. GitHub account
2. Render account (render.com)
3. All API keys ready

---

## Step 1: Push to GitHub

```bash
# Initialize git repo
git init
git add .
git commit -m "Initial Voxmill Intelligence Platform"

# Create GitHub repo and push
git remote add origin https://github.com/yourusername/voxmill-intelligence.git
git branch -M main
git push -u origin main
```

---

## Step 2: Deploy to Render

### Option A: Auto-Deploy (Recommended)

1. Go to https://dashboard.render.com
2. Click "New" → "Blueprint"
3. Connect your GitHub repository
4. Render will auto-detect `render.yaml`
5. Click "Apply"

### Option B: Manual Setup

**Create PostgreSQL Database:**
1. Dashboard → "New" → "PostgreSQL"
2. Name: `voxmill-db`
3. Region: Frankfurt (or closest to you)
4. Plan: Starter ($7/mo) or Free
5. Click "Create Database"
6. Copy the "Internal Database URL"

**Create Redis:**
1. Dashboard → "New" → "Redis"
2. Name: `voxmill-redis`
3. Region: Frankfurt
4. Plan: Starter ($10/mo) or Free
5. Click "Create Redis"

**Create Web Service:**
1. Dashboard → "New" → "Web Service"
2. Connect to GitHub repo
3. Settings:
   - Name: `voxmill-api`
   - Region: Frankfurt
   - Branch: `main`
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
4. Environment Variables (click "Add Environment Variable"):
   ```
   DATABASE_URL = [paste from PostgreSQL internal URL]
   REDIS_URL = [paste from Redis URL]
   GOOGLE_PLACES_API = AIzaSyECVDGLffKyYBsn1U1aPs4nXubAtzA
   SERP_API = effe5fa5c5a4a81fffe1a32ea2a257f6a2097fc38ca5ca5d5a67bd29f7e0303d
   YELP_API = RP4QNPPXDJLioJAPyQcQ9hKnzsGZJ_PjpkYVcpokpE4nrqPElt4qhGk3GyuEcHiRPc2wE3gjtFG9rFV8WqR8fPYBcuqPJWaJdPTpjbcxmj
   FACEBOOK_API = [your key]
   OUTSCRAPER_API = [your key]
   OPENAI_API_KEY = [optional - for AI features]
   ENVIRONMENT = production
   ```
5. Click "Create Web Service"

**Create Worker:**
1. Dashboard → "New" → "Background Worker"
2. Connect to same GitHub repo
3. Settings:
   - Name: `voxmill-worker`
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `celery -A app.worker worker --loglevel=info`
4. Add same environment variables as Web Service
5. Click "Create Background Worker"

---

## Step 3: Initialize Database

Once deployed, run database migrations:

```bash
# SSH into web service (use Render Shell)
alembic upgrade head
```

Or use Render's Shell:
1. Go to your Web Service dashboard
2. Click "Shell" tab
3. Run: `alembic upgrade head`

---

## Step 4: Test the API

Your API will be live at: `https://voxmill-api.onrender.com`

Test health check:
```bash
curl https://voxmill-api.onrender.com/health
```

---

## Step 5: Mine Your First Leads

### Via API:
```bash
curl -X POST https://voxmill-api.onrender.com/api/mine-leads \
  -H "Content-Type: application/json" \
  -d '{
    "categories": ["boutique real estate"],
    "cities": ["London"],
    "country": "UK",
    "target_type": "voxmill"
  }'
```

### Via CLI (if you clone locally):
```bash
python cli.py mine --city London --category "boutique real estate" --country UK
```

---

## Step 6: Access Your Data

### View leads:
```
GET https://voxmill-api.onrender.com/api/leads?priority_min=7
```

### Export to CSV:
```
GET https://voxmill-api.onrender.com/api/export/csv?priority_min=7&country=UK
```

### View specific lead:
```
GET https://voxmill-api.onrender.com/api/leads/1
```

---

## Monitoring & Logs

1. **Web Service Logs:** Dashboard → voxmill-api → "Logs" tab
2. **Worker Logs:** Dashboard → voxmill-worker → "Logs" tab
3. **Database:** Dashboard → voxmill-db → "Info" tab

---

## Scheduled Tasks (Optional)

To run weekly lead refreshes:

1. Dashboard → voxmill-worker
2. Click "Cron Jobs"
3. Add:
   - Name: `Weekly Refresh`
   - Schedule: `0 9 * * 1` (Every Monday 9am)
   - Command: `celery -A app.worker call app.worker.weekly_lead_refresh`

---

## Troubleshooting

**"Module not found" errors:**
- Check requirements.txt is complete
- Rebuild service: Dashboard → Service → "Manual Deploy" → "Clear build cache & deploy"

**Database connection errors:**
- Verify DATABASE_URL is correct
- Check PostgreSQL is running
- Ensure alembic migrations ran

**Worker not processing:**
- Check REDIS_URL is correct
- View worker logs for errors
- Restart worker service

**API timeout:**
- Check worker is running (it processes the jobs)
- View logs for specific error messages

---

## Cost Summary

**Monthly Costs:**
- PostgreSQL Starter: $7/mo
- Redis Starter: $10/mo (or Free tier)
- Web Service: $7/mo
- Worker: $7/mo

**Total: ~$21-31/mo**

**Plus API costs:**
- Google Places: ~£10-15/month (400 leads)
- SerpAPI: ~£5/month
- Yelp: Free

**Grand Total: ~£40-50/mo for production system**

---

## Next Steps

1. ✅ Deploy platform
2. ✅ Mine first 100 leads
3. ✅ Export top 20 priority leads
4. ✅ Start outreach campaign
5. ✅ Track conversions in `/api/outreach/log`
6. ✅ Close first client
7. ✅ Scale to 10 clients

**You now have a production-grade lead intelligence system running 24/7.**
"""
