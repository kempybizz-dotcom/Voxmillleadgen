"""
Voxmill Intelligence Platform - Main FastAPI Application
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
import logging

from app.config import settings
from app.database import get_db, engine
from app import models, schemas
from app.services.mining.lead_miner import LeadMiner
from app.services.intelligence.intel_processor import IntelligenceProcessor
from app.worker import mine_leads_task

# Create database tables
models.Base.metadata.create_all(bind=engine)

# Initialize FastAPI
app = FastAPI(
    title="Voxmill Intelligence Platform",
    description="Automated lead intelligence for market research",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================
# HEALTH CHECK
# ============================================
@app.get("/")
def read_root():
    return {
        "status": "operational",
        "service": "Voxmill Intelligence Platform",
        "version": "1.0.0"
    }

@app.get("/health")
def health_check():
    return {"status": "healthy"}


# ============================================
# LEAD MINING ENDPOINTS
# ============================================
@app.post("/api/mine-leads", response_model=schemas.MiningJobResponse)
async def start_mining(
    request: schemas.MiningRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Start a lead mining job
    """
    try:
        # Queue the mining task
        task = mine_leads_task.delay(
            categories=request.categories,
            cities=request.cities,
            country=request.country,
            target_type=request.target_type
        )
        
        return {
            "job_id": task.id,
            "status": "queued",
            "message": f"Mining job started for {len(request.cities)} cities"
        }
    except Exception as e:
        logger.error(f"Error starting mining job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/leads", response_model=List[schemas.LeadResponse])
def get_leads(
    priority_min: Optional[int] = None,
    country: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Get leads with optional filters
    """
    query = db.query(models.Lead)
    
    if priority_min:
        query = query.filter(models.Lead.priority_score >= priority_min)
    if country:
        query = query.filter(models.Lead.country == country)
    if category:
        query = query.filter(models.Lead.category == category)
    
    query = query.order_by(models.Lead.priority_score.desc())
    leads = query.limit(limit).all()
    
    return leads


@app.get("/api/leads/{lead_id}", response_model=schemas.LeadDetailResponse)
def get_lead(lead_id: int, db: Session = Depends(get_db)):
    """
    Get single lead with full intelligence
    """
    lead = db.query(models.Lead).filter(models.Lead.id == lead_id).first()
    
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    return lead


# ============================================
# INTELLIGENCE ENDPOINTS
# ============================================
@app.get("/api/intel/{lead_id}", response_model=schemas.IntelligenceResponse)
def get_intelligence(lead_id: int, db: Session = Depends(get_db)):
    """
    Get full intelligence report for a lead
    """
    intel = db.query(models.Intelligence).filter(
        models.Intelligence.lead_id == lead_id
    ).first()
    
    if not intel:
        raise HTTPException(status_code=404, detail="Intelligence not found")
    
    return intel


@app.post("/api/intel/refresh/{lead_id}")
async def refresh_intelligence(
    lead_id: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Refresh intelligence data for a lead
    """
    lead = db.query(models.Lead).filter(models.Lead.id == lead_id).first()
    
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    
    # Queue refresh task
    background_tasks.add_task(refresh_lead_data, lead_id, db)
    
    return {"message": "Intelligence refresh queued", "lead_id": lead_id}


def refresh_lead_data(lead_id: int, db: Session):
    """Background task to refresh lead data"""
    processor = IntelligenceProcessor()
    # Implementation in services
    pass


# ============================================
# OUTREACH ENDPOINTS
# ============================================
@app.post("/api/outreach/log", response_model=schemas.OutreachLogResponse)
def log_outreach(
    outreach: schemas.OutreachLogCreate,
    db: Session = Depends(get_db)
):
    """
    Log an outreach message
    """
    db_outreach = models.OutreachLog(**outreach.dict())
    db.add(db_outreach)
    db.commit()
    db.refresh(db_outreach)
    
    return db_outreach


@app.get("/api/outreach/stats")
def get_outreach_stats(db: Session = Depends(get_db)):
    """
    Get outreach conversion statistics
    """
    total_sent = db.query(models.OutreachLog).count()
    total_replied = db.query(models.OutreachLog).filter(
        models.OutreachLog.replied == True
    ).count()
    total_calls = db.query(models.OutreachLog).filter(
        models.OutreachLog.call_booked == True
    ).count()
    total_closed = db.query(models.OutreachLog).filter(
        models.OutreachLog.deal_closed == True
    ).count()
    
    reply_rate = (total_replied / total_sent * 100) if total_sent > 0 else 0
    call_rate = (total_calls / total_replied * 100) if total_replied > 0 else 0
    close_rate = (total_closed / total_calls * 100) if total_calls > 0 else 0
    
    return {
        "total_sent": total_sent,
        "total_replied": total_replied,
        "total_calls_booked": total_calls,
        "total_closed": total_closed,
        "reply_rate": round(reply_rate, 2),
        "call_booking_rate": round(call_rate, 2),
        "close_rate": round(close_rate, 2)
    }


# ============================================
# EXPORT ENDPOINTS
# ============================================
@app.get("/api/export/csv")
def export_to_csv(
    priority_min: int = 7,
    country: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Export leads to CSV format
    """
    import pandas as pd
    from fastapi.responses import StreamingResponse
    import io
    
    query = db.query(models.Lead).filter(
        models.Lead.priority_score >= priority_min
    )
    
    if country:
        query = query.filter(models.Lead.country == country)
    
    leads = query.all()
    
    # Convert to DataFrame
    data = [{
        'name': lead.name,
        'category': lead.category,
        'city': lead.city,
        'country': lead.country,
        'phone': lead.phone,
        'email': lead.email,
        'website': lead.website,
        'rating': lead.rating,
        'reviews': lead.total_reviews,
        'priority': lead.priority_score,
        'instagram': lead.instagram_handle,
        'linkedin': lead.linkedin_url
    } for lead in leads]
    
    df = pd.DataFrame(data)
    
    # Create CSV in memory
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    stream.seek(0)
    
    return StreamingResponse(
        iter([stream.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=voxmill_leads.csv"}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
