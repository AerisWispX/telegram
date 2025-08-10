import os
import json
import logging
import asyncio
import threading
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import schedule

from sofascore_fetcher import SofaScoreFetcher

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="SofaScore Match API",
    description="API for fetching and serving football match data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data directory
DATA_DIR = Path("/app/data")
DATA_DIR.mkdir(exist_ok=True)

# File paths
LIVE_MATCHES_FILE = DATA_DIR / "live_matches.json"
SCHEDULED_MATCHES_FILE = DATA_DIR / "scheduled_matches.json"
LAST_UPDATE_FILE = DATA_DIR / "last_update.json"

# Initialize fetcher with production-friendly settings
fetcher = SofaScoreFetcher(max_retries=2, base_delay=2)

# Global variables for scheduler
scheduler_running = False
last_successful_fetch = None

def save_json(data, filepath):
    """Save data to JSON file with error handling"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"📁 Data saved to {filepath}")
        return True
    except Exception as e:
        logger.error(f"💥 Failed to save data to {filepath}: {str(e)}")
        return False

def load_json(filepath, default=None):
    """Load data from JSON file with fallback"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            logger.info(f"📖 Loaded data from {filepath}")
            return data
    except FileNotFoundError:
        logger.info(f"📄 File not found: {filepath}, using default")
        return default or {"matches": [], "lastUpdate": None, "count": 0}
    except Exception as e:
        logger.warning(f"⚠️ Failed to load {filepath}: {str(e)}")
        return default or {"matches": [], "lastUpdate": None, "count": 0}

def update_last_fetch_time(success=True):
    """Update last fetch timestamp"""
    global last_successful_fetch
    
    timestamp_data = {
        "lastUpdate": datetime.now().isoformat(),
        "timestamp": datetime.now().timestamp(),
        "success": success
    }
    
    if success:
        last_successful_fetch = datetime.now()
        timestamp_data["lastSuccessfulFetch"] = last_successful_fetch.isoformat()
    
    save_json(timestamp_data, LAST_UPDATE_FILE)

async def fetch_and_store_data():
    """Fetch data from SofaScore and store in JSON files"""
    try:
        logger.info("🚀 Starting data fetch...")
        success = False
        
        # Fetch live matches
        try:
            live_matches = fetcher.process_live_matches()
            if live_matches is not None:
                live_data = {
                    "matches": live_matches,
                    "lastUpdate": datetime.now().isoformat(),
                    "count": len(live_matches),
                    "status": "success"
                }
                save_json(live_data, LIVE_MATCHES_FILE)
                logger.info(f"✅ Updated live matches: {len(live_matches)} matches")
                success = True
            else:
                logger.warning("⚠️ No live matches data received")
        except Exception as e:
            logger.error(f"❌ Error fetching live matches: {str(e)}")
        
        # Fetch scheduled matches for today and tomorrow
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
            
            scheduled_today = fetcher.process_scheduled_matches(today)
            scheduled_tomorrow = fetcher.process_scheduled_matches(tomorrow)
            
            all_scheduled = []
            if scheduled_today:
                all_scheduled.extend(scheduled_today)
            if scheduled_tomorrow:
                all_scheduled.extend(scheduled_tomorrow)
            
            if all_scheduled:
                scheduled_data = {
                    "matches": all_scheduled,
                    "lastUpdate": datetime.now().isoformat(),
                    "count": len(all_scheduled),
                    "status": "success"
                }
                save_json(scheduled_data, SCHEDULED_MATCHES_FILE)
                logger.info(f"✅ Updated scheduled matches: {len(all_scheduled)} matches")
                success = True
            else:
                logger.warning("⚠️ No scheduled matches data received")
        except Exception as e:
            logger.error(f"❌ Error fetching scheduled matches: {str(e)}")
        
        # Update last fetch time
        update_last_fetch_time(success)
        
        if success:
            logger.info("🎉 Data fetch completed successfully")
        else:
            logger.warning("⚠️ Data fetch completed with warnings")
        
        return success
        
    except Exception as e:
        logger.error(f"💥 Critical error in fetch_and_store_data: {str(e)}")
        update_last_fetch_time(False)
        return False

def scheduled_fetch():
    """Wrapper for scheduled fetching"""
    try:
        # Run async function in thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(fetch_and_store_data())
        loop.close()
    except Exception as e:
        logger.error(f"💥 Error in scheduled fetch: {str(e)}")

def run_scheduler():
    """Run the scheduler in a separate thread"""
    global scheduler_running
    scheduler_running = True
    
    # Schedule fetches every 3 minutes
    schedule.every(3).minutes.do(scheduled_fetch)
    
    logger.info("⏰ Scheduler started - fetching every 3 minutes")
    
    while scheduler_running:
        try:
            schedule.run_pending()
            import time
            time.sleep(30)  # Check every 30 seconds
        except Exception as e:
            logger.error(f"💥 Scheduler error: {str(e)}")
            import time
            time.sleep(60)  # Wait 1 minute on error

# API Endpoints

@app.on_event("startup")
async def startup_event():
    """Initialize data on startup"""
    logger.info("🚀 Starting SofaScore API server...")
    
    # Start scheduler in background thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Initial data fetch
    await fetch_and_store_data()
    logger.info("✅ Server startup completed")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global scheduler_running
    scheduler_running = False
    logger.info("🛑 Server shutting down...")

@app.get("/")
async def root():
    """Root endpoint with API information"""
    last_update_data = load_json(LAST_UPDATE_FILE)
    
    return {
        "message": "SofaScore Match API",
        "version": "1.0.0",
        "status": "running",
        "lastUpdate": last_update_data.get("lastUpdate", "Never"),
        "lastSuccessfulFetch": last_update_data.get("lastSuccessfulFetch", "Never"),
        "endpoints": {
            "live": "/api/livescores",
            "scheduled": "/api/scheduled", 
            "refresh": "/api/refresh",
            "status": "/api/status",
            "docs": "/docs"
        }
    }

@app.get("/api/livescores")
async def get_live_scores():
    """Get live match scores - compatible with your existing API"""
    try:
        data = load_json(LIVE_MATCHES_FILE)
        
        # Ensure the response format matches your current API
        return JSONResponse(content={
            "matches": data.get("matches", []),
            "lastUpdate": data.get("lastUpdate"),
            "count": data.get("count", 0)
        })
    except Exception as e:
        logger.error(f"💥 Error serving live scores: {str(e)}")
        
        # Return empty but valid response on error
        return JSONResponse(content={
            "matches": [],
            "lastUpdate": None,
            "count": 0,
            "error": "Failed to load live scores"
        })

@app.get("/api/scheduled")
async def get_scheduled_matches():
    """Get scheduled matches - compatible with your existing API"""
    try:
        data = load_json(SCHEDULED_MATCHES_FILE)
        
        return JSONResponse(content={
            "matches": data.get("matches", []),
            "lastUpdate": data.get("lastUpdate"),
            "count": data.get("count", 0)
        })
    except Exception as e:
        logger.error(f"💥 Error serving scheduled matches: {str(e)}")
        
        return JSONResponse(content={
            "matches": [],
            "lastUpdate": None,
            "count": 0,
            "error": "Failed to load scheduled matches"
        })

@app.post("/api/refresh")
async def refresh_data(background_tasks: BackgroundTasks):
    """Manually refresh match data"""
    background_tasks.add_task(fetch_and_store_data)
    return {
        "message": "Data refresh initiated", 
        "status": "processing",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/status")
async def get_status():
    """Get API status and statistics"""
    try:
        last_update_data = load_json(LAST_UPDATE_FILE)
        live_data = load_json(LIVE_MATCHES_FILE)
        scheduled_data = load_json(SCHEDULED_MATCHES_FILE)
        
        # Calculate uptime
        global last_successful_fetch
        minutes_since_last_fetch = "Never"
        if last_successful_fetch:
            delta = datetime.now() - last_successful_fetch
            minutes_since_last_fetch = int(delta.total_seconds() / 60)
        
        return {
            "status": "healthy" if scheduler_running else "degraded",
            "lastUpdate": last_update_data.get("lastUpdate", "Never"),
            "lastSuccessfulFetch": last_update_data.get("lastSuccessfulFetch", "Never"),
            "minutesSinceLastFetch": minutes_since_last_fetch,
            "schedulerRunning": scheduler_running,
            "statistics": {
                "liveMatches": len(live_data.get("matches", [])),
                "scheduledMatches": len(scheduled_data.get("matches", [])),
                "totalMatches": len(live_data.get("matches", [])) + len(scheduled_data.get("matches", []))
            },
            "dataFiles": {
                "liveExists": LIVE_MATCHES_FILE.exists(),
                "scheduledExists": SCHEDULED_MATCHES_FILE.exists(),
                "lastUpdateExists": LAST_UPDATE_FILE.exists()
            },
            "system": {
                "dataDirectory": str(DATA_DIR),
                "fetchInterval": "3 minutes"
            }
        }
    except Exception as e:
        logger.error(f"💥 Error getting status: {str(e)}")
        return {
            "status": "error", 
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/api/match/{match_id}")
async def get_match_details(match_id: str):
    """Get detailed information for a specific match"""
    try:
        match_details = fetcher.get_match_details(match_id)
        if match_details:
            return JSONResponse(content=match_details)
        else:
            raise HTTPException(status_code=404, detail=f"Match {match_id} not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"💥 Error getting match details for {match_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching match details")

@app.get("/api/match/{match_id}/incidents")
async def get_match_incidents(match_id: str):
    """Get incidents for a specific match"""
    try:
        incidents = fetcher.get_match_incidents(match_id)
        if incidents:
            return JSONResponse(content=incidents)
        else:
            raise HTTPException(status_code=404, detail=f"Match incidents for {match_id} not found")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"💥 Error getting match incidents for {match_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching match incidents")

# Health check endpoint for Coolify/Docker
@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    try:
        # Check if we have recent data
        last_update_data = load_json(LAST_UPDATE_FILE)
        last_update_str = last_update_data.get("lastUpdate")
        
        status = "healthy"
        if last_update_str:
            last_update = datetime.fromisoformat(last_update_str.replace('Z', '+00:00'))
            minutes_since_update = (datetime.now() - last_update.replace(tzinfo=None)).total_seconds() / 60
            
            if minutes_since_update > 10:  # No update in 10 minutes
                status = "degraded"
        
        return {
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "lastUpdate": last_update_str,
            "schedulerRunning": scheduler_running
        }
    except Exception as e:
        logger.error(f"💥 Health check error: {str(e)}")
        return {
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

# Add a simple metrics endpoint
@app.get("/metrics")
async def get_metrics():
    """Simple metrics endpoint"""
    try:
        live_data = load_json(LIVE_MATCHES_FILE)
        scheduled_data = load_json(SCHEDULED_MATCHES_FILE)
        last_update_data = load_json(LAST_UPDATE_FILE)
        
        return {
            "live_matches_total": len(live_data.get("matches", [])),
            "scheduled_matches_total": len(scheduled_data.get("matches", [])),
            "last_update_timestamp": last_update_data.get("timestamp", 0),
            "scheduler_running": 1 if scheduler_running else 0
        }
    except Exception:
        return {
            "live_matches_total": 0,
            "scheduled_matches_total": 0,
            "last_update_timestamp": 0,
            "scheduler_running": 0
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        log_level="info",
        access_log=True
    )
