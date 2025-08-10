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
    version="1.0.1",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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

# Initialize fetcher with more conservative settings
fetcher = SofaScoreFetcher(max_retries=2, base_delay=3)

# Global variables
scheduler_running = False
last_successful_fetch = None
fetch_failures = 0
consecutive_failures = 0

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
    """Update last fetch timestamp with enhanced tracking"""
    global last_successful_fetch, fetch_failures, consecutive_failures
    
    timestamp_data = {
        "lastUpdate": datetime.now().isoformat(),
        "timestamp": datetime.now().timestamp(),
        "success": success,
        "totalFailures": fetch_failures,
        "consecutiveFailures": consecutive_failures
    }
    
    if success:
        last_successful_fetch = datetime.now()
        consecutive_failures = 0
        timestamp_data["lastSuccessfulFetch"] = last_successful_fetch.isoformat()
        logger.info("✅ Successful fetch recorded")
    else:
        fetch_failures += 1
        consecutive_failures += 1
        logger.warning(f"❌ Fetch failure recorded (consecutive: {consecutive_failures})")
        
        # Reset failed proxies after multiple consecutive failures
        if consecutive_failures >= 3:
            logger.info("🔄 Resetting failed proxies due to consecutive failures")
            fetcher.reset_failed_proxies()
    
    save_json(timestamp_data, LAST_UPDATE_FILE)

async def fetch_and_store_data():
    """Enhanced data fetching with better error recovery"""
    try:
        logger.info("🚀 Starting data fetch...")
        success = False
        partial_success = False
        
        # Check proxy status before starting
        proxy_status = fetcher.get_proxy_status()
        logger.info(f"🌐 Proxy status: {proxy_status['available_proxies']}/{proxy_status['total_proxies']} available")
        
        # If too many proxies failed, reset them
        if proxy_status['available_proxies'] < 3:
            logger.info("🔄 Low proxy availability, resetting failed proxies")
            fetcher.reset_failed_proxies()
        
        # Fetch live matches with retry
        live_matches_data = None
        try:
            logger.info("📺 Fetching live matches...")
            live_matches = fetcher.process_live_matches()
            
            if live_matches is not None:
                live_matches_data = {
                    "matches": live_matches,
                    "lastUpdate": datetime.now().isoformat(),
                    "count": len(live_matches),
                    "status": "success"
                }
                save_json(live_matches_data, LIVE_MATCHES_FILE)
                logger.info(f"✅ Live matches: {len(live_matches)} matches")
                success = True
                partial_success = True
            else:
                logger.warning("⚠️ No live matches data received")
                # Keep existing data but mark as stale
                existing_data = load_json(LIVE_MATCHES_FILE)
                if existing_data.get("matches"):
                    existing_data["status"] = "stale"
                    existing_data["lastAttempt"] = datetime.now().isoformat()
                    save_json(existing_data, LIVE_MATCHES_FILE)
                
        except Exception as e:
            logger.error(f"❌ Error fetching live matches: {str(e)}")
        
        # Add delay between requests
        await asyncio.sleep(5)
        
        # Fetch scheduled matches with enhanced error handling
        try:
            logger.info("📅 Fetching scheduled matches...")
            today = datetime.now().strftime('%Y-%m-%d')
            tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
            
            all_scheduled = []
            
            # Try today's matches
            try:
                scheduled_today = fetcher.process_scheduled_matches(today)
                if scheduled_today:
                    all_scheduled.extend(scheduled_today)
                    logger.info(f"✅ Today's matches: {len(scheduled_today)}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to fetch today's matches: {str(e)}")
            
            # Add delay before next request
            await asyncio.sleep(3)
            
            # Try tomorrow's matches
            try:
                scheduled_tomorrow = fetcher.process_scheduled_matches(tomorrow)
                if scheduled_tomorrow:
                    all_scheduled.extend(scheduled_tomorrow)
                    logger.info(f"✅ Tomorrow's matches: {len(scheduled_tomorrow)}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to fetch tomorrow's matches: {str(e)}")
            
            if all_scheduled:
                scheduled_data = {
                    "matches": all_scheduled,
                    "lastUpdate": datetime.now().isoformat(),
                    "count": len(all_scheduled),
                    "status": "success"
                }
                save_json(scheduled_data, SCHEDULED_MATCHES_FILE)
                logger.info(f"✅ Scheduled matches: {len(all_scheduled)} total")
                success = True
                partial_success = True
            else:
                logger.warning("⚠️ No scheduled matches data received")
                # Keep existing data but mark as stale
                existing_data = load_json(SCHEDULED_MATCHES_FILE)
                if existing_data.get("matches"):
                    existing_data["status"] = "stale"
                    existing_data["lastAttempt"] = datetime.now().isoformat()
                    save_json(existing_data, SCHEDULED_MATCHES_FILE)
                
        except Exception as e:
            logger.error(f"❌ Error fetching scheduled matches: {str(e)}")
        
        # Update fetch status
        update_last_fetch_time(success or partial_success)
        
        # Log final proxy status
        final_proxy_status = fetcher.get_proxy_status()
        logger.info(f"🌐 Final proxy status: {final_proxy_status['success_rate']}")
        
        if success:
            logger.info("🎉 Data fetch completed successfully")
        elif partial_success:
            logger.info("⚠️ Data fetch partially successful")
        else:
            logger.error("❌ Data fetch failed completely")
        
        return success or partial_success
        
    except Exception as e:
        logger.error(f"💥 Critical error in fetch_and_store_data: {str(e)}")
        update_last_fetch_time(False)
        return False

def scheduled_fetch():
    """Wrapper for scheduled fetching with enhanced error handling"""
    try:
        logger.info("⏰ Running scheduled fetch...")
        
        # Check if we should skip this fetch due to too many consecutive failures
        global consecutive_failures
        if consecutive_failures >= 5:
            logger.warning(f"🛑 Skipping fetch due to {consecutive_failures} consecutive failures")
            
            # Reset after waiting longer
            if consecutive_failures >= 10:
                logger.info("🔄 Resetting failure count after extended wait")
                consecutive_failures = 0
                fetcher.reset_failed_proxies()
            
            return
        
        # Run async function in thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        success = loop.run_until_complete(fetch_and_store_data())
        loop.close()
        
        if not success:
            logger.warning("⚠️ Scheduled fetch failed")
        
    except Exception as e:
        logger.error(f"💥 Error in scheduled fetch: {str(e)}")

def run_scheduler():
    """Run the scheduler with dynamic intervals based on success rate"""
    global scheduler_running
    scheduler_running = True
    
    # Start with longer intervals due to API issues
    schedule.every(5).minutes.do(scheduled_fetch)
    
    logger.info("⏰ Scheduler started - fetching every 5 minutes")
    
    while scheduler_running:
        try:
            schedule.run_pending()
            
            # Adjust schedule based on consecutive failures
            if consecutive_failures >= 3:
                # Increase interval if we're having problems
                schedule.clear()
                schedule.every(10).minutes.do(scheduled_fetch)
                logger.info("📈 Increased fetch interval to 10 minutes due to failures")
            elif consecutive_failures == 0:
                # Reset to normal interval if everything is working
                schedule.clear()
                schedule.every(5).minutes.do(scheduled_fetch)
            
            time.sleep(30)  # Check every 30 seconds
            
        except Exception as e:
            logger.error(f"💥 Scheduler error: {str(e)}")
            time.sleep(60)

# API Endpoints with enhanced error handling

@app.on_event("startup")
async def startup_event():
    """Initialize data on startup"""
    logger.info("🚀 Starting SofaScore API server...")
    
    # Start scheduler in background thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Initial data fetch (but don't fail startup if it fails)
    try:
        await fetch_and_store_data()
        logger.info("✅ Initial data fetch completed")
    except Exception as e:
        logger.warning(f"⚠️ Initial data fetch failed: {str(e)}")
    
    logger.info("✅ Server startup completed")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global scheduler_running
    scheduler_running = False
    logger.info("🛑 Server shutting down...")

@app.get("/")
async def root():
    """Root endpoint with enhanced API information"""
    last_update_data = load_json(LAST_UPDATE_FILE)
    
    return {
        "message": "SofaScore Match API",
        "version": "1.0.1",
        "status": "running",
        "lastUpdate": last_update_data.get("lastUpdate", "Never"),
        "lastSuccessfulFetch": last_update_data.get("lastSuccessfulFetch", "Never"),
        "totalFailures": last_update_data.get("totalFailures", 0),
        "consecutiveFailures": last_update_data.get("consecutiveFailures", 0),
        "proxyStatus": fetcher.get_proxy_status(),
        "endpoints": {
            "live": "/api/livescores",
            "scheduled": "/api/scheduled", 
            "refresh": "/api/refresh",
            "status": "/api/status",
            "proxy-status": "/api/proxy-status",
            "docs": "/docs"
        }
    }

@app.get("/api/livescores")
async def get_live_scores():
    """Get live match scores with enhanced error handling"""
    try:
        data = load_json(LIVE_MATCHES_FILE)
        
        # Add data freshness information
        last_update = data.get("lastUpdate")
        is_stale = False
        age_minutes = None
        
        if last_update:
            try:
                update_time = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                age_minutes = (datetime.now() - update_time.replace(tzinfo=None)).total_seconds() / 60
                is_stale = age_minutes > 15  # Mark as stale if older than 15 minutes
            except:
                pass
        
        return JSONResponse(content={
            "matches": data.get("matches", []),
            "lastUpdate": last_update,
            "count": data.get("count", 0),
            "status": data.get("status", "unknown"),
            "dataAge": {
                "minutes": int(age_minutes) if age_minutes else None,
                "isStale": is_stale
            }
        })
        
    except Exception as e:
        logger.error(f"💥 Error serving live scores: {str(e)}")
        
        return JSONResponse(
            status_code=500,
            content={
                "matches": [],
                "lastUpdate": None,
                "count": 0,
                "error": "Failed to load live scores",
                "message": "Service temporarily unavailable"
            }
        )

@app.get("/api/scheduled")
async def get_scheduled_matches():
    """Get scheduled matches with enhanced error handling"""
    try:
        data = load_json(SCHEDULED_MATCHES_FILE)
        
        # Add data freshness information
        last_update = data.get("lastUpdate")
        is_stale = False
        age_minutes = None
        
        if last_update:
            try:
                update_time = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                age_minutes = (datetime.now() - update_time.replace(tzinfo=None)).total_seconds() / 60
                is_stale = age_minutes > 30  # Scheduled matches can be stale for longer
            except:
                pass
        
        return JSONResponse(content={
            "matches": data.get("matches", []),
            "lastUpdate": last_update,
            "count": data.get("count", 0),
            "status": data.get("status", "unknown"),
            "dataAge": {
                "minutes": int(age_minutes) if age_minutes else None,
                "isStale": is_stale
            }
        })
        
    except Exception as e:
        logger.error(f"💥 Error serving scheduled matches: {str(e)}")
        
        return JSONResponse(
            status_code=500,
            content={
                "matches": [],
                "lastUpdate": None,
                "count": 0,
                "error": "Failed to load scheduled matches",
                "message": "Service temporarily unavailable"
            }
        )

@app.post("/api/refresh")
async def refresh_data(background_tasks: BackgroundTasks):
    """Manually refresh match data"""
    global consecutive_failures
    
    # Reset consecutive failures on manual refresh
    consecutive_failures = 0
    fetcher.reset_failed_proxies()
    
    background_tasks.add_task(fetch_and_store_data)
    
    return {
        "message": "Data refresh initiated", 
        "status": "processing",
        "timestamp": datetime.now().isoformat(),
        "note": "Failed proxies have been reset"
    }

@app.get("/api/status")
async def get_status():
    """Get comprehensive API status"""
    try:
        last_update_data = load_json(LAST_UPDATE_FILE)
        live_data = load_json(LIVE_MATCHES_FILE)
        scheduled_data = load_json(SCHEDULED_MATCHES_FILE)
        
        # Calculate uptime and health metrics
        global last_successful_fetch, consecutive_failures
        minutes_since_last_fetch = "Never"
        health_status = "degraded"
        
        if last_successful_fetch:
            delta = datetime.now() - last_successful_fetch
            minutes_since_last_fetch = int(delta.total_seconds() / 60)
            
            if minutes_since_last_fetch < 10:
                health_status = "healthy"
            elif minutes_since_last_fetch < 30:
                health_status = "degraded"
            else:
                health_status = "unhealthy"
        
        # Determine overall health
        if consecutive_failures >= 5:
            health_status = "critical"
        
        proxy_status = fetcher.get_proxy_status()
        
        return {
            "status": health_status,
            "lastUpdate": last_update_data.get("lastUpdate", "Never"),
            "lastSuccessfulFetch": last_update_data.get("lastSuccessfulFetch", "Never"),
            "minutesSinceLastFetch": minutes_since_last_fetch,
            "schedulerRunning": scheduler_running,
            "failures": {
                "total": last_update_data.get("totalFailures", 0),
                "consecutive": last_update_data.get("consecutiveFailures", 0)
            },
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
            "proxy": {
                "available": proxy_status["available_proxies"],
                "total": proxy_status["total_proxies"],
                "failed": proxy_status["failed_proxies"],
                "successRate": proxy_status["success_rate"],
                "current": proxy_status["current_proxy"]
            },
            "system": {
                "dataDirectory": str(DATA_DIR),
                "fetchInterval": "5-10 minutes (adaptive)"
            }
        }
        
    except Exception as e:
        logger.error(f"💥 Error getting status: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error", 
                "message": str(e),
                "timestamp": datetime.now().isoformat()
            }
        )

@app.get("/api/proxy-status")
async def get_proxy_status():
    """Get detailed proxy status information"""
    try:
        proxy_status = fetcher.get_proxy_status()
        
        return {
            "timestamp": datetime.now().isoformat(),
            "proxies": {
                "total": proxy_status["total_proxies"],
                "available": proxy_status["available_proxies"],
                "failed": proxy_status["failed_proxies"],
                "successRate": proxy_status["success_rate"]
            },
            "current": proxy_status["current_proxy"],
            "failedProxies": proxy_status.get("failed_proxy_list", []),
            "actions": {
                "reset": "/api/proxy-reset"
            }
        }
        
    except Exception as e:
        logger.error(f"💥 Error getting proxy status: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "error": "Failed to get proxy status",
                "message": str(e)
            }
        )

@app.post("/api/proxy-reset")
async def reset_proxies():
    """Reset failed proxies list"""
    try:
        old_failed_count = len(fetcher.failed_proxies)
        fetcher.reset_failed_proxies()
        
        global consecutive_failures
        consecutive_failures = 0
        
        return {
            "message": "Proxy list reset successfully",
            "clearedProxies": old_failed_count,
            "timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"💥 Error resetting proxies: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "error": "Failed to reset proxies",
                "message": str(e)
            }
        )

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

# Enhanced health check endpoint
@app.get("/health")
async def health_check():
    """Comprehensive health check endpoint"""
    try:
        last_update_data = load_json(LAST_UPDATE_FILE)
        last_update_str = last_update_data.get("lastUpdate")
        
        status = "healthy"
        issues = []
        
        # Check data freshness
        if last_update_str:
            try:
                last_update = datetime.fromisoformat(last_update_str.replace('Z', '+00:00'))
                minutes_since_update = (datetime.now() - last_update.replace(tzinfo=None)).total_seconds() / 60
                
                if minutes_since_update > 15:
                    status = "degraded"
                    issues.append(f"Data is {int(minutes_since_update)} minutes old")
            except:
                status = "degraded"
                issues.append("Unable to parse last update time")
        else:
            status = "unhealthy"
            issues.append("No update data available")
        
        # Check consecutive failures
        consecutive = last_update_data.get("consecutiveFailures", 0)
        if consecutive >= 5:
            status = "unhealthy"
            issues.append(f"{consecutive} consecutive fetch failures")
        elif consecutive >= 3:
            if status == "healthy":
                status = "degraded"
            issues.append(f"{consecutive} consecutive fetch failures")
        
        # Check proxy availability
        proxy_status = fetcher.get_proxy_status()
        if proxy_status["available_proxies"] < 3:
            if status == "healthy":
                status = "degraded"
            issues.append(f"Only {proxy_status['available_proxies']} proxies available")
        
        return {
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "lastUpdate": last_update_str,
            "schedulerRunning": scheduler_running,
            "issues": issues,
            "consecutiveFailures": consecutive,
            "proxyAvailability": f"{proxy_status['available_proxies']}/{proxy_status['total_proxies']}"
        }
        
    except Exception as e:
        logger.error(f"💥 Health check error: {str(e)}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }
        )

@app.get("/metrics")
async def get_metrics():
    """Enhanced metrics endpoint"""
    try:
        live_data = load_json(LIVE_MATCHES_FILE)
        scheduled_data = load_json(SCHEDULED_MATCHES_FILE)
        last_update_data = load_json(LAST_UPDATE_FILE)
        proxy_status = fetcher.get_proxy_status()
        
        return {
            "live_matches_total": len(live_data.get("matches", [])),
            "scheduled_matches_total": len(scheduled_data.get("matches", [])),
            "last_update_timestamp": last_update_data.get("timestamp", 0),
            "scheduler_running": 1 if scheduler_running else 0,
            "total_failures": last_update_data.get("totalFailures", 0),
            "consecutive_failures": last_update_data.get("consecutiveFailures", 0),
            "available_proxies": proxy_status["available_proxies"],
            "failed_proxies": proxy_status["failed_proxies"]
        }
    except Exception:
        return {
            "live_matches_total": 0,
            "scheduled_matches_total": 0,
            "last_update_timestamp": 0,
            "scheduler_running": 0,
            "total_failures": 0,
            "consecutive_failures": 0,
            "available_proxies": 0,
            "failed_proxies": 0
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
