import requests
import json
import time
import random
import os
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sofascore.log'),
        logging.StreamHandler()
    ]
)

class SofaScoreFetcher:
    def __init__(self, data_dir="data"):
        self.session = requests.Session()
        self.base_url = "https://api.sofascore.com/api/v1"
        self.data_dir = data_dir
        
        # Create data directory if it doesn't exist
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Headers to mimic a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'Referer': 'https://www.sofascore.com/',
            'Origin': 'https://www.sofascore.com',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        self.session.headers.update(self.headers)
        self.logger = logging.getLogger(__name__)
        
    def _make_request(self, url: str, max_retries: int = 3) -> Optional[Dict]:
        """Make HTTP request with retry logic and random delays"""
        for attempt in range(max_retries):
            try:
                # Random delay to avoid rate limiting
                time.sleep(random.uniform(1, 3))
                
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 200:
                    self.logger.info(f"Successfully fetched: {url}")
                    return response.json()
                elif response.status_code == 403:
                    self.logger.warning(f"403 Forbidden for {url}, attempt {attempt + 1}")
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(5, 10))
                elif response.status_code == 429:
                    self.logger.warning(f"Rate limited for {url}, waiting...")
                    time.sleep(random.uniform(10, 20))
                else:
                    self.logger.error(f"HTTP {response.status_code} for {url}")
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed for {url}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(2, 5))
                    
        return None
    
    def get_live_matches(self) -> Optional[Dict]:
        """Fetch live football matches"""
        url = f"{self.base_url}/sport/football/events/live"
        return self._make_request(url)
    
    def get_scheduled_matches(self, date: str = None) -> Optional[Dict]:
        """Fetch scheduled matches for a specific date"""
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
        
        url = f"{self.base_url}/sport/football/scheduled-events/{date}"
        return self._make_request(url)
    
    def get_match_details(self, event_id: str) -> Optional[Dict]:
        """Fetch detailed information for a specific match"""
        url = f"{self.base_url}/event/{event_id}"
        return self._make_request(url)
    
    def get_match_incidents(self, event_id: str) -> Optional[Dict]:
        """Fetch match incidents (goals, cards, etc.) for a specific match"""
        url = f"{self.base_url}/event/{event_id}/incidents"
        return self._make_request(url)
    
    def process_live_matches(self) -> List[Dict]:
        """Process live matches into simplified format"""
        live_data = self.get_live_matches()
        if not live_data or 'events' not in live_data:
            return []
        
        processed_matches = []
        
        for event in live_data['events']:
            try:
                incidents = self.get_match_incidents(str(event['id']))
                
                match_data = {
                    'id': event['id'],
                    'home': event['homeTeam']['name'],
                    'away': event['awayTeam']['name'],
                    'homeScore': event.get('homeScore', {}).get('current', 0),
                    'awayScore': event.get('awayScore', {}).get('current', 0),
                    'status': event.get('status', {}).get('description', 'Unknown'),
                    'currentTime': None,
                    'addedTime': None,
                    'homeScorers': [],
                    'awayScorers': [],
                    'isLive': True,
                    'tournament': event.get('tournament', {}).get('name', ''),
                    'startTime': event.get('startTimestamp', 0),
                    'lastUpdated': int(time.time())
                }
                
                if incidents and 'incidents' in incidents:
                    for incident in incidents['incidents']:
                        if incident.get('incidentType') == 'goal':
                            scorer_info = {
                                'name': incident.get('player', {}).get('name', 'Unknown'),
                                'minute': incident.get('time', 0)
                            }
                            
                            if incident.get('isHome', False):
                                match_data['homeScorers'].append(scorer_info)
                            else:
                                match_data['awayScorers'].append(scorer_info)
                        
                        if 'time' in incident and incident.get('time'):
                            match_data['currentTime'] = incident['time']
                        if 'addedTime' in incident:
                            match_data['addedTime'] = incident['addedTime']
                
                processed_matches.append(match_data)
                
            except Exception as e:
                self.logger.error(f"Error processing match {event.get('id', 'unknown')}: {str(e)}")
                continue
        
        return processed_matches
    
    def process_scheduled_matches(self, date: str = None) -> List[Dict]:
        """Process scheduled matches into simplified format"""
        scheduled_data = self.get_scheduled_matches(date)
        if not scheduled_data or 'events' not in scheduled_data:
            return []
        
        processed_matches = []
        
        for event in scheduled_data['events']:
            try:
                match_data = {
                    'id': event['id'],
                    'home': event['homeTeam']['name'],
                    'away': event['awayTeam']['name'],
                    'homeScore': 0,
                    'awayScore': 0,
                    'status': event.get('status', {}).get('description', 'Scheduled'),
                    'isLive': False,
                    'tournament': event.get('tournament', {}).get('name', ''),
                    'startTime': event.get('startTimestamp', 0),
                    'timestamp': event.get('startTimestamp', 0),
                    'lastUpdated': int(time.time())
                }
                
                processed_matches.append(match_data)
                
            except Exception as e:
                self.logger.error(f"Error processing scheduled match {event.get('id', 'unknown')}: {str(e)}")
                continue
        
        return processed_matches
    
    def save_to_json(self, data: Dict, filename: str):
        """Save data to JSON file in data directory"""
        try:
            filepath = os.path.join(self.data_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Data saved to {filepath}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to save data to {filename}: {str(e)}")
            return False
    
    def load_from_json(self, filename: str) -> Optional[Dict]:
        """Load data from JSON file in data directory"""
        try:
            filepath = os.path.join(self.data_dir, filename)
            if os.path.exists(filepath):
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load data from {filename}: {str(e)}")
        return None
    
    def fetch_and_store_all_data(self):
        """Fetch and store all data periodically"""
        self.logger.info("🔄 Starting data fetch cycle...")
        
        # Fetch live matches
        live_matches = self.process_live_matches()
        if live_matches:
            data = {
                'matches': live_matches,
                'lastUpdated': int(time.time()),
                'count': len(live_matches)
            }
            self.save_to_json(data, 'live_matches.json')
            self.logger.info(f"✅ Saved {len(live_matches)} live matches")
        
        # Fetch scheduled matches for today and tomorrow
        today = datetime.now().strftime('%Y-%m-%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        
        for date, label in [(today, 'today'), (tomorrow, 'tomorrow')]:
            scheduled_matches = self.process_scheduled_matches(date)
            if scheduled_matches:
                data = {
                    'matches': scheduled_matches,
                    'lastUpdated': int(time.time()),
                    'count': len(scheduled_matches),
                    'date': date
                }
                filename = f'scheduled_matches_{label}.json'
                self.save_to_json(data, filename)
                self.logger.info(f"✅ Saved {len(scheduled_matches)} scheduled matches for {label}")
        
        self.logger.info("✨ Data fetch cycle completed")


# Flask Application
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes
fetcher = SofaScoreFetcher()

@app.route('/')
def index():
    """API Information endpoint"""
    return jsonify({
        'service': 'SofaScore API Fetcher',
        'version': '1.0.0',
        'endpoints': {
            '/api/live': 'Get live matches',
            '/api/scheduled': 'Get scheduled matches for today',
            '/api/scheduled/tomorrow': 'Get scheduled matches for tomorrow',
            '/api/match/<match_id>': 'Get specific match details',
            '/api/status': 'Get service status',
            '/health': 'Health check endpoint'
        },
        'status': 'running',
        'timestamp': int(time.time())
    })

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': int(time.time()),
        'service': 'sofascore-fetcher'
    })

@app.route('/api/live')
def get_live_matches():
    """Get live matches from stored data or fetch new if old"""
    data = fetcher.load_from_json('live_matches.json')
    
    # If no data or data is older than 2 minutes, fetch new
    if not data or (int(time.time()) - data.get('lastUpdated', 0)) > 120:
        live_matches = fetcher.process_live_matches()
        if live_matches:
            data = {
                'matches': live_matches,
                'lastUpdated': int(time.time()),
                'count': len(live_matches)
            }
            fetcher.save_to_json(data, 'live_matches.json')
        else:
            # Return old data if available
            if data:
                data['warning'] = 'Using cached data - API unavailable'
            else:
                return jsonify({'error': 'No live matches available', 'matches': []}), 503
    
    return jsonify(data)

@app.route('/api/scheduled')
def get_scheduled_matches():
    """Get today's scheduled matches"""
    data = fetcher.load_from_json('scheduled_matches_today.json')
    
    # If no data or data is older than 1 hour, fetch new
    if not data or (int(time.time()) - data.get('lastUpdated', 0)) > 3600:
        today = datetime.now().strftime('%Y-%m-%d')
        scheduled_matches = fetcher.process_scheduled_matches(today)
        if scheduled_matches:
            data = {
                'matches': scheduled_matches,
                'lastUpdated': int(time.time()),
                'count': len(scheduled_matches),
                'date': today
            }
            fetcher.save_to_json(data, 'scheduled_matches_today.json')
        else:
            if data:
                data['warning'] = 'Using cached data - API unavailable'
            else:
                return jsonify({'error': 'No scheduled matches available', 'matches': []}), 503
    
    return jsonify(data)

@app.route('/api/scheduled/tomorrow')
def get_scheduled_matches_tomorrow():
    """Get tomorrow's scheduled matches"""
    data = fetcher.load_from_json('scheduled_matches_tomorrow.json')
    
    # If no data or data is older than 1 hour, fetch new
    if not data or (int(time.time()) - data.get('lastUpdated', 0)) > 3600:
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        scheduled_matches = fetcher.process_scheduled_matches(tomorrow)
        if scheduled_matches:
            data = {
                'matches': scheduled_matches,
                'lastUpdated': int(time.time()),
                'count': len(scheduled_matches),
                'date': tomorrow
            }
            fetcher.save_to_json(data, 'scheduled_matches_tomorrow.json')
        else:
            if data:
                data['warning'] = 'Using cached data - API unavailable'
            else:
                return jsonify({'error': 'No scheduled matches available', 'matches': []}), 503
    
    return jsonify(data)

@app.route('/api/match/<int:match_id>')
def get_match_details(match_id):
    """Get detailed information for a specific match"""
    try:
        # Try to load from cache first
        filename = f'match_{match_id}_details.json'
        data = fetcher.load_from_json(filename)
        
        # If no cached data or data is older than 5 minutes, fetch new
        if not data or (int(time.time()) - data.get('lastUpdated', 0)) > 300:
            match_details = fetcher.get_match_details(str(match_id))
            if match_details:
                data = {
                    'match': match_details,
                    'lastUpdated': int(time.time())
                }
                fetcher.save_to_json(data, filename)
            else:
                if data:
                    data['warning'] = 'Using cached data - API unavailable'
                else:
                    return jsonify({'error': f'Match {match_id} not found'}), 404
        
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch match details: {str(e)}'}), 500

@app.route('/api/status')
def get_status():
    """Get service status and statistics"""
    live_data = fetcher.load_from_json('live_matches.json')
    today_data = fetcher.load_from_json('scheduled_matches_today.json')
    
    return jsonify({
        'service': 'SofaScore Fetcher',
        'status': 'running',
        'timestamp': int(time.time()),
        'data_status': {
            'live_matches': {
                'available': live_data is not None,
                'count': live_data.get('count', 0) if live_data else 0,
                'last_updated': live_data.get('lastUpdated', 0) if live_data else 0
            },
            'scheduled_matches': {
                'available': today_data is not None,
                'count': today_data.get('count', 0) if today_data else 0,
                'last_updated': today_data.get('lastUpdated', 0) if today_data else 0
            }
        }
    })

@app.route('/api/refresh')
def force_refresh():
    """Force refresh all data"""
    try:
        fetcher.fetch_and_store_all_data()
        return jsonify({
            'message': 'Data refresh completed',
            'timestamp': int(time.time())
        })
    except Exception as e:
        return jsonify({
            'error': f'Refresh failed: {str(e)}'
        }), 500

# Background scheduler setup
scheduler = BackgroundScheduler()
scheduler.start()

# Schedule data fetching every 2 minutes for live matches
scheduler.add_job(
    func=fetcher.fetch_and_store_all_data,
    trigger=IntervalTrigger(minutes=2),
    id='fetch_data_job',
    name='Fetch SofaScore data',
    replace_existing=True
)

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    # Initial data fetch
    fetcher.fetch_and_store_all_data()
    
    # Run the Flask app
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)
