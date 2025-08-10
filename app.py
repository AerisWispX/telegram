import requests
import json
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

class SofaScoreFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = "https://api.sofascore.com/api/v1"
        
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
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
                        # Longer delay on 403
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
        """
        Fetch scheduled matches for a specific date
        date format: YYYY-MM-DD (default: today)
        """
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
    
    def get_match_lineups(self, event_id: str) -> Optional[Dict]:
        """Fetch match lineups"""
        url = f"{self.base_url}/event/{event_id}/lineups"
        return self._make_request(url)
    
    def get_match_statistics(self, event_id: str) -> Optional[Dict]:
        """Fetch match statistics"""
        url = f"{self.base_url}/event/{event_id}/statistics"
        return self._make_request(url)
    
    def process_live_matches(self) -> List[Dict]:
        """Process live matches into simplified format"""
        live_data = self.get_live_matches()
        if not live_data or 'events' not in live_data:
            return []
        
        processed_matches = []
        
        for event in live_data['events']:
            try:
                # Get incidents for live match to get current score and scorers
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
                    'startTime': event.get('startTimestamp', 0)
                }
                
                # Process incidents to get scorers and current time
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
                        
                        # Get current match time from latest incident
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
                    'timestamp': event.get('startTimestamp', 0)
                }
                
                processed_matches.append(match_data)
                
            except Exception as e:
                self.logger.error(f"Error processing scheduled match {event.get('id', 'unknown')}: {str(e)}")
                continue
        
        return processed_matches
    
    def save_to_json(self, data: Dict, filename: str):
        """Save data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Data saved to {filename}")
        except Exception as e:
            self.logger.error(f"Failed to save data to {filename}: {str(e)}")


def main():
    """Main function to demonstrate usage"""
    fetcher = SofaScoreFetcher()
    
    print("🚀 Starting SofaScore data fetch...")
    
    # Fetch live matches
    print("\n📺 Fetching live matches...")
    live_matches = fetcher.process_live_matches()
    if live_matches:
        print(f"✅ Found {len(live_matches)} live matches")
        fetcher.save_to_json({'matches': live_matches}, 'live_matches.json')
        
        # Display first few live matches
        for i, match in enumerate(live_matches[:3]):
            print(f"   {match['home']} {match['homeScore']}-{match['awayScore']} {match['away']} ({match['status']})")
    else:
        print("❌ No live matches found or failed to fetch")
    
    # Fetch scheduled matches for today
    print("\n📅 Fetching today's scheduled matches...")
    today = datetime.now().strftime('%Y-%m-%d')
    scheduled_matches = fetcher.process_scheduled_matches(today)
    if scheduled_matches:
        print(f"✅ Found {len(scheduled_matches)} scheduled matches for {today}")
        fetcher.save_to_json({'matches': scheduled_matches}, 'scheduled_matches.json')
        
        # Display first few scheduled matches
        for i, match in enumerate(scheduled_matches[:3]):
            start_time = datetime.fromtimestamp(match['timestamp']).strftime('%H:%M')
            print(f"   {start_time} - {match['home']} vs {match['away']} ({match['tournament']})")
    else:
        print("❌ No scheduled matches found or failed to fetch")
    
    # Fetch specific match details (example)
    print("\n🔍 Fetching specific match details...")
    if live_matches:
        sample_match_id = live_matches[0]['id']
        match_details = fetcher.get_match_details(str(sample_match_id))
        if match_details:
            print(f"✅ Fetched details for match ID {sample_match_id}")
            fetcher.save_to_json(match_details, f'match_{sample_match_id}_details.json')
    
    print("\n🎉 Data fetch completed!")
    print("\nGenerated files:")
    print("- live_matches.json (processed live matches)")
    print("- scheduled_matches.json (processed scheduled matches)")
    if live_matches:
        print(f"- match_{live_matches[0]['id']}_details.json (sample match details)")


if __name__ == "__main__":
    main()
