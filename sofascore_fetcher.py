import requests
import json
import time
import random
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import itertools

class SofaScoreFetcher:
    def __init__(self, max_retries=3, base_delay=1):
        self.base_url = "https://api.sofascore.com/api/v1"
        self.max_retries = max_retries
        self.base_delay = base_delay
        
        # Proxy configuration
        self.proxies = [
            "23.95.150.145:6114:pxtkihuu:ia5j6e2ylokw",
            "198.23.239.134:6540:pxtkihuu:ia5j6e2ylokw",
            "45.38.107.97:6014:pxtkihuu:ia5j6e2ylokw",
            "207.244.217.165:6712:pxtkihuu:ia5j6e2ylokw",
            "107.172.163.27:6543:pxtkihuu:ia5j6e2ylokw",
            "104.222.161.211:6343:pxtkihuu:ia5j6e2ylokw",
            "64.137.96.74:6641:pxtkihuu:ia5j6e2ylokw",
            "216.10.27.159:6837:pxtkihuu:ia5j6e2ylokw",
            "136.0.207.84:6661:pxtkihuu:ia5j6e2ylokw",
            "142.147.128.93:6593:pxtkihuu:ia5j6e2ylokw"
        ]
        
        # Create proxy cycle for rotation
        self.proxy_cycle = itertools.cycle(self.proxies)
        self.current_proxy = None
        self.failed_proxies = set()
        
        # Headers to mimic a real browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
            'Pragma': 'no-cache',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
    def _get_proxy_config(self, proxy_string: str) -> Dict[str, str]:
        """Convert proxy string to requests proxy configuration"""
        try:
            parts = proxy_string.split(':')
            if len(parts) == 4:
                host, port, username, password = parts
                proxy_url = f"http://{username}:{password}@{host}:{port}"
                return {
                    'http': proxy_url,
                    'https': proxy_url
                }
        except Exception as e:
            self.logger.error(f"Error parsing proxy {proxy_string}: {str(e)}")
        return {}
    
    def _get_next_proxy(self) -> Optional[str]:
        """Get next available proxy from the rotation"""
        attempts = 0
        max_attempts = len(self.proxies) * 2  # Allow full cycle twice
        
        while attempts < max_attempts:
            proxy = next(self.proxy_cycle)
            if proxy not in self.failed_proxies:
                return proxy
            attempts += 1
        
        # If all proxies are failed, reset and try again
        if self.failed_proxies:
            self.logger.warning("All proxies failed, resetting failed proxy list")
            self.failed_proxies.clear()
            return next(self.proxy_cycle)
        
        return None
    
    def _create_session(self, proxy_string: str = None) -> requests.Session:
        """Create a new session with proxy configuration"""
        session = requests.Session()
        session.headers.update(self.headers)
        
        if proxy_string:
            proxy_config = self._get_proxy_config(proxy_string)
            if proxy_config:
                session.proxies.update(proxy_config)
                self.logger.info(f"🌐 Using proxy: {proxy_string.split(':')[0]}:{proxy_string.split(':')[1]}")
        
        return session
    
    def _make_request(self, url: str, max_retries: int = None) -> Optional[Dict]:
        """Make HTTP request with proxy rotation and retry logic"""
        max_retries = max_retries or self.max_retries
        
        for attempt in range(max_retries):
            # Get a proxy for this attempt
            proxy = self._get_next_proxy()
            if not proxy:
                self.logger.error("No available proxies")
                break
                
            # Create session with current proxy
            session = self._create_session(proxy)
            self.current_proxy = proxy
            
            try:
                # Random delay to avoid rate limiting
                time.sleep(random.uniform(self.base_delay, self.base_delay + 2))
                
                self.logger.info(f"🚀 Attempting request to {url} (attempt {attempt + 1}/{max_retries})")
                
                response = session.get(url, timeout=30)
                
                if response.status_code == 200:
                    self.logger.info(f"✅ Success: {url}")
                    return response.json()
                elif response.status_code == 403:
                    self.logger.warning(f"🚫 403 Forbidden with proxy {proxy.split(':')[0]} (attempt {attempt + 1})")
                    # Mark this proxy as failed for this session
                    self.failed_proxies.add(proxy)
                elif response.status_code == 429:
                    self.logger.warning(f"⏰ Rate limited with proxy {proxy.split(':')[0]}")
                    time.sleep(random.uniform(10, 20))
                elif response.status_code == 404:
                    self.logger.info(f"🔍 Not found: {url}")
                    return None
                else:
                    self.logger.error(f"❌ HTTP {response.status_code}: {url}")
                    
            except requests.exceptions.ProxyError as e:
                self.logger.error(f"🔌 Proxy error with {proxy.split(':')[0]}: {str(e)}")
                self.failed_proxies.add(proxy)
            except requests.exceptions.Timeout:
                self.logger.warning(f"⏱️ Timeout with proxy {proxy.split(':')[0]}: {url}")
                self.failed_proxies.add(proxy)
            except requests.exceptions.RequestException as e:
                self.logger.error(f"🔌 Network error for {url}: {str(e)}")
                self.failed_proxies.add(proxy)
            finally:
                session.close()
                
            # Exponential backoff between attempts
            if attempt < max_retries - 1:
                delay = random.uniform(3 * (attempt + 1), 6 * (attempt + 1))
                self.logger.info(f"⏳ Waiting {delay:.1f}s before next attempt...")
                time.sleep(delay)
                    
        self.logger.error(f"💥 Failed after {max_retries} attempts: {url}")
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
            self.logger.warning("No live match data received")
            return []
        
        processed_matches = []
        
        for event in live_data['events']:
            try:
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
                
                # Try to get incidents for more detailed info, but don't fail if we can't
                try:
                    incidents = self.get_match_incidents(str(event['id']))
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
                except Exception as e:
                    self.logger.warning(f"Could not fetch incidents for match {event['id']}: {str(e)}")
                
                processed_matches.append(match_data)
                
            except Exception as e:
                self.logger.error(f"Error processing match {event.get('id', 'unknown')}: {str(e)}")
                continue
        
        self.logger.info(f"✅ Processed {len(processed_matches)} live matches")
        return processed_matches
    
    def process_scheduled_matches(self, date: str = None) -> List[Dict]:
        """Process scheduled matches into simplified format"""
        scheduled_data = self.get_scheduled_matches(date)
        if not scheduled_data or 'events' not in scheduled_data:
            self.logger.warning(f"No scheduled match data received for {date}")
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
        
        self.logger.info(f"✅ Processed {len(processed_matches)} scheduled matches for {date}")
        return processed_matches
    
    def save_to_json(self, data: Dict, filename: str):
        """Save data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"📁 Data saved to {filename}")
        except Exception as e:
            self.logger.error(f"💥 Failed to save data to {filename}: {str(e)}")
    
    def get_proxy_status(self) -> Dict:
        """Get status of proxy usage"""
        return {
            'total_proxies': len(self.proxies),
            'failed_proxies': len(self.failed_proxies),
            'current_proxy': self.current_proxy.split(':')[0] + ':' + self.current_proxy.split(':')[1] if self.current_proxy else None,
            'available_proxies': len(self.proxies) - len(self.failed_proxies)
        }


def main():
    """Main function to demonstrate usage"""
    fetcher = SofaScoreFetcher(max_retries=3, base_delay=2)
    
    print("🚀 Starting SofaScore data fetch with proxy support...")
    
    # Show proxy status
    proxy_status = fetcher.get_proxy_status()
    print(f"🌐 Proxy status: {proxy_status['available_proxies']}/{proxy_status['total_proxies']} available")
    
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
    
    # Show final proxy status
    final_proxy_status = fetcher.get_proxy_status()
    print(f"\n🌐 Final proxy status: {final_proxy_status}")
    
    print("\n🎉 Data fetch completed!")


if __name__ == "__main__":
    main()
