import requests
import json
import time
import random
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import itertools
from urllib.parse import urlencode
import ssl
import urllib3

class SofaScoreFetcher:
    def __init__(self, max_retries=2, base_delay=2):
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
        
        # Enhanced headers with more realistic browser fingerprint
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0'
        ]
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Disable SSL warnings for proxy connections
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Rate limiting
        self.last_request_time = 0
        self.min_request_interval = 5  # Minimum 5 seconds between requests
        
    def _get_headers(self) -> Dict[str, str]:
        """Generate realistic headers with random user agent"""
        return {
            'User-Agent': random.choice(self.user_agents),
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
            'DNT': '1',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
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
        max_attempts = len(self.proxies) * 2
        
        while attempts < max_attempts:
            proxy = next(self.proxy_cycle)
            if proxy not in self.failed_proxies:
                return proxy
            attempts += 1
        
        # If all proxies are failed, reset and try again
        if self.failed_proxies:
            self.logger.warning("🔄 All proxies failed, resetting failed proxy list")
            self.failed_proxies.clear()
            return next(self.proxy_cycle)
        
        return None
    
    def _create_session(self, proxy_string: str = None) -> requests.Session:
        """Create a new session with enhanced configuration"""
        session = requests.Session()
        session.headers.update(self._get_headers())
        
        if proxy_string:
            proxy_config = self._get_proxy_config(proxy_string)
            if proxy_config:
                session.proxies.update(proxy_config)
                proxy_display = f"{proxy_string.split(':')[0]}:{proxy_string.split(':')[1]}"
                self.logger.info(f"🌐 Using proxy: {proxy_display}")
        
        # Configure session for better reliability
        adapter = requests.adapters.HTTPAdapter(
            max_retries=urllib3.util.Retry(
                total=0,  # We handle retries manually
                backoff_factor=1,
                status_forcelist=[500, 502, 503, 504]
            )
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        return session
    
    def _enforce_rate_limit(self):
        """Enforce minimum time between requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            self.logger.info(f"⏱️ Rate limiting: sleeping for {sleep_time:.1f}s")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, max_retries: int = None) -> Optional[Dict]:
        """Enhanced request method with better error handling"""
        max_retries = max_retries or self.max_retries
        
        # Enforce rate limiting
        self._enforce_rate_limit()
        
        for attempt in range(max_retries):
            proxy = self._get_next_proxy()
            if not proxy:
                self.logger.error("❌ No available proxies")
                break
                
            session = self._create_session(proxy)
            self.current_proxy = proxy
            
            try:
                # Add some randomness to avoid pattern detection
                time.sleep(random.uniform(1, 3))
                
                self.logger.info(f"🚀 Request to {url} (attempt {attempt + 1}/{max_retries})")
                
                response = session.get(
                    url, 
                    timeout=(15, 30),  # (connection, read) timeout
                    verify=False,  # Skip SSL verification for proxies
                    allow_redirects=True
                )
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        self.logger.info(f"✅ Success: {url}")
                        return data
                    except json.JSONDecodeError:
                        self.logger.error(f"❌ Invalid JSON response from {url}")
                        return None
                        
                elif response.status_code == 403:
                    self.logger.warning(f"🚫 403 Forbidden (attempt {attempt + 1})")
                    self.failed_proxies.add(proxy)
                    
                elif response.status_code == 429:
                    self.logger.warning(f"⏰ Rate limited (attempt {attempt + 1})")
                    time.sleep(random.uniform(15, 30))
                    
                elif response.status_code == 404:
                    self.logger.info(f"🔍 404 Not found: {url}")
                    return None
                    
                else:
                    self.logger.error(f"❌ HTTP {response.status_code}: {url}")
                    
            except requests.exceptions.ProxyError as e:
                self.logger.error(f"🔌 Proxy error: {str(e)}")
                self.failed_proxies.add(proxy)
                
            except requests.exceptions.Timeout:
                self.logger.warning(f"⏱️ Request timeout")
                self.failed_proxies.add(proxy)
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"🔌 Request error: {str(e)}")
                self.failed_proxies.add(proxy)
                
            except Exception as e:
                self.logger.error(f"💥 Unexpected error: {str(e)}")
                
            finally:
                session.close()
                
            # Progressive backoff between attempts
            if attempt < max_retries - 1:
                delay = random.uniform(5 * (attempt + 1), 10 * (attempt + 1))
                self.logger.info(f"⏳ Waiting {delay:.1f}s before retry...")
                time.sleep(delay)
                    
        self.logger.error(f"💥 Failed after {max_retries} attempts: {url}")
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
            self.logger.warning("⚠️ No live match data received")
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
                
                # Get basic status info without fetching incidents to reduce API calls
                if 'time' in event and event['time']:
                    match_data['currentTime'] = event['time'].get('currentPeriodStartTimestamp', 0)
                
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
            self.logger.warning(f"⚠️ No scheduled match data for {date}")
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
            'failed_proxy_list': list(self.failed_proxies),
            'current_proxy': self.current_proxy.split(':')[0] + ':' + self.current_proxy.split(':')[1] if self.current_proxy else None,
            'available_proxies': len(self.proxies) - len(self.failed_proxies),
            'success_rate': f"{((len(self.proxies) - len(self.failed_proxies)) / len(self.proxies) * 100):.1f}%"
        }
    
    def reset_failed_proxies(self):
        """Reset failed proxies list - useful for periodic cleanup"""
        self.logger.info(f"🔄 Resetting {len(self.failed_proxies)} failed proxies")
        self.failed_proxies.clear()


def main():
    """Test the enhanced fetcher"""
    fetcher = SofaScoreFetcher(max_retries=2, base_delay=3)
    
    print("🚀 Starting enhanced SofaScore data fetch...")
    
    # Show initial proxy status
    proxy_status = fetcher.get_proxy_status()
    print(f"🌐 Proxy status: {proxy_status['available_proxies']}/{proxy_status['total_proxies']} available")
    
    # Test with live matches first (usually more reliable)
    print("\n📺 Testing live matches endpoint...")
    live_matches = fetcher.process_live_matches()
    
    if live_matches:
        print(f"✅ Successfully fetched {len(live_matches)} live matches")
        for i, match in enumerate(live_matches[:2]):
            print(f"   {match['home']} {match['homeScore']}-{match['awayScore']} {match['away']} ({match['status']})")
    else:
        print("❌ Failed to fetch live matches")
    
    # Show final proxy status
    final_status = fetcher.get_proxy_status()
    print(f"\n🌐 Final proxy status: {final_status['success_rate']} success rate")
    print(f"   Available: {final_status['available_proxies']}/{final_status['total_proxies']}")
    
    if final_status['failed_proxies'] > 0:
        print(f"   Failed proxies: {final_status['failed_proxies']}")


if __name__ == "__main__":
    main()
