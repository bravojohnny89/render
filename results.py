import requests
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
import os
from pathlib import Path
import sys
import hashlib
import csv
from itertools import zip_longest

class VirtualResultsScraper:
    """
    A production scraper for Odibets virtual football results with CSV export capability
    Adapted for Render cron jobs
    """
    
    def __init__(self, base_dir: str = "/tmp/virtual_data"):
        """
        Initialize the scraper with organized directory structure
        Using /tmp for Render cron jobs as it's writable
        
        Args:
            base_dir: Base directory for all scraped data (default: /tmp/virtual_data)
        """
        # Use /tmp directory for Render (writable in cron jobs)
        self.base_dir = Path(base_dir)
        self.base_url = "https://odibets.com/pxy/virtuals"
        
        # Headers from the original request
        self.headers = {
            'authority': 'odibets.com',
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': 'Bearer',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://odibets.com/odileague',
            'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
            'x-odi-key': 'OTlMzIjM2IzN3EjK1QTMqUWbvJHaDpyc39GZul2VqATdxcWMmNTZ4ATMnp2MuN2aiF3ct9mckTE='
        }
        
        # Base query parameters
        self.base_params = {
            'competition_id': '1',
            'tab': 'results',
            'level': '2',
            'platform': 'desktop',
            'view': 'odileague',
            'resource': 'virtuals',
            'ua': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
        }
        
        # Setup directory structure
        self.setup_directory_structure()
        
        # Setup logging
        self.setup_logging()
        
        # Enhanced tracking for seen data
        self.tracking_file = self.admin_dir / 'tracking.json'
        self.seen_data = self.load_seen_data()
        
    def setup_directory_structure(self):
        """Create the organized directory structure"""
        # Base directory
        self.base_dir.mkdir(exist_ok=True, parents=True)
        
        # Results directory
        self.results_dir = self.base_dir / 'results'
        self.results_dir.mkdir(exist_ok=True)
        
        # Summaries directory
        self.summaries_dir = self.base_dir / 'summaries'
        self.summaries_dir.mkdir(exist_ok=True)
        
        # Summaries results directory
        self.summaries_results_dir = self.summaries_dir / 'results'
        self.summaries_results_dir.mkdir(exist_ok=True)
        
        # Admin directory
        self.admin_dir = self.results_dir / 'admin'
        self.admin_dir.mkdir(exist_ok=True)
        
        # CSV exports directory
        self.csv_dir = self.results_dir / 'csv_exports'
        self.csv_dir.mkdir(exist_ok=True)
        
        # JSON exports directory
        self.json_dir = self.results_dir / 'json_exports'
        self.json_dir.mkdir(exist_ok=True)
        
    def setup_logging(self):
        """Setup logging to admin folder"""
        log_file = self.admin_dir / 'scraper.log'
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()  # Still log to console for Render logs
            ],
            force=True
        )
        self.logger = logging.getLogger(__name__)

    def load_seen_data(self) -> Dict:
        """Load comprehensive tracking data from admin folder"""
        if self.tracking_file.exists():
            try:
                with open(self.tracking_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return {
                        'seen_match_ids': set(data.get('seen_match_ids', [])),
                        'seen_rounds': set(data.get('seen_rounds', [])),
                        'seen_timestamps': data.get('seen_timestamps', {}),
                        'match_fingerprints': data.get('match_fingerprints', {}),
                        'last_period_checked': data.get('last_period_checked'),
                        'total_matches_seen': data.get('total_matches_seen', 0),
                        'last_export_timestamp': data.get('last_export_timestamp'),
                        'total_api_calls': data.get('total_api_calls', 0),
                        'total_cycles_completed': data.get('total_cycles_completed', 0),
                        'scraper_start_date': data.get('scraper_start_date', datetime.now().isoformat()),
                        'total_summaries_created': data.get('total_summaries_created', 0)
                    }
            except Exception as e:
                print(f"Error loading tracking data: {e}")
                return self.get_empty_tracking_data()
        return self.get_empty_tracking_data()
    
    def get_empty_tracking_data(self) -> Dict:
        """Return empty tracking data structure"""
        return {
            'seen_match_ids': set(),
            'seen_rounds': set(),
            'seen_timestamps': {},
            'match_fingerprints': {},
            'last_period_checked': None,
            'total_matches_seen': 0,
            'last_export_timestamp': None,
            'total_api_calls': 0,
            'total_cycles_completed': 0,
            'scraper_start_date': datetime.now().isoformat(),
            'total_summaries_created': 0
        }
    
    def save_seen_data(self):
        """Save comprehensive tracking data to admin folder"""
        try:
            serializable_data = {
                'seen_match_ids': list(self.seen_data['seen_match_ids']),
                'seen_rounds': list(self.seen_data['seen_rounds']),
                'seen_timestamps': self.seen_data['seen_timestamps'],
                'match_fingerprints': self.seen_data['match_fingerprints'],
                'last_period_checked': self.seen_data['last_period_checked'],
                'total_matches_seen': len(self.seen_data['seen_match_ids']),
                'last_export_timestamp': self.seen_data.get('last_export_timestamp'),
                'total_api_calls': self.seen_data.get('total_api_calls', 0),
                'total_cycles_completed': self.seen_data.get('total_cycles_completed', 0),
                'scraper_start_date': self.seen_data.get('scraper_start_date'),
                'total_summaries_created': self.seen_data.get('total_summaries_created', 0)
            }
            
            with open(self.tracking_file, 'w', encoding='utf-8') as f:
                json.dump(serializable_data, f, indent=2)
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error saving tracking data: {e}")
    
    def create_match_fingerprint(self, match: Dict, round_data: Dict) -> str:
        """Create a unique fingerprint for a match"""
        fingerprint_data = f"{match.get('parent_match_id')}_{match.get('result')}_{round_data.get('start_time')}"
        return hashlib.md5(fingerprint_data.encode()).hexdigest()
    
    def extract_new_results_enhanced(self, data: Dict) -> Tuple[List[Dict], Dict]:
        """Extract new results with better tracking"""
        new_matches = []
        stats = {
            'total_rounds': 0,
            'total_matches': 0,
            'new_matches': 0,
            'updated_matches': 0,
            'skipped_duplicates': 0,
            'periods_covered': []
        }
        
        if not data or 'data' not in data or 'results' not in data['data']:
            return new_matches, stats
        
        results = data['data']['results']
        stats['total_rounds'] = len(results)
        
        periods_set = set()
        
        for round_data in results:
            round_time = round_data.get('start_time', '')
            round_id = round_data.get('round_id', '')
            season_id = round_data.get('season_id', '')
            
            round_key = f"{season_id}_{round_id}"
            
            if round_time:
                periods_set.add(round_time)
            
            if 'matches' in round_data:
                for match in round_data['matches']:
                    stats['total_matches'] += 1
                    match_id = match.get('parent_match_id')
                    
                    if not match_id:
                        continue
                    
                    current_fingerprint = self.create_match_fingerprint(match, round_data)
                    
                    if match_id in self.seen_data['seen_match_ids']:
                        old_fingerprint = self.seen_data['match_fingerprints'].get(match_id)
                        if old_fingerprint != current_fingerprint:
                            stats['updated_matches'] += 1
                            
                            enriched_match = {
                                **match,
                                'round_id': round_id,
                                'round_start_time': round_time,
                                'season_id': season_id,
                                'scraped_at': datetime.now().isoformat(),
                                'update_type': 'score_updated',
                                'previous_fingerprint': old_fingerprint
                            }
                            new_matches.append(enriched_match)
                            
                            self.seen_data['match_fingerprints'][match_id] = current_fingerprint
                        else:
                            stats['skipped_duplicates'] += 1
                    else:
                        stats['new_matches'] += 1
                        
                        enriched_match = {
                            **match,
                            'round_id': round_id,
                            'round_start_time': round_time,
                            'season_id': season_id,
                            'scraped_at': datetime.now().isoformat(),
                            'update_type': 'new_match'
                        }
                        new_matches.append(enriched_match)
                        
                        self.seen_data['seen_match_ids'].add(match_id)
                        self.seen_data['match_fingerprints'][match_id] = current_fingerprint
                        self.seen_data['seen_rounds'].add(round_key)
        
        stats['periods_covered'] = list(periods_set)
        
        if results and results[0].get('start_time'):
            self.seen_data['last_period_checked'] = results[0]['start_time']
        
        if new_matches:
            self.save_seen_data()
        
        return new_matches, stats
    
    def get_next_period_to_check(self) -> Optional[str]:
        """Determine the next period to check"""
        if self.seen_data['last_period_checked']:
            try:
                last_time = datetime.strptime(self.seen_data['last_period_checked'], '%Y-%m-%d %H:%M:%S')
                next_time = last_time + timedelta(seconds=2)
                return next_time.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
        return None

    def save_to_csv(self, matches: List[Dict], timestamp: str) -> Optional[Path]:
        """Save matches to CSV file"""
        if not matches:
            return None
        
        csv_columns = [
            'scrape_timestamp',
            'match_id',
            'parent_match_id',
            'home_team',
            'away_team',
            'result',
            'round_id',
            'round_start_time',
            'season_id',
            'update_type',
            'home_score',
            'away_score',
            'match_status'
        ]
        
        filename = f"{timestamp}.csv"
        filepath = self.csv_dir / filename
        
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                
                for match in matches:
                    result = match.get('result', '')
                    home_score, away_score = self.parse_result(result)
                    
                    row = {
                        'scrape_timestamp': match.get('scraped_at', ''),
                        'match_id': match.get('match_id', ''),
                        'parent_match_id': match.get('parent_match_id', ''),
                        'home_team': match.get('home_team', ''),
                        'away_team': match.get('away_team', ''),
                        'result': result,
                        'round_id': match.get('round_id', ''),
                        'round_start_time': match.get('round_start_time', ''),
                        'season_id': match.get('season_id', ''),
                        'update_type': match.get('update_type', ''),
                        'home_score': home_score,
                        'away_score': away_score,
                        'match_status': self.determine_match_status(result, match.get('update_type', ''))
                    }
                    writer.writerow(row)
            
            self.logger.info(f"Saved {len(matches)} matches to CSV: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")
            return None
    
    def parse_result(self, result: str) -> Tuple[str, str]:
        """Parse result string like '2-1' into home and away scores"""
        if result and '-' in result:
            parts = result.split('-')
            if len(parts) == 2:
                return parts[0].strip(), parts[1].strip()
        return '', ''
    
    def determine_match_status(self, result: str, update_type: str) -> str:
        """Determine match status based on result and update type"""
        if not result:
            return 'scheduled'
        elif update_type == 'score_updated':
            return 'live'
        elif result and '-' in result:
            return 'completed'
        else:
            return 'unknown'
    
    def save_to_json(self, matches: List[Dict], metadata: Dict, timestamp: str) -> Tuple[Optional[Path], Optional[Path]]:
        """Save matches and metadata to JSON files"""
        data_filename = f"{timestamp}.json"
        data_filepath = self.json_dir / data_filename
        
        metadata_filename = f"metadata_{timestamp}.json"
        metadata_filepath = self.json_dir / metadata_filename
        
        try:
            with open(data_filepath, 'w', encoding='utf-8') as f:
                json.dump(matches, f, indent=2)
            
            with open(metadata_filepath, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            
            self.logger.info(f"Saved {len(matches)} matches to JSON: {data_filepath}")
            return data_filepath, metadata_filepath
            
        except Exception as e:
            self.logger.error(f"Error saving to JSON: {e}")
            return None, None
    
    def save_cycle_summary(self, cycle_number: int, stats: Dict, matches: List[Dict], timestamp: str) -> Optional[Path]:
        """Save a detailed summary of each cycle"""
        summary_filename = f"cycle_{cycle_number:04d}_{timestamp}.txt"
        summary_filepath = self.summaries_results_dir / summary_filename
        
        try:
            with open(summary_filepath, 'w', encoding='utf-8') as f:
                f.write("="*80 + "\n")
                f.write(f"CYCLE SUMMARY - Cycle #{cycle_number}\n")
                f.write("="*80 + "\n\n")
                
                f.write("CYCLE INFORMATION:\n")
                f.write("-"*40 + "\n")
                f.write(f"Cycle Number: {cycle_number}\n")
                f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Export Timestamp: {timestamp}\n\n")
                
                f.write("STATISTICS:\n")
                f.write("-"*40 + "\n")
                f.write(f"Total Rounds Processed: {stats.get('total_rounds', 0)}\n")
                f.write(f"Total Matches Processed: {stats.get('total_matches', 0)}\n")
                f.write(f"New Matches Found: {stats.get('new_matches', 0)}\n")
                f.write(f"Updated Matches: {stats.get('updated_matches', 0)}\n")
                f.write(f"Duplicates Skipped: {stats.get('skipped_duplicates', 0)}\n")
                f.write(f"Periods Covered: {len(stats.get('periods_covered', []))}\n\n")
                
                if stats.get('periods_covered'):
                    f.write("PERIODS COVERED:\n")
                    f.write("-"*40 + "\n")
                    for period in stats['periods_covered'][:10]:  # Limit to 10 periods
                        f.write(f"  • {period}\n")
                    if len(stats['periods_covered']) > 10:
                        f.write(f"  • ... and {len(stats['periods_covered']) - 10} more\n")
                    f.write("\n")
                
                f.write("EXPORT FILES:\n")
                f.write("-"*40 + "\n")
                f.write(f"CSV: {timestamp}.csv\n")
                f.write(f"JSON: {timestamp}.json\n")
                f.write(f"Metadata: metadata_{timestamp}.json\n\n")
                
                if matches:
                    f.write("MATCHES FOUND IN THIS CYCLE:\n")
                    f.write("-"*40 + "\n")
                    for i, match in enumerate(matches[:20], 1):  # Limit to 20 matches
                        update_type = match.get('update_type', 'unknown')
                        type_indicator = "NEW" if update_type == 'new_match' else "UPD"
                        
                        f.write(f"{i:3}. [{type_indicator}] ")
                        f.write(f"{match.get('home_team', 'Unknown')} vs {match.get('away_team', 'Unknown')}: ")
                        f.write(f"{match.get('result', 'No result')}\n")
                    
                    if len(matches) > 20:
                        f.write(f"\n  ... and {len(matches) - 20} more matches\n")
                    f.write("\n")
                
                f.write("RUNNING TOTALS:\n")
                f.write("-"*40 + "\n")
                f.write(f"Total Unique Matches Overall: {len(self.seen_data['seen_match_ids'])}\n")
                f.write(f"Total Unique Rounds Overall: {len(self.seen_data['seen_rounds'])}\n")
                f.write(f"Total API Calls Made: {self.seen_data.get('total_api_calls', 0)}\n\n")
                
                f.write("="*80 + "\n")
                f.write("END OF CYCLE SUMMARY\n")
                f.write("="*80 + "\n")
            
            self.seen_data['total_summaries_created'] = self.seen_data.get('total_summaries_created', 0) + 1
            self.save_seen_data()
            
            self.logger.info(f"Saved cycle summary to: {summary_filepath}")
            return summary_filepath
            
        except Exception as e:
            self.logger.error(f"Error saving cycle summary: {e}")
            return None
    
    def make_api_call(self, period_time: Optional[datetime] = None) -> Optional[Dict]:
        """Make the API call to fetch virtual results"""
        params = self.base_params.copy()
        
        if period_time:
            period_str = period_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            next_period = self.get_next_period_to_check()
            if next_period:
                period_str = next_period
            else:
                period_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        params['period'] = period_str
        
        try:
            self.logger.info(f"Making API call with period: {period_str}")
            
            self.seen_data['total_api_calls'] = self.seen_data.get('total_api_calls', 0) + 1
            self.save_seen_data()
            
            response = requests.get(
                self.base_url,
                headers=self.headers,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                self.logger.info("API call successful")
                return response.json()
            else:
                self.logger.error(f"API call failed with status code: {response.status_code}")
                return None
                
        except Exception as e:
            self.logger.error(f"API call error: {e}")
            return None
    
    def export_results(self, matches: List[Dict], cycle_number: int = 1, stats: Dict = None) -> Optional[str]:
        """Export scraped results to both CSV and JSON formats"""
        if not matches:
            self.logger.info("No matches to export")
            return None
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        metadata = {
            'export_timestamp': datetime.now().isoformat(),
            'cycle_number': cycle_number,
            'total_matches': len(matches),
            'stats': stats,
            'new_matches': stats.get('new_matches', 0) if stats else 0,
            'updated_matches': stats.get('updated_matches', 0) if stats else 0,
            'periods_covered': stats.get('periods_covered', []) if stats else []
        }
        
        csv_path = self.save_to_csv(matches, timestamp)
        json_path, metadata_path = self.save_to_json(matches, metadata, timestamp)
        
        self.seen_data['last_export_timestamp'] = timestamp
        self.save_seen_data()
        
        if csv_path and json_path:
            self.logger.info(f"Successfully exported {len(matches)} matches")
        
        return timestamp
    
    def run_once(self):
        """
        Run a single scraping cycle (for cron job usage)
        Returns exit code (0 for success, 1 for failure)
        """
        try:
            cycle_count = self.seen_data.get('total_cycles_completed', 0) + 1
            
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"CRON JOB CYCLE #{cycle_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"{'='*60}")
            
            # Make API call
            data = self.make_api_call()
            
            if data:
                # Extract new results
                new_matches, stats = self.extract_new_results_enhanced(data)
                
                if new_matches:
                    # Export matches
                    timestamp = self.export_results(new_matches, cycle_count, stats)
                    
                    # Save cycle summary
                    if timestamp:
                        self.save_cycle_summary(cycle_count, stats, new_matches, timestamp)
                    
                    self.logger.info(f"Cycle {cycle_count} Summary:")
                    self.logger.info(f"  New matches: {stats['new_matches']}")
                    self.logger.info(f"  Updated matches: {stats['updated_matches']}")
                    self.logger.info(f"  Total unique matches: {len(self.seen_data['seen_match_ids'])}")
                else:
                    self.logger.info(f"No new or updated matches found in cycle #{cycle_count}")
                    
                    # Still save a summary
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    self.save_cycle_summary(cycle_count, stats, [], timestamp)
                    self.logger.info("  Saved summary with no new matches")
                
                self.seen_data['total_cycles_completed'] = cycle_count
                self.save_seen_data()
                
                return 0  # Success
            else:
                self.logger.error("Failed to get data from API")
                return 1  # Failure
                
        except Exception as e:
            self.logger.error(f"Error in run_once: {e}")
            return 1

def main():
    """Main function for cron job execution"""
    # Use /tmp directory for Render cron jobs
    scraper = VirtualResultsScraper(base_dir="/tmp/virtual_data")
    
    # Run a single cycle
    exit_code = scraper.run_once()
    
    # Print final status
    if exit_code == 0:
        print(f"\n✅ Cron job completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Total unique matches: {len(scraper.seen_data['seen_match_ids'])}")
        print(f"   Data saved in: {scraper.base_dir}")
    else:
        print(f"\n❌ Cron job failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   Check logs in: {scraper.admin_dir / 'scraper.log'}")
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main()