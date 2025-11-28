import pandas as pd
import sys
import time
from sqlalchemy import create_engine
from nba_api.stats.endpoints import leaguegamefinder
from ingest_game import ingest_game
from requests.exceptions import ReadTimeout, ConnectionError, JSONDecodeError

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
TARGET_SEASON = '2024-25'
RETRY_PAUSE = 300  # Seconds to wait if API blocks us (5 Minutes)

def get_season_schedule():
    print(f"📅 Fetching schedule for {TARGET_SEASON}...")
    finder = leaguegamefinder.LeagueGameFinder(
        league_id_nullable='00',
        season_nullable=TARGET_SEASON,
        season_type_nullable='Regular Season'
    )
    df = finder.get_data_frames()[0]
    completed_games = df[df['WL'].notna()].copy()
    unique_game_ids = completed_games['GAME_ID'].unique().tolist()
    print(f"   Found {len(unique_game_ids)} completed games so far.")
    return unique_game_ids

def get_existing_games():
    engine = create_engine(DB_URL)
    try:
        existing = pd.read_sql("SELECT DISTINCT game_id FROM player_game_stats", engine)
        return existing['game_id'].tolist()
    except Exception:
        return []

def run_season_ingest():
    schedule_ids = get_season_schedule()
    done_ids = get_existing_games()
    missing_ids = [g for g in schedule_ids if g not in done_ids]
    
    if not missing_ids:
        print("🎉 Your database is fully up to date! No new games to ingest.")
        return

    print(f"🚀 Starting ingestion for {len(missing_ids)} new games...")
    print("-" * 50)

    # 4. The Smart Loop
    for i, game_id in enumerate(missing_ids):
        success = False
        attempts = 0
        
        # Retry up to 3 times for the same game
        while not success and attempts < 3:
            try:
                print(f"[{i+1}/{len(missing_ids)}] Processing Game {game_id} (Attempt {attempts+1})...")
                
                # Run the Worker
                ingest_game(game_id, full_mode=True)
                success = True
                
                # Standard polite wait
                time.sleep(1.5) 
                
            except (ReadTimeout, ConnectionError, JSONDecodeError) as e:
                attempts += 1
                
                # Implement incremental backoff: wait longer on the second failure.
                # 1st failure: waits RETRY_PAUSE seconds.
                # 2nd failure: waits RETRY_PAUSE * 2 seconds.
                current_pause = RETRY_PAUSE * attempts
                
                print(f"\n🛑 API LIMIT/TIMEOUT on Game {game_id} (Attempt {attempts}). Pausing for {current_pause / 60:.1f} minutes...")
                print(f"   (Don't close the window, it will resume automatically. Error: {e})\n")
                time.sleep(current_pause)
            except Exception as e:
                print(f"   ⚠️ Unexpected Critical Error on {game_id}: {e}")
                break # Move to next game if it's a weird code error, not a network error

    print("\n✅ Season Ingestion Complete.")

if __name__ == "__main__":
    run_season_ingest()