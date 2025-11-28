import pandas as pd
import sys
import time
from sqlalchemy import create_engine
from nba_api.stats.endpoints import leaguegamefinder
from ingest_game import ingest_game  # Import the worker we just built

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
TARGET_SEASON = '2024-25'

def get_season_schedule():
    print(f"📅 Fetching schedule for {TARGET_SEASON}...")
    
    # LeagueID '00' = NBA (excludes G-League/WNBA)
    # SeasonType 'Regular Season' (excludes Preseason/All-Star)
    finder = leaguegamefinder.LeagueGameFinder(
        league_id_nullable='00',
        season_nullable=TARGET_SEASON,
        season_type_nullable='Regular Season'
    )
    
    df = finder.get_data_frames()[0]
    
    # Filter: Only keep completed games (Games with a "WL" result)
    # This prevents us from trying to download games scheduled for tomorrow
    completed_games = df[df['WL'].notna()].copy()
    
    # Dedup: The API returns 2 rows per game (Home & Away). We only need the unique Game IDs.
    unique_game_ids = completed_games['GAME_ID'].unique().tolist()
    
    print(f"   Found {len(unique_game_ids)} completed games so far.")
    return unique_game_ids

def get_existing_games():
    """Checks the database to see what we already have."""
    engine = create_engine(DB_URL)
    try:
        # We check the 'games' table (or player_game_stats) for IDs
        existing = pd.read_sql("SELECT DISTINCT game_id FROM player_game_stats", engine)
        return existing['game_id'].tolist()
    except Exception:
        return []

def run_season_ingest():
    # 1. Get the "To Do" List
    schedule_ids = get_season_schedule()
    
    # 2. Get the "Done" List
    done_ids = get_existing_games()
    
    # 3. Calculate the difference
    missing_ids = [g for g in schedule_ids if g not in done_ids]
    
    if not missing_ids:
        print("🎉 Your database is fully up to date! No new games to ingest.")
        return

    print(f"🚀 Starting ingestion for {len(missing_ids)} new games...")
    print("-" * 50)

    # 4. The Loop
    for i, game_id in enumerate(missing_ids):
        print(f"[{i+1}/{len(missing_ids)}] Processing Game {game_id}...")
        
        try:
            # Call the 'ingest_game' function from your other script
            # full_mode=True gets PBP, Matchups, etc. Set to False for speed.
            ingest_game(game_id, full_mode=True)
            
        except Exception as e:
            print(f"   ⚠️ Failed to ingest {game_id}: {e}")
        
        # Sleep to avoid "API Jail" (Rate Limits)
        time.sleep(1.5) 

    print("\n✅ Season Ingestion Complete.")

if __name__ == "__main__":
    run_season_ingest()