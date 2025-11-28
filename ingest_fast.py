import pandas as pd
import time
import sys
from sqlalchemy import create_engine, text
from requests.exceptions import ReadTimeout, ConnectionError, JSONDecodeError
from nba_api.stats.endpoints import leaguegamefinder, playbyplayv3
from models import PlayByPlay

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
TARGET_SEASON = '2024-25'
RETRY_PAUSE = 300  # 5 minutes pause if banned

# --- MINIMAL COLUMN MAP (Just for PBP) ---
COLUMN_MAP = {
    'game_id': ['gameId', 'GAME_ID'],
    'team_id': ['teamId', 'TEAM_ID'],
    'player_id': ['personId', 'PLAYER_ID', 'person_id'],
    'event_num': ['actionNumber', 'EVENTNUM'],
    'period': ['period', 'PERIOD'],
    'clock': ['clock', 'PCTIMESTRING'],
    'action_type': ['actionType', 'EVENTMSGTYPE'],
    'sub_type': ['subType', 'EVENTMSGACTIONTYPE'],
    'description': ['description', 'HOMEDESCRIPTION', 'VISITORDESCRIPTION'],
    'shot_result': ['shotResult'],
    'loc_x': ['xLegacy', 'LOC_X'],
    'loc_y': ['yLegacy', 'LOC_Y'],
    'margin': ['pointsTotal', 'SCOREMARGIN'],
    'score_home': ['scoreHome'],
    'score_away': ['scoreAway']
}

def get_db_engine():
    return create_engine(DB_URL)

def prepare_df(df, table_model):
    """Renames columns and keeps only what fits in the DB."""
    final_cols = {}
    for db_col, api_options in COLUMN_MAP.items():
        for api_col in api_options:
            if api_col in df.columns:
                final_cols[api_col] = db_col
                break
    
    df_renamed = df.rename(columns=final_cols)
    valid_db_cols = [c.name for c in table_model.__table__.columns]
    
    # Return only valid columns that exist in the dataframe
    return df_renamed[[c for c in df_renamed.columns if c in valid_db_cols]].copy()

def clean_pbp(game_id, engine):
    """Deletes any existing events for this game to prevent duplicates."""
    with engine.connect() as conn:
        try:
            conn.execute(text("DELETE FROM play_by_play WHERE game_id = :gid"), {'gid': game_id})
            conn.commit()
        except Exception:
            pass

def ingest_pbp_single(game_id, engine):
    print(f"⚡ Ingesting PBP for {game_id}...", end=" ")
    
    clean_pbp(game_id, engine)
    
    try:
        # Fetch Data
        pbp = playbyplayv3.PlayByPlayV3(game_id=game_id, timeout=30).get_data_frames()[0]
        
        if pbp.empty:
            print("❌ Empty Data.")
            return

        # Prepare Data
        df_pbp = prepare_df(pbp, PlayByPlay)
        df_pbp['game_id'] = game_id
        
        # Deduplicate
        df_pbp = df_pbp.drop_duplicates(subset=['event_num'])
        
        # Save
        df_pbp.to_sql('play_by_play', engine, if_exists='append', index=False)
        print(f"✅ Saved {len(df_pbp)} rows.")
        
    except (ReadTimeout, ConnectionError, JSONDecodeError) as e:
        print("🛑 TIMEOUT.")
        raise e # Re-raise to trigger the pause in the main loop
    except Exception as e:
        print(f"⚠️ Error: {e}")

def get_todo_list(engine):
    print(f"📅 Fetching {TARGET_SEASON} schedule...")
    finder = leaguegamefinder.LeagueGameFinder(
        league_id_nullable='00',
        season_nullable=TARGET_SEASON,
        season_type_nullable='Regular Season'
    )
    all_games = finder.get_data_frames()[0]
    
    # Filter for completed games
    completed = all_games[all_games['WL'].notna()].copy()
    target_ids = set(completed['GAME_ID'].unique())
    
    # Check what we already have in PBP table
    try:
        existing = pd.read_sql("SELECT DISTINCT game_id FROM play_by_play", engine)
        done_ids = set(existing['game_id'].tolist())
    except:
        done_ids = set()
        
    missing = sorted(list(target_ids - done_ids))
    print(f"   Total Games: {len(target_ids)} | Already Done: {len(done_ids)} | To Do: {len(missing)}")
    return missing

def run_fast_ingest():
    engine = get_db_engine()
    missing_ids = get_todo_list(engine)
    
    if not missing_ids:
        print("🎉 All caught up!")
        return

    print("-" * 50)
    
    for i, game_id in enumerate(missing_ids):
        success = False
        attempts = 0
        
        while not success and attempts < 3:
            try:
                ingest_pbp_single(game_id, engine)
                success = True
                time.sleep(0.6) # Short pause for speed
                
            except (ReadTimeout, ConnectionError, JSONDecodeError):
                attempts += 1
                print(f"\n🛑 API Limit hit! Pausing for {RETRY_PAUSE}s...")
                time.sleep(RETRY_PAUSE)
                print("   Resuming...")

if __name__ == "__main__":
    run_fast_ingest()