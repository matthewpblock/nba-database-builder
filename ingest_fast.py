import pandas as pd
import time
import logging
from sqlalchemy import create_engine, text
from requests.exceptions import ReadTimeout, ConnectionError, JSONDecodeError
from nba_api.stats.endpoints import leaguegamefinder, playbyplayv3
from models import PlayByPlay
from logger_config import setup_logger

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
    logging.info(f"⚡ Ingesting PBP for {game_id}...")
    
    clean_pbp(game_id, engine)
    
    try:
        # Fetch Data
        pbp = playbyplayv3.PlayByPlayV3(game_id=game_id, timeout=45).get_data_frames()[0]
        
        if pbp.empty:
            logging.warning(f"No PBP data returned for {game_id}.")
            return

        # Prepare Data
        df_pbp = prepare_df(pbp, PlayByPlay)
        df_pbp['game_id'] = game_id
        
        # Deduplicate
        df_pbp = df_pbp.drop_duplicates(subset=['event_num'])
        
        # Save
        df_pbp.to_sql('play_by_play', engine, if_exists='append', index=False)
        logging.info(f"   ✅ Saved {len(df_pbp)} rows for {game_id}.")
        
    except (ReadTimeout, ConnectionError, JSONDecodeError) as e:
        logging.warning(f"Timeout/Connection Error on {game_id}.")
        raise e # Re-raise to trigger the pause in the main loop
    except Exception as e:
        logging.error(f"Unexpected error on {game_id}: {e}", exc_info=True)

def get_todo_list(engine):
    print(f"📅 Fetching {TARGET_SEASON} complete schedule (Reg + Playoffs)...")
    
    # --- UPDATED LOGIC TO CATCH PLAYOFFS ---
    season_types = ['Regular Season', 'Playoffs', 'PlayIn']
    all_dfs = []
    
    for s_type in season_types:
        try:
            finder = leaguegamefinder.LeagueGameFinder(
                league_id_nullable='00',
                season_nullable=TARGET_SEASON,
                season_type_nullable=s_type
            )
            df = finder.get_data_frames()[0]
            if not df.empty:
                print(f"   Found {len(df)} games for {s_type}.")
                all_dfs.append(df)
        except Exception:
            # It's normal for Playoffs to be empty early in the season
            pass

    if not all_dfs:
        print("❌ Critical Error: Could not find any games via API.")
        return []

    # Combine all season types
    all_games = pd.concat(all_dfs, ignore_index=True)
    
    # Filter for completed games (Games with a W/L result)
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
        logging.info("🎉 All PBP data is up to date!")
        return

    logging.info(f"--- Starting FAST PBP Ingest for {len(missing_ids)} games ---")
    
    for i, game_id in enumerate(missing_ids, 1):
        success = False
        attempts = 0
        
        while not success and attempts < 3:
            try:
                ingest_pbp_single(game_id, engine)
                success = True
                time.sleep(0.6) # Short pause for speed
                
            except (ReadTimeout, ConnectionError, JSONDecodeError):
                attempts += 1
                # Incremental backoff: wait longer after each failure
                if attempts < 3:
                    current_pause = RETRY_PAUSE * attempts
                    logging.warning(f"API Limit hit on attempt {attempts}! Pausing for {current_pause}s...")
                    time.sleep(current_pause)
                    logging.info("Resuming...")
                else:
                    logging.error(f"Failed to ingest {game_id} after 3 attempts. Skipping.")

if __name__ == "__main__":
    setup_logger()
    run_fast_ingest()
    logging.info("--- Fast PBP Ingestion Run Finished ---")