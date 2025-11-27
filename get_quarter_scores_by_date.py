import pandas as pd
import time
import os
import random
from sqlalchemy import create_engine
from nba_api.stats.endpoints import scoreboardv2
from requests.exceptions import ReadTimeout, ConnectionError, RequestException
from dotenv import load_dotenv

# --- CONFIGURATION ---
BATCH_SIZE = 10        # Save to DB every 10 dates
MAX_RETRIES = 3        # Try 3 times before giving up on a specific date
BASE_SLEEP = 1.0       # Seconds to sleep between successful requests
ERROR_SLEEP = 15       # Seconds to sleep after a timeout

def get_quarters_robust():
    load_dotenv()
    db_str = os.getenv("DATABASE_URL", "sqlite:///nba_analysis.db")
    engine = create_engine(db_str)
    
    print(f"--- NBA DATA PIPELINE: ROBUST MODE ---")
    print(f"Database: {db_str}")

    # 1. Get ALL target dates from the Games table
    print("1. analyzing missing data...")
    all_dates_df = pd.read_sql("SELECT DISTINCT GAME_DATE FROM games", engine)
    all_dates = set(all_dates_df['GAME_DATE'].tolist())

    # 2. Get dates we ALREADY have in the line_scores table
    # We wrap this in a try/except because the table might not exist yet
    try:
        existing_dates_df = pd.read_sql("SELECT DISTINCT GAME_DATE_EST FROM line_scores", engine)
        # The API returns dates like '2023-10-25T00:00:00', so we chop off the time part
        existing_dates = set([d.split('T')[0] for d in existing_dates_df['GAME_DATE_EST'].tolist()])
    except Exception:
        existing_dates = set()
        print("   (No existing line_scores table found. Starting from scratch.)")

    # 3. Calculate what is missing
    missing_dates = sorted(list(all_dates - existing_dates))
    total_missing = len(missing_dates)
    
    if total_missing == 0:
        print("🎉 Great news! Your database is already up to date. No downloads needed.")
        return

    print(f"   Found {len(all_dates)} game days total.")
    print(f"   Found {len(existing_dates)} days already downloaded.")
    print(f"   Downloading remaining {total_missing} days...")
    print("------------------------------------------------")

    # 4. The Loop
    batch_data = []
    
    for i, date_str in enumerate(missing_dates):
        success = False
        attempts = 0
        
        while not success and attempts < MAX_RETRIES:
            try:
                print(f"[{i+1}/{total_missing}] Fetching {date_str} (Attempt {attempts+1})...", end="\r")
                
                # We set a distinct timeout so it doesn't hang forever
                board = scoreboardv2.ScoreboardV2(game_date=date_str, timeout=30)
                daily_stats = board.line_score.get_data_frame()
                
                if not daily_stats.empty:
                    batch_data.append(daily_stats)
                
                success = True
                # Sleep a variable amount to look more human
                time.sleep(BASE_SLEEP + random.random()) 

            except (ReadTimeout, ConnectionError) as e:
                attempts += 1
                print(f"\n   ⚠️  Timeout/Connection Error on {date_str}. Sleeping {ERROR_SLEEP}s...")
                time.sleep(ERROR_SLEEP)
            except Exception as e:
                print(f"\n   ❌ Unexpected Error on {date_str}: {e}")
                break # Move to next date on unknown error

        # 5. Checkpoint: Save every BATCH_SIZE or at the very end
        if len(batch_data) >= BATCH_SIZE or (i + 1 == total_missing):
            save_batch(batch_data, engine)
            batch_data = [] # Clear memory
            print(f"\n   ✅ Saved batch to database. Progress saved.")

def save_batch(data_list, engine):
    """Helper function to process and save a list of dataframes"""
    if not data_list:
        return

    final_df = pd.concat(data_list, ignore_index=True)
    
    # Ensure OT columns exist
    ot_cols = ['PTS_OT1', 'PTS_OT2', 'PTS_OT3', 'PTS_OT4']
    for col in ot_cols:
        if col not in final_df.columns:
            final_df[col] = 0
        else:
            final_df[col] = final_df[col].fillna(0)

    # Select Columns
    cols_to_keep = [
        'GAME_DATE_EST', 'GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 
        'PTS_QTR1', 'PTS_QTR2', 'PTS_QTR3', 'PTS_QTR4', 
        'PTS_OT1', 'PTS_OT2', 'PTS_OT3', 'PTS_OT4', 'PTS'
    ]
    
    # Filter valid columns only
    available_cols = [c for c in cols_to_keep if c in final_df.columns]
    clean_df = final_df[available_cols]

    # Append to database (fail silently if table doesn't exist yet, it will create it)
    clean_df.to_sql('line_scores', engine, if_exists='append', index=False)

if __name__ == "__main__":
    get_quarters_robust()