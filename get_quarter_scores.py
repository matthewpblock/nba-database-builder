import pandas as pd
import time
import os
from sqlalchemy import create_engine
from nba_api.stats.endpoints import boxscoresummaryv3
from dotenv import load_dotenv

def get_quarter_data():
    load_dotenv()
    db_str = os.getenv("DATABASE_URL", "sqlite:///nba_analysis.db")
    engine = create_engine(db_str)
    
    print(f"Connecting to: {db_str}")

    # 1. Get Game IDs (Last 5)
    game_ids_df = pd.read_sql("SELECT DISTINCT GAME_ID FROM games ORDER BY GAME_DATE DESC LIMIT 5", engine)
    game_ids = game_ids_df['GAME_ID'].tolist()
    
    print(f"Found {len(game_ids)} games. Switching to V3 Endpoint...")

    all_line_scores = []

    for i, game_id in enumerate(game_ids):
        try:
            print(f"[{i+1}/{len(game_ids)}] Fetching V3 details for Game {game_id}...")
            
            box_summary = boxscoresummaryv3.BoxScoreSummaryV3(game_id=game_id)
            line_score_df = box_summary.line_score.get_data_frame()
            
            if not line_score_df.empty:
                all_line_scores.append(line_score_df)
            
            time.sleep(0.6) # Pause to respect API limits
            
        except Exception as e:
            print(f"Error fetching game {game_id}: {e}")

    # 3. Combine, Rename, and Save
    if all_line_scores:
        final_df = pd.concat(all_line_scores, ignore_index=True)
        
        # --- THE FIX: RENAME COLUMNS ---
        # We create a dictionary to map V3 names to our database standard
        rename_map = {
            'gameId': 'GAME_ID',
            'teamId': 'TEAM_ID',
            'teamTricode': 'TEAM_ABBREVIATION',
            'period1Score': 'PTS_QTR1',
            'period2Score': 'PTS_QTR2',
            'period3Score': 'PTS_QTR3',
            'period4Score': 'PTS_QTR4',
            'score': 'PTS'
        }
        
        # Rename the columns in the dataframe
        final_df.rename(columns=rename_map, inplace=True)
        
        # Select only the columns we want to keep
        # We use the NEW names here because we just renamed them
        cols_to_keep = [
            'GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 
            'PTS_QTR1', 'PTS_QTR2', 'PTS_QTR3', 'PTS_QTR4', 'PTS'
        ]
        
        # Save to database
        final_df[cols_to_keep].to_sql('line_scores', engine, if_exists='replace', index=False)
        
        print("\nSUCCESS! Quarter scores saved to table 'line_scores'.")
        print(final_df[cols_to_keep])
            
    else:
        print("No data collected.")

if __name__ == "__main__":
    get_quarter_data()