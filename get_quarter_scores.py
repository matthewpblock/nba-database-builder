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

    # 1. Get Game IDs (Last 10 to increase odds of finding data)
    game_ids_df = pd.read_sql("SELECT DISTINCT GAME_ID FROM games ORDER BY GAME_DATE DESC LIMIT 10", engine)
    game_ids = game_ids_df['GAME_ID'].tolist()
    
    print(f"Found {len(game_ids)} games. Processing...")

    all_line_scores = []

    for i, game_id in enumerate(game_ids):
        try:
            print(f"[{i+1}/{len(game_ids)}] Game {game_id}...", end="\r")
            
            box_summary = boxscoresummaryv3.BoxScoreSummaryV3(game_id=game_id)
            line_score_df = box_summary.line_score.get_data_frame()
            
            if not line_score_df.empty:
                all_line_scores.append(line_score_df)
            
            time.sleep(0.6)
            
        except Exception as e:
            print(f"Error fetching game {game_id}: {e}")

    # 3. Process Data
    if all_line_scores:
        final_df = pd.concat(all_line_scores, ignore_index=True)
        
        # --- DEFINING COLUMNS ---
        # Map V3 names to Our Names
        rename_map = {
            'gameId': 'GAME_ID',
            'teamId': 'TEAM_ID',
            'teamTricode': 'TEAM_ABBREVIATION',
            'period1Score': 'PTS_QTR1',
            'period2Score': 'PTS_QTR2',
            'period3Score': 'PTS_QTR3',
            'period4Score': 'PTS_QTR4',
            'period5Score': 'PTS_OT1', # First Overtime
            'period6Score': 'PTS_OT2', # Second Overtime
            'period7Score': 'PTS_OT3', # Third Overtime
            'score': 'PTS'
        }
        
        # --- ROBUSTNESS CHECK ---
        # Even if no games in this batch went to OT, we want these columns to exist 
        # so our database table structure doesn't break.
        potential_ot_cols = ['period5Score', 'period6Score', 'period7Score']
        
        for col in potential_ot_cols:
            if col not in final_df.columns:
                final_df[col] = 0 # Create column filled with 0
            else:
                final_df[col] = final_df[col].fillna(0) # Fill NaNs with 0

        final_df.rename(columns=rename_map, inplace=True)
        
        cols_to_keep = [
            'GAME_ID', 'TEAM_ID', 'TEAM_ABBREVIATION', 
            'PTS_QTR1', 'PTS_QTR2', 'PTS_QTR3', 'PTS_QTR4', 
            'PTS_OT1', 'PTS_OT2', 'PTS_OT3',
            'PTS'
        ]
        
        # Save to database
        final_df[cols_to_keep].to_sql('line_scores', engine, if_exists='replace', index=False)
        
        print(f"\nSUCCESS! Processed {len(final_df)} team entries.")
        print(final_df[cols_to_keep].head())
            
    else:
        print("No data collected.")

if __name__ == "__main__":
    get_quarter_data()