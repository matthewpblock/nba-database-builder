import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def verify_scores():
    # 1. Connect to Database
    load_dotenv()
    db_str = os.getenv("DATABASE_URL", "sqlite:///nba_analysis.db")
    engine = create_engine(db_str)

    print(f"Checking data in: {db_str}...\n")

    # 2. Query raw data (Just getting the columns we care about)
    # We order by GAME_ID to ensure the two team rows stay together
    query = """
    SELECT GAME_ID, GAME_DATE, MATCHUP, PTS 
    FROM games 
    ORDER BY GAME_ID DESC 
    LIMIT 10
    """
    
    df = pd.read_sql(query, engine)

    print("--- RAW DATABASE VIEW (What is actually saved) ---")
    print(df)
    print("\n" + "="*50 + "\n")

    print("--- SCOREBOARD VIEW (Reconstructed by Game ID) ---")
    # 3. Logic to pair them up
    # We group by GAME_ID to bundle the two rows together
    grouped = df.groupby('GAME_ID')

    for game_id, group in grouped:
        # Check if we have both sides of the game
        if len(group) == 2:
            team1 = group.iloc[0]
            team2 = group.iloc[1]
            
            # Simple string formatting to look like a scoreboard
            print(f"Date: {team1['GAME_DATE']} | {team1['MATCHUP']} vs {team2['MATCHUP']}")
            print(f"Score: {team1['PTS']} - {team2['PTS']}")
            print("-" * 30)
        else:
            print(f"Game {game_id} is incomplete (found {len(group)} rows).")

if __name__ == "__main__":
    verify_scores()