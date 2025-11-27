import os
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 1. Load configuration from .env file
load_dotenv()

# Get the connection string. If it's missing, default to a local SQLite file.
db_connection_str = os.getenv("DATABASE_URL", "sqlite:///nba_analysis.db")

def fetch_and_store_nba_data():
    print(f"1. Connecting to database: {db_connection_str.split(':')[0]}...") 
    # (The split just hides the password/details for the print statement)

    print("2. Fetching data from NBA API...")
    game_finder = leaguegamefinder.LeagueGameFinder()
    games_df = game_finder.get_data_frames()[0]
    
    # Filter for NBA games (League ID '00')
    nba_games = games_df[games_df.LEAGUE_ID == '00'].copy()
    print(f"   Fetched {len(nba_games)} games.")

    print("3. Writing to Database...")
    
    # Create the engine using the string from our .env file
    engine = create_engine(db_connection_str)
    
    # Write to SQL
    # 'chunksize' is a good optimization for larger datasets (Postgres loves this)
    nba_games.to_sql('games', engine, if_exists='replace', index=False, chunksize=500)
    
    print("Success! Data saved.")

if __name__ == "__main__":
    fetch_and_store_nba_data()