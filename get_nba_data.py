import os
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables at module level
load_dotenv()
# Get the database URL at module level
db_connection_str = os.getenv("DATABASE_URL", "sqlite:///nba_analysis.db")

def fetch_and_store_nba_data():
    print(f"---------------------")

    print("1. Requesting data from NBA API...")
    
    # Fetch all games
    game_finder = leaguegamefinder.LeagueGameFinder()
    games_df = game_finder.get_data_frames()[0]
    
    # Filter for NBA games only (League ID '00')
    nba_games = games_df[games_df.LEAGUE_ID == '00'].copy()
    
    print(f"   Success! Downloaded {len(nba_games)} games.")

    print("2. Saving to Database...")
    
    # Create the database engine based on the config string
    engine = create_engine(db_connection_str)
    
    # Write to the 'games' table
    # chunksize=500 helps performance when we switch to Postgres later
    nba_games.to_sql('games', engine, if_exists='replace', index=False, chunksize=500)
    
    print("3. Pipeline Complete.")

if __name__ == "__main__":
    fetch_and_store_nba_data()