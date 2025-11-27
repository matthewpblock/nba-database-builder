import os
import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
from sqlalchemy import create_engine
from dotenv import load_dotenv
from urllib.parse import urlparse


def fetch_and_store_nba_data():
    """
    Fetch NBA game data from the NBA API and store it in a SQL database.

    Data source and filtering:
    - Uses nba_api.stats.endpoints.LeagueGameFinder with league_id_nullable='00'
      where '00' = NBA, '10' = WNBA, '20' = G-League.
    - By default this function saves all games returned by the API (including
      preseason); adjust filtering if you want only regular season or playoffs.
    """
    load_dotenv()
    db_connection_str = os.getenv("DATABASE_URL", "sqlite:///nba_analysis.db")

    # Normalize legacy Heroku-style URLs so SQLAlchemy doesn't error:
    # SQLAlchemy expects "postgresql://" but some environments provide "postgres://"
    if db_connection_str.startswith("postgres://"):
        db_connection_str = db_connection_str.replace("postgres://", "postgresql://", 1)
    
    parsed = urlparse(db_connection_str)
    print(f"--- Configuration ---")
    db_scheme = parsed.scheme or db_connection_str.split(':')[0]
    print(f"Target Database: {db_scheme}")
    print(f"Target Database URL: {db_connection_str}")

    print("1. Requesting data from NBA API (NBA League Only)...")
    
    # '00' = NBA, '10' = WNBA, '20' = G-League
    game_finder = leaguegamefinder.LeagueGameFinder(league_id_nullable='00')
    
    games_df = game_finder.get_data_frames()[0]
    
    print(f"   Success! Downloaded {len(games_df)} NBA games.")
    
    # Optional: Filter out Pre-season games if you want strict regular season/playoffs
    # Pre-season Season_IDs usually start with '1' (e.g., '12023'). Regular season is '2', Playoffs '4'.
    # For now, we will keep everything to be safe.

    print("2. Saving to Database...")
    engine = create_engine(db_connection_str)
    
    # This will OVERWRITE the previous 'dirty' table with this new clean one
    games_df.to_sql('games', engine, if_exists='replace', index=False, chunksize=500)
    
    print("3. Pipeline Complete.")
    
    # --- VERIFICATION ---
    print("\n--- Data Sample ---")
    print(games_df[['SEASON_ID', 'GAME_DATE', 'MATCHUP', 'PTS']].head())
    print("\n--- All Column Names ---")
    # Convert to list for cleaner printing
    print(games_df.columns.tolist())

if __name__ == "__main__":
    fetch_and_store_nba_data()