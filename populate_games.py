import pandas as pd
from sqlalchemy import create_engine
from nba_api.stats.endpoints import leaguegamefinder

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
TARGET_SEASON = '2024-25'

def populate_games_table():
    engine = create_engine(DB_URL)
    print(f"📅 Fetching {TARGET_SEASON} schedule from NBA API...")
    
    # 1. Fetch all games for the season
    finder = leaguegamefinder.LeagueGameFinder(
        league_id_nullable='00',
        season_nullable=TARGET_SEASON,
        season_type_nullable='Regular Season'
    )
    all_games = finder.get_data_frames()[0]
    
    # 2. Split into Home and Away rows
    # The API returns 2 rows per game. We split them to join them back together.
    # "vs." implies Home, "@" implies Away.
    home_rows = all_games[all_games['MATCHUP'].str.contains('vs.')].copy()
    away_rows = all_games[all_games['MATCHUP'].str.contains('@')].copy()
    
    print(f"   Found {len(home_rows)} unique games.")
    
    # 3. Join on GAME_ID to get both Team IDs in one row
    # This ensures we get the correct Home_Team_ID and Away_Team_ID
    merged_games = pd.merge(
        home_rows, 
        away_rows[['GAME_ID', 'TEAM_ID', 'PTS']], 
        on='GAME_ID', 
        suffixes=('_home', '_away')
    )
    
    # 4. Format for Database
    # We select and rename columns to match models.py
    db_data = pd.DataFrame({
        'game_id': merged_games['GAME_ID'],
        'game_date': pd.to_datetime(merged_games['GAME_DATE']).dt.date,
        'season_id': merged_games['SEASON_ID'],
        'matchup': merged_games['MATCHUP'],
        'home_team_id': merged_games['TEAM_ID_home'],
        'away_team_id': merged_games['TEAM_ID_away'],
        'home_pts': merged_games['PTS_home'],
        'away_pts': merged_games['PTS_away']
    })
    
    # 5. Save to SQL
    # We use 'append' because we just created the table empty
    db_data.to_sql('games', engine, if_exists='append', index=False)
    
    print(f"✅ Successfully inserted {len(db_data)} games into the 'games' table.")

if __name__ == "__main__":
    populate_games_table()