import pandas as pd
from sqlalchemy import create_engine
from nba_api.stats.static import players, teams
from models import Player, Team

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'

def populate_dimensions():
    engine = create_engine(DB_URL)
    print(f"🔌 Connecting to {DB_URL}...")
    
    # --- 1. POPULATE TEAMS ---
    print("🏀 Populating Teams...")
    nba_teams = teams.get_teams()
    df_teams = pd.DataFrame(nba_teams)
    
    # FIX: Explicitly select ONLY the source columns we want first.
    # This prevents the "Duplicate Column" error by dropping the original 'nickname' 
    # before we rename 'full_name' to take its place.
    df_teams = df_teams[['id', 'abbreviation', 'full_name', 'city', 'state', 'year_founded']]
    
    # Rename to match our Schema (models.py)
    df_teams = df_teams.rename(columns={
        'id': 'team_id',
        'full_name': 'nickname' # We use the full name (Atlanta Hawks) as the primary name
    })
    
    # Save to SQL
    df_teams.to_sql('teams', engine, if_exists='replace', index=False)
    print(f"   ✅ Added {len(df_teams)} teams.")

    # --- 2. POPULATE PLAYERS ---
    print("👤 Populating Players (History & Active)...")
    nba_players = players.get_players()
    df_players = pd.DataFrame(nba_players)
    
    # Select specific columns to be safe
    df_players = df_players[['id', 'full_name', 'first_name', 'last_name', 'is_active']]
    
    # Rename to match our Schema
    df_players = df_players.rename(columns={
        'id': 'player_id'
    })
    
    # Save to SQL
    df_players.to_sql('players', engine, if_exists='replace', index=False)
    print(f"   ✅ Added {len(df_players)} players.")
    
    print("\n🎉 Dimensions populated! Run 'check_db.py' again to see the report.")

if __name__ == "__main__":
    populate_dimensions()