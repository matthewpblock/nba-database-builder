import pandas as pd
from sqlalchemy import create_engine, text
from nba_api.stats.static import players, teams
from models import init_db

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'

def reset_dimensions():
    engine = create_engine(DB_URL)
    print(f"🔌 Connecting to {DB_URL}...")
    
    # --- 1. DROP BROKEN TABLES ---
    print("🗑️  Dropping outdated dimension tables...")
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS teams"))
        conn.execute(text("DROP TABLE IF EXISTS players"))
        conn.commit()

    # --- 2. RE-INITIALIZE SCHEMA ---
    print("✨ Re-creating tables with Platinum Schema...")
    init_db(DB_URL)

    # --- 3. REFILL TEAMS ---
    print("🏀 Populating Teams...")
    nba_teams = teams.get_teams()
    df_teams = pd.DataFrame(nba_teams)
    
    # FIX: Select columns FIRST to drop the original 'nickname' column
    # This prevents the DuplicateColumnError when we rename full_name
    df_teams = df_teams[['id', 'abbreviation', 'full_name', 'city', 'state', 'year_founded']]
    
    # Now safe to rename
    df_teams = df_teams.rename(columns={
        'id': 'team_id',
        'full_name': 'nickname'
    })
    
    # Save (using 'append' to respect the new schema structure)
    df_teams.to_sql('teams', engine, if_exists='append', index=False)
    print(f"   ✅ Added {len(df_teams)} teams.")

    # --- 4. REFILL PLAYERS ---
    print("👤 Populating Players...")
    nba_players = players.get_players()
    df_players = pd.DataFrame(nba_players)
    
    # Select columns first
    df_players = df_players[['id', 'full_name', 'first_name', 'last_name', 'is_active']]
    
    # Rename
    df_players = df_players.rename(columns={
        'id': 'player_id'
    })
    
    df_players.to_sql('players', engine, if_exists='append', index=False)
    print(f"   ✅ Added {len(df_players)} players.")
    
    print("\n🎉 Dimensions repaired! You can now run 'audit_schema.py' to confirm.")

if __name__ == "__main__":
    reset_dimensions()