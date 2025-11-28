import sys
from sqlalchemy import create_engine, text
from models import init_db, Base

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
# --- Which table do you want to reset? ---
# Options: 'player_matchups', 'player_game_stats', 'play_by_play', 'hustle_stats', 'game_rotations'
TARGET_TABLE = 'player_matchups'

def reset_table(table_name_to_reset):
    """
    Drops a specific table and then re-initializes the database schema,
    which recreates the table based on its definition in models.py.
    """
    engine = create_engine(DB_URL)
    print(f"🔌 Connecting to {DB_URL}...")
    
    # --- Safety Check ---
    # Get a list of all table names defined in models.py
    all_known_tables = Base.metadata.tables.keys()
    if table_name_to_reset not in all_known_tables:
        print(f"❌ Error: Table '{table_name_to_reset}' is not a valid table defined in models.py.")
        print(f"   Valid options are: {list(all_known_tables)}")
        return

    print(f"🗑️  Dropping '{table_name_to_reset}' table...")
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name_to_reset}"))
        conn.commit()
        
    print("✨ Re-initializing database schema...")
    # This checks models.py and creates any missing tables (like the one we just dropped)
    init_db(DB_URL)
    
    print(f"✅ Done! The '{table_name_to_reset}' table has been reset. You can now run an ingestion script to refill it.")

if __name__ == "__main__":
    reset_table(TARGET_TABLE)