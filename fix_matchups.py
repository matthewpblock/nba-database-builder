from sqlalchemy import create_engine, text
from models import init_db

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'

def reset_matchups():
    engine = create_engine(DB_URL)
    print(f"🔌 Connecting to {DB_URL}...")
    
    print("🗑️  Dropping 'player_matchups' table...")
    with engine.connect() as conn:
        # This deletes the table entirely
        conn.execute(text("DROP TABLE IF EXISTS player_matchups"))
        conn.commit()
        
    print("✨ Re-initializing database schema...")
    # This checks models.py and creates any missing tables (our fresh player_matchups)
    init_db(DB_URL)
    
    print("✅ Done! You can now run 'ingest_season.py' to refill the data.")

if __name__ == "__main__":
    reset_matchups()