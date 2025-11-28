import sys
from sqlalchemy import create_engine, inspect
# Import all your models to check their definitions
from models import (
    Base, Team, Player, Game, PlayerGameStats, PlayByPlay, 
    HustleStats, TrackingStats, GameRotation, PlayerMatchups
)

DB_URL = 'sqlite:///nba_analysis.db'

def audit_schema():
    print(f"🕵️  Auditing Database Schema against Models...")
    print(f"    Database: {DB_URL}")
    
    engine = create_engine(DB_URL)
    inspector = inspect(engine)
    
    # List of (Model Class, Table Name) to check
    models_to_check = [
        (Team, 'teams'),
        (Player, 'players'),
        (Game, 'games'),
        (PlayerGameStats, 'player_game_stats'),
        (PlayByPlay, 'play_by_play'),
        (HustleStats, 'hustle_stats'),
        (TrackingStats, 'tracking_stats'),
        (GameRotation, 'game_rotations'),
        (PlayerMatchups, 'player_matchups')
    ]
    
    issues_found = 0
    tables_to_fix = []
    
    for model_cls, table_name in models_to_check:
        print(f"\n📋 Checking table: '{table_name}'...")
        
        # 1. Check if table exists
        if not inspector.has_table(table_name):
            print(f"   ❌ CRITICAL: Table '{table_name}' does not exist.")
            issues_found += 1
            tables_to_fix.append(table_name)
            continue
            
        # 2. Get actual columns in the .db file
        db_columns = [c['name'] for c in inspector.get_columns(table_name)]
        
        # 3. Get expected columns from models.py
        model_columns = [c.name for c in model_cls.__table__.columns]
        
        # 4. Find gaps
        missing_in_db = [c for c in model_columns if c not in db_columns]
        
        if missing_in_db:
            print(f"   ❌ MISSING COLUMNS (Code expects them, DB lacks them):")
            for c in missing_in_db:
                print(f"      - {c}")
            issues_found += 1
            tables_to_fix.append(table_name)
        else:
            print(f"   ✅ Schema Match ({len(db_columns)} columns)")
            
    print("\n" + "="*50)
    if issues_found > 0:
        print(f"❌ AUDIT FAILED: Found {issues_found} broken tables.")
        print("\n🔧 RECOMMENDED FIXES:")
        for t in tables_to_fix:
            print(f"   1. Open 'fix_table.py'")
            print(f"   2. Set TARGET_TABLE = '{t}'")
            print(f"   3. Run 'python fix_table.py'")
            print("   -----------------------------")
        print("   (Then run 'ingest_season.py' to refill the data)")
    else:
        print("✅ AUDIT PASSED: Your database schema is perfect.")

if __name__ == "__main__":
    audit_schema()