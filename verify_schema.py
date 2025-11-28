from sqlalchemy import create_engine, inspect

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'

def verify_table():
    engine = create_engine(DB_URL)
    inspector = inspect(engine)
    
    table_name = 'player_matchups'
    
    print(f"🕵️  Inspecting table: '{table_name}'...")
    
    if not inspector.has_table(table_name):
        print(f"❌ Error: Table '{table_name}' does not exist.")
        print("   (Did you run 'fix_matchups.py' yet?)")
        return

    columns = inspector.get_columns(table_name)
    col_names = [c['name'] for c in columns]
    
    print(f"   Found {len(col_names)} columns:")
    print(f"   {col_names}")
    
    # The moment of truth
    if 'team_id' in col_names:
        print("\n✅ SUCCESS: 'team_id' column is present!")
        print("   You are safe to run 'ingest_season.py' now.")
    else:
        print("\n❌ FAILURE: 'team_id' is MISSING.")
        print("   Please check your 'models.py' file to ensure the column is defined there,")
        print("   then run 'fix_matchups.py' again.")

if __name__ == "__main__":
    verify_table()