import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///nba_analysis.db')

print("--- 📊 TABLE COUNTS ---")
for table in ['games', 'teams', 'play_by_play']:
    try:
        count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", engine).iloc[0,0]
        status = "✅ OK" if count > 0 else "❌ EMPTY"
        print(f"{table.ljust(15)}: {count} rows  {status}")
    except Exception as e:
        print(f"{table.ljust(15)}: (Table Not Found)")