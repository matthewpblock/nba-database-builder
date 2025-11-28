import pandas as pd
from sqlalchemy import create_engine

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
TARGET_PLAYER = "Luka Dončić" 

def check_database_health():
    engine = create_engine(DB_URL)
    
    print("--- 🏥 DATABASE HEALTH CHECK ---")
    
    # 1. Count Rows
    tables = ['games', 'player_game_stats', 'play_by_play', 'player_matchups', 'game_rotations']
    print("\n📊 Row Counts:")
    with engine.connect() as conn:
        for t in tables:
            try:
                count = pd.read_sql(f"SELECT COUNT(*) as c FROM {t}", conn)['c'][0]
                print(f"   {t.ljust(20)}: {count:,} rows")
            except Exception:
                print(f"   {t.ljust(20)}: (Table not found)")

    # 2. The Nemesis Report
    print(f"\n🛡️  The '{TARGET_PLAYER}' Nemesis Report")
    print("   (Who has guarded him the most, and how many points did he score?)")
    
    query = f"""
    SELECT 
        def.full_name as Defender,
        t.abbreviation as Team,
        COUNT(DISTINCT m.game_id) as Games_Faced,
        
        -- FIX: Do NOT divide by 60. The column is already in minutes.
        SUM(m.matchup_minutes) as Mins_Guarded,
        
        SUM(m.points_allowed) as Pts_Scored,
        SUM(m.matchup_ast) as Ast_Allowed,
        SUM(m.matchup_tov) as Tov_Forced
    FROM player_matchups m
    JOIN players off ON m.off_player_id = off.player_id
    JOIN players def ON m.def_player_id = def.player_id
    
    -- Join stats to find the defender's team
    JOIN player_game_stats pgs_def 
      ON m.def_player_id = pgs_def.player_id 
      AND m.game_id = pgs_def.game_id
      
    JOIN teams t ON pgs_def.team_id = t.team_id
    
    -- Join stats to find the OFFENDER'S team (to filter teammates)
    JOIN player_game_stats pgs_off
      ON m.off_player_id = pgs_off.player_id
      AND m.game_id = pgs_off.game_id

    WHERE off.full_name = '{TARGET_PLAYER}'
      AND pgs_def.team_id != pgs_off.team_id
      
    GROUP BY def.full_name
    ORDER BY Mins_Guarded DESC
    LIMIT 10
    """
    
    try:
        df = pd.read_sql(query, engine)
        if not df.empty:
            df['Mins_Guarded'] = df['Mins_Guarded'].apply(lambda x: f"{x:.1f}")
            print(df.to_string(index=False))
        else:
            print("   (No data found. Try ingesting more games!)")
            
    except Exception as e:
        print(f"   ❌ Query Failed: {e}")

if __name__ == "__main__":
    check_database_health()