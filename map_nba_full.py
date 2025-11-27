import pandas as pd
import time
from nba_api.stats.endpoints import (
    # --- PROFILES ---
    commonplayerinfo, commonteamroster, teaminfocommon,
    # --- CAREER & SEASON ---
    playercareerstats, teamyearbyyearstats,
    # --- LEAGUE WIDE ---
    leaguestandings, leagueleaders, leaguedashteamstats,
    # --- GAME DATA ---
    scoreboardv2, leaguegamefinder,
    # --- SHOTS & PLAYS ---
    shotchartdetail, playbyplayv2,
    # --- BOX SCORES (V3) ---
    boxscoretraditionalv3, boxscoreadvancedv3, boxscoremiscv3,
    boxscorefourfactorsv3, boxscoreusagev3, boxscorescoringv3
)

# OUTPUT FILE NAME
FILE_NAME = "nba_api_comprehensive_map.txt"

# SAMPLE IDs (Using Lebron and Celtics for robust data)
SAMPLE_GAME_ID = '0022300001' 
SAMPLE_TEAM_ID = '1610612738' 
SAMPLE_PLAYER_ID = '2544'     

def log_to_file(msg):
    """Writes a line to the text file and prints to console"""
    print(msg)
    with open(FILE_NAME, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def audit_endpoint(category, name, endpoint_call):
    try:
        log_to_file(f"   > Pinging {name}...")
        
        # Pause to respect rate limits
        time.sleep(0.6) 
        
        # Get ALL dataframes (some endpoints return 2 or 3 tables)
        dfs = endpoint_call.get_data_frames()
        
        if not dfs:
            log_to_file(f"     [WARNING] {name} returned no data.")
            return

        for i, df in enumerate(dfs):
            log_to_file(f"\n### [{category}] {name.upper()} - Table {i} ###")
            log_to_file(f"Row Count: {len(df)}")
            log_to_file("Columns & Data Types:")
            
            for col, dtype in df.dtypes.items():
                # We format this to look like "  - COLUMN_NAME : int64"
                log_to_file(f"  - {col}: {dtype}")
            
            log_to_file("-" * 40)
        
    except Exception as e:
        log_to_file(f"\n### [{category}] {name.upper()} - FAILED ###")
        log_to_file(f"  Error: {e}")
        log_to_file("-" * 40)

def run_comprehensive_audit():
    # Initialize/Clear the file
    with open(FILE_NAME, "w", encoding="utf-8") as f:
        f.write("=== NBA API COMPREHENSIVE SCHEMA MAP ===\n")
        f.write("Generated via automated audit script.\n")
        f.write("========================================\n\n")

    print(f"Starting Deep Scan... Output will be saved to: {FILE_NAME}\n")

    # 1. PROFILES
    audit_endpoint("PROFILE", "Player Info", 
                   commonplayerinfo.CommonPlayerInfo(player_id=SAMPLE_PLAYER_ID))
    
    audit_endpoint("PROFILE", "Team Roster", 
                   commonteamroster.CommonTeamRoster(team_id=SAMPLE_TEAM_ID, season='2023-24'))

    # 2. CAREER STATS
    audit_endpoint("CAREER", "Player Career Stats", 
                   playercareerstats.PlayerCareerStats(player_id=SAMPLE_PLAYER_ID))
    
    audit_endpoint("CAREER", "Team History", 
                   teamyearbyyearstats.TeamYearByYearStats(team_id=SAMPLE_TEAM_ID))

    # 3. LEAGUE DASHBOARDS
    audit_endpoint("LEAGUE", "Standings", 
                   leaguestandings.LeagueStandings(season='2023-24'))
    
    audit_endpoint("LEAGUE", "League Leaders", 
                   leagueleaders.LeagueLeaders(season='2023-24'))

    # 4. GAME & SHOT DATA
    audit_endpoint("GAME", "Scoreboard (Daily)", 
                   scoreboardv2.ScoreboardV2(game_date='2023-10-25'))
    
    audit_endpoint("GAME", "Shot Chart", 
                   shotchartdetail.ShotChartDetail(
                       team_id=SAMPLE_TEAM_ID, 
                       player_id=0, 
                       game_id_nullable=SAMPLE_GAME_ID,
                       context_measure_simple='FGA'))

    # 5. DETAILED BOX SCORES (V3)
    audit_endpoint("BOXSCORE", "Traditional V3", 
                   boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=SAMPLE_GAME_ID))
    
    audit_endpoint("BOXSCORE", "Advanced V3", 
                   boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=SAMPLE_GAME_ID))
    
    audit_endpoint("BOXSCORE", "Misc V3", 
                   boxscoremiscv3.BoxScoreMiscV3(game_id=SAMPLE_GAME_ID))

    # 6. EVENTS
    audit_endpoint("EVENTS", "Play By Play V2", 
                   playbyplayv2.PlayByPlayV2(game_id=SAMPLE_GAME_ID))

    print(f"\n✅ Audit Complete! Open '{FILE_NAME}' to see the results.")

if __name__ == "__main__":
    run_comprehensive_audit()