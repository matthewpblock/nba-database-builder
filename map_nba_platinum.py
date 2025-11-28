import pandas as pd
import time
from nba_api.stats.endpoints import (
    # --- CORE 1: PROFILES ---
    commonplayerinfo, commonteamroster,
    # --- CORE 2: CAREER ---
    playercareerstats, teamyearbyyearstats,
    # --- CORE 3: LEAGUE ---
    leaguestandings, leagueleaders,
    # --- CORE 4: GAME ---
    scoreboardv2, shotchartdetail,
    # --- CORE 5: BOX SCORES (V3) ---
    boxscoretraditionalv3, boxscoreadvancedv3, boxscoremiscv3,
    # --- CORE 6: EVENTS ---
    playbyplayv3,
    # --- ADDON 1: HUSTLE ---
    leaguehustlestatsplayer,
    # --- ADDON 2: TRACKING ---
    leaguedashptstats,
    # --- ADDON 3: LINEUPS ---
    leaguedashlineups,
    # --- ADDON 4: SYNERGY ---
    synergyplaytypes,
    # --- ADDON 5: DEFENSE & CLUTCH ---
    boxscorematchupsv3, leaguedashptdefend,  # <--- FIXED IMPORT
    leaguedashplayerclutch, leaguedashteamclutch,
    # --- ADDON 6: SHOT CONTEXT ---
    playerdashptshots, playerdashptshotdefend,
    # --- ADDON 7: ROTATION ---
    gamerotation, winprobabilitypbp,
    # --- NEW: OPPONENT DASHBOARD ---
    leaguedashoppptshot
)

FILE_NAME = "reports/nba_api_platinum_map.txt"

# CONFIG
SAMPLE_GAME_ID = '0042300301' 
SAMPLE_TEAM_ID = '1610612738' 
SAMPLE_PLAYER_ID = '2544'     
CURRENT_SEASON = '2023-24'

def log_to_file(msg):
    print(msg)
    with open(FILE_NAME, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def audit_endpoint(category, name, endpoint_call):
    try:
        log_to_file(f"   > Pinging {name}...")
        time.sleep(0.6) 
        
        dfs = endpoint_call.get_data_frames()
        
        if not dfs:
            log_to_file(f"     [WARNING] {name} returned no data.")
            return

        for i, df in enumerate(dfs):
            log_to_file(f"\n### [{category}] {name.upper()} - Table {i} ###")
            log_to_file(f"Row Count: {len(df)}")
            log_to_file("Columns & Data Types:")
            for col, dtype in df.dtypes.items():
                log_to_file(f"  - {col}: {dtype}")
            log_to_file("-" * 40)
        
    except Exception as e:
        log_to_file(f"\n### [{category}] {name.upper()} - FAILED ###")
        log_to_file(f"  Error: {e}")
        log_to_file("-" * 40)

def run_platinum_audit():
    with open(FILE_NAME, "w", encoding="utf-8") as f:
        f.write("=== NBA API PLATINUM SCHEMA MAP ===\n")
        f.write(f"Generated for Season: {CURRENT_SEASON}\n")
        f.write("=======================================\n\n")

    print(f"Starting Platinum Scan... Output: {FILE_NAME}\n")

    # --- THE BASICS ---
    audit_endpoint("PROFILE", "Player Info", 
                   commonplayerinfo.CommonPlayerInfo(player_id=SAMPLE_PLAYER_ID))
    audit_endpoint("GAME", "Scoreboard", 
                   scoreboardv2.ScoreboardV2(game_date='2024-05-21'))
    audit_endpoint("BOXSCORE", "Traditional V3", 
                   boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=SAMPLE_GAME_ID))
    audit_endpoint("EVENTS", "Play By Play V3", 
                   playbyplayv3.PlayByPlayV3(game_id=SAMPLE_GAME_ID))
    
    # --- THE ADVANCED SUITE ---
    audit_endpoint("HUSTLE", "Player Hustle Stats", 
                   leaguehustlestatsplayer.LeagueHustleStatsPlayer(season=CURRENT_SEASON))
    audit_endpoint("TRACKING", "Speed & Distance", 
                   leaguedashptstats.LeagueDashPtStats(season=CURRENT_SEASON, player_or_team='Player', pt_measure_type='SpeedDistance'))
    audit_endpoint("LINEUPS", "5-Man Lineups", 
                   leaguedashlineups.LeagueDashLineups(season=CURRENT_SEASON, group_quantity=5))
    audit_endpoint("SYNERGY", "Play Type: Isolation", 
                   synergyplaytypes.SynergyPlayTypes(season=CURRENT_SEASON, player_or_team_abbreviation='P', play_type_nullable='Isolation', type_grouping_nullable='offensive'))

    # --- THE SPECIALIST SUITE ---
    # FIXED: Using BoxScoreMatchupsV3
    audit_endpoint("DEFENSE", "Player Matchups", 
                   boxscorematchupsv3.BoxScoreMatchupsV3(game_id=SAMPLE_GAME_ID))
                   
    audit_endpoint("CLUTCH", "Clutch Stats", 
                   leaguedashplayerclutch.LeagueDashPlayerClutch(season=CURRENT_SEASON))
    audit_endpoint("SHOTS", "Contextual Shooting", 
                   playerdashptshots.PlayerDashPtShots(team_id=SAMPLE_TEAM_ID, player_id=SAMPLE_PLAYER_ID, season=CURRENT_SEASON))
    audit_endpoint("ROTATION", "Game Rotation", 
                   gamerotation.GameRotation(game_id=SAMPLE_GAME_ID))

    # --- NEW: OPPONENT DASHBOARD ---
    audit_endpoint("OPPONENT", "League Opponent Shooting", 
                   leaguedashoppptshot.LeagueDashOppPtShot(season=CURRENT_SEASON))

    print(f"\n✅ Platinum Audit Complete! Open '{FILE_NAME}' to view the full schema.")

if __name__ == "__main__":
    run_platinum_audit()