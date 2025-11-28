import pandas as pd
import time
import logging
from sqlalchemy import create_engine, text
from requests.exceptions import ReadTimeout, ConnectionError, JSONDecodeError
from nba_api.stats.endpoints import (
    boxscoretraditionalv3,
    boxscoreadvancedv3,
    boxscoremiscv3,
    boxscorematchupsv3,
    playbyplayv3,
    hustlestatsboxscore,
    gamerotation
)
from models import PlayerGameStats, PlayByPlay, PlayerMatchups, HustleStats, GameRotation

# --- CONFIGURATION ---
DB_URL = 'sqlite:///nba_analysis.db'
engine = create_engine(DB_URL)

# --- THE ROSETTA STONE ---
COLUMN_MAP = {
    # Identity
    'game_id': ['gameId', 'GAME_ID'],
    'team_id': ['teamId', 'TEAM_ID', 'offensiveTeamId'],
    'player_id': ['personId', 'PLAYER_ID', 'person_id'],
    'minutes': ['minutes', 'MIN'],
    
    # Traditional Stats
    'pts': ['points', 'PTS'],
    'reb': ['reboundsTotal', 'REB'],
    'ast': ['assists', 'AST'],
    'stl': ['steals', 'STL'],
    'blk': ['blocks', 'BLK'],
    'tov': ['turnovers', 'TOV'],
    'pf': ['foulsPersonal', 'PF'],
    'plus_minus': ['plusMinusPoints', 'PLUS_MINUS'],
    'fgm': ['fieldGoalsMade', 'FGM'],
    'fga': ['fieldGoalsAttempted', 'FGA'],
    'fg_pct': ['fieldGoalsPercentage', 'FG_PCT'],
    'fg3m': ['threePointersMade', 'FG3M'],
    'fg3a': ['threePointersAttempted', 'FG3A'],
    'fg3_pct': ['threePointersPercentage', 'FG3_PCT'],
    'ftm': ['freeThrowsMade', 'FTM'],
    'fta': ['freeThrowsAttempted', 'FTA'],
    'ft_pct': ['freeThrowsPercentage', 'FT_PCT'],

    # Advanced
    'off_rating': ['offensiveRating'],
    'def_rating': ['defensiveRating'],
    'net_rating': ['netRating'],
    'usg_pct': ['usagePercentage'],
    'pace': ['pace'],
    'pie': ['PIE'],

    # Matchups (V3 Fixed)
    'off_player_id': ['personIdOff', 'offensivePlayerId'],
    'def_player_id': ['personIdDef', 'defensivePlayerId'],
    'matchup_minutes': ['matchupMinutes'],
    'points_allowed': ['playerPoints', 'playerPts'],
    'matchup_ast': ['matchupAssists'],
    'matchup_tov': ['matchupTurnovers'],
    'matchup_blk': ['matchupBlocks'],
    
    # Hustle
    'screen_assists': ['SCREEN_ASSISTS'],
    'deflections': ['DEFLECTIONS'],
    'loose_balls_recovered': ['LOOSE_BALLS_RECOVERED'],
    'charges_drawn': ['CHARGES_DRAWN'],
    'contested_shots': ['CONTESTED_SHOTS'],
    'box_outs': ['BOX_OUTS'],
    
    # PBP
    'event_num': ['actionNumber', 'EVENTNUM'],
    'period': ['period', 'PERIOD'],
    'clock': ['clock', 'PCTIMESTRING'],
    'action_type': ['actionType', 'EVENTMSGTYPE'],
    'sub_type': ['subType', 'EVENTMSGACTIONTYPE'],
    'description': ['description', 'HOMEDESCRIPTION', 'VISITORDESCRIPTION'],
    'shot_result': ['shotResult'],
    'loc_x': ['xLegacy', 'LOC_X'],
    'loc_y': ['yLegacy', 'LOC_Y'],
    'margin': ['pointsTotal', 'SCOREMARGIN']
}

def clean_existing_game(game_id):
    """Deletes existing data for a game to prevent Unique Constraint errors."""
    tables = [
        'player_game_stats', 'play_by_play', 'hustle_stats', 
        'tracking_stats', 'game_rotations', 'player_matchups'
    ]
    with engine.connect() as conn:
        for table in tables:
            try:
                conn.execute(text(f"DELETE FROM {table} WHERE game_id = :gid"), {'gid': game_id})
            except Exception:
                pass
        conn.commit()

def prepare_df(df, table_model):
    """Renames columns and drops any that don't match the database schema."""
    final_cols = {}
    for db_col, api_options in COLUMN_MAP.items():
        for api_col in api_options:
            if api_col in df.columns:
                final_cols[api_col] = db_col
                break
    
    df_renamed = df.rename(columns=final_cols)
    valid_db_cols = [c.name for c in table_model.__table__.columns]
    
    return df_renamed[[c for c in df_renamed.columns if c in valid_db_cols]].copy()

def ingest_game(game_id, full_mode=True):
    logging.info(f"Starting ingest for Game {game_id}...")
    
    # Checkpoint: Wipe old data first
    clean_existing_game(game_id)
    
    # CRITICAL: We want to catch 'normal' errors (like missing data) but 
    # LET THROUGH 'critical' errors (like Timeouts) so the Manager script detects them.
    CRITICAL_ERRORS = (ReadTimeout, ConnectionError, JSONDecodeError)

    # --- 1. BOX SCORES ---
    try:
        trad = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id).get_data_frames()[0]
        adv = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id).get_data_frames()[0]
        misc = boxscoremiscv3.BoxScoreMiscV3(game_id=game_id).get_data_frames()[0]
        
        merged = pd.merge(trad, adv, on=['personId', 'teamId'])
        merged = pd.merge(merged, misc, on=['personId', 'teamId'])
        
        df_clean = prepare_df(merged, PlayerGameStats)
        df_clean['game_id'] = game_id 
        
        df_clean.to_sql('player_game_stats', engine, if_exists='append', index=False)
        logging.info(f"Saved {len(df_clean)} player stats for game {game_id}.")
        time.sleep(0.6) # <--- ADD THIS
    except CRITICAL_ERRORS: raise 
    except Exception as e: logging.error(f"Error fetching Box Scores for {game_id}: {e}")

    # --- 2. PLAY BY PLAY ---
    if full_mode:
        try:
            pbp = playbyplayv3.PlayByPlayV3(game_id=game_id).get_data_frames()[0]
            df_pbp = prepare_df(pbp, PlayByPlay)
            df_pbp['game_id'] = game_id
            df_pbp = df_pbp.drop_duplicates(subset=['event_num'])
            df_pbp.to_sql('play_by_play', engine, if_exists='append', index=False)
            logging.info(f"Saved {len(df_pbp)} PBP events for game {game_id}.")
            time.sleep(0.6) # <--- ADD THIS
        except CRITICAL_ERRORS: raise
        except Exception as e: logging.error(f"Error fetching PBP for {game_id}: {e}")

    # --- 3. HUSTLE STATS ---
    if full_mode:
        try:
            hustle = hustlestatsboxscore.HustleStatsBoxScore(game_id=game_id).get_data_frames()[1]
            df_hustle = prepare_df(hustle, HustleStats)
            df_hustle['game_id'] = game_id
            df_hustle.to_sql('hustle_stats', engine, if_exists='append', index=False)
            logging.info(f"Saved {len(df_hustle)} hustle records for game {game_id}.")
            time.sleep(0.6) # <--- ADD THIS
        except CRITICAL_ERRORS: raise
        except Exception as e: logging.error(f"Error fetching Hustle stats for {game_id}: {e}")

    # --- 4. MATCHUPS ---
    if full_mode:
        try:
            matchups = boxscorematchupsv3.BoxScoreMatchupsV3(game_id=game_id).get_data_frames()[0]
            df_match = prepare_df(matchups, PlayerMatchups)
            df_match['game_id'] = game_id
            df_match = df_match.dropna(subset=['off_player_id', 'def_player_id'])
            df_match['off_player_id'] = df_match['off_player_id'].astype(int)
            df_match['def_player_id'] = df_match['def_player_id'].astype(int)
            df_match = df_match.drop_duplicates(subset=['off_player_id', 'def_player_id'])
            df_match.to_sql('player_matchups', engine, if_exists='append', index=False)
            logging.info(f"Saved {len(df_match)} matchup records for game {game_id}.")
            time.sleep(0.6) # <--- ADD THIS
        except CRITICAL_ERRORS: raise
        except Exception as e: logging.error(f"Error fetching Matchups for {game_id}: {e}")

    # --- 5. ROTATIONS ---
    if full_mode:
        try:
            rot = gamerotation.GameRotation(game_id=game_id).get_data_frames()[0]
            df_rot = prepare_df(rot, GameRotation)
            df_rot['game_id'] = game_id
            df_rot.to_sql('game_rotations', engine, if_exists='append', index=False)
            logging.info(f"Saved {len(df_rot)} rotation shifts for game {game_id}.")
            # No sleep needed here, we sleep at the end of the function
        except CRITICAL_ERRORS: raise
        except Exception as e: logging.error(f"Error fetching Rotations for {game_id}: {e}")

    logging.info(f"Finished ingest for Game {game_id}.")
    time.sleep(1.0)