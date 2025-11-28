import sys
import logging
from sqlalchemy import create_engine, text
from models import init_db, Base
from logger_config import setup_logger

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
    logging.info(f"Connecting to {DB_URL}...")
    
    # --- Safety Check ---
    # Get a list of all table names defined in models.py
    all_known_tables = Base.metadata.tables.keys()
    if table_name_to_reset not in all_known_tables:
        logging.error(f"Table '{table_name_to_reset}' is not a valid table defined in models.py.")
        logging.error(f"Valid options are: {list(all_known_tables)}")
        return

    logging.info(f"Dropping '{table_name_to_reset}' table...")
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name_to_reset}"))
        conn.commit()
    logging.info(f"Successfully dropped table '{table_name_to_reset}'.")
        
    logging.info("Re-initializing database schema...")
    # This checks models.py and creates any missing tables (like the one we just dropped)
    init_db(DB_URL)
    
    logging.info(f"Table '{table_name_to_reset}' has been reset successfully.")

if __name__ == "__main__":
    setup_logger()
    logging.info(f"--- Running fix_table script for table: {TARGET_TABLE} ---")
    reset_table(TARGET_TABLE)
    logging.info(f"--- fix_table script finished ---")