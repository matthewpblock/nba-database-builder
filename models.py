from sqlalchemy import Column, Integer, String, Float, Date, Boolean, ForeignKey, create_engine
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

# ==========================================
# 1. CORE DIMENSIONS (The "Who")
# ==========================================

class Team(Base):
    __tablename__ = 'teams'
    team_id = Column(Integer, primary_key=True)
    abbreviation = Column(String(10))
    nickname = Column(String(50))
    city = Column(String(50))
    state = Column(String(50))
    year_founded = Column(Integer)
    arena = Column(String(100))

class Player(Base):
    __tablename__ = 'players'
    player_id = Column(Integer, primary_key=True)
    full_name = Column(String(100))
    first_name = Column(String(50))
    last_name = Column(String(50))
    is_active = Column(Boolean)
    
    # Draft & Bio (From CommonPlayerInfo)
    school = Column(String(100))
    country = Column(String(50))
    height = Column(String(10))
    weight = Column(String(10))
    draft_year = Column(String(10))
    draft_round = Column(String(10))
    draft_number = Column(String(10))

# ==========================================
# 2. CORE FACTS (The "What")
# ==========================================

class Game(Base):
    __tablename__ = 'games'
    game_id = Column(String(20), primary_key=True)
    game_date = Column(Date)
    season_id = Column(String(10))
    matchup = Column(String(50))
    
    home_team_id = Column(Integer, ForeignKey('teams.team_id'))
    away_team_id = Column(Integer, ForeignKey('teams.team_id'))
    home_pts = Column(Integer)
    away_pts = Column(Integer)

class PlayerGameStats(Base):
    """
    Standard Box Score Stats (Points, Rebounds, Assists)
    """
    __tablename__ = 'player_game_stats'
    
    game_id = Column(String(20), ForeignKey('games.game_id'), primary_key=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), primary_key=True)
    team_id = Column(Integer, ForeignKey('teams.team_id'))
    
    minutes = Column(String(10))
    pts = Column(Integer)
    reb = Column(Integer)
    ast = Column(Integer)
    stl = Column(Integer)
    blk = Column(Integer)
    tov = Column(Integer)
    pf = Column(Integer)
    plus_minus = Column(Float)
    
    # Shooting
    fgm = Column(Integer)
    fga = Column(Integer)
    fg_pct = Column(Float)
    fg3m = Column(Integer)
    fg3a = Column(Integer)
    fg3_pct = Column(Float)
    ftm = Column(Integer)
    fta = Column(Integer)
    ft_pct = Column(Float)

    # Advanced
    off_rating = Column(Float)
    def_rating = Column(Float)
    net_rating = Column(Float)
    usg_pct = Column(Float)
    pace = Column(Float)
    pie = Column(Float)

# ==========================================
# 3. THE "PLATINUM" ADD-ONS
# ==========================================

class PlayByPlay(Base):
    __tablename__ = 'play_by_play'
    
    game_id = Column(String(20), ForeignKey('games.game_id'), primary_key=True)
    event_num = Column(Integer, primary_key=True)
    
    period = Column(Integer)
    clock = Column(String(10))
    team_id = Column(Integer, ForeignKey('teams.team_id'))
    player_id = Column(Integer, ForeignKey('players.player_id'))
    
    action_type = Column(String(50))
    sub_type = Column(String(50))
    description = Column(String(255))
    shot_result = Column(String(20))
    loc_x = Column(Integer)
    loc_y = Column(Integer)
    margin = Column(Integer)

class HustleStats(Base):
    """
    The "Dirty Work" stats (Screen Assists, Deflections, Charges)
    """
    __tablename__ = 'hustle_stats'
    
    game_id = Column(String(20), ForeignKey('games.game_id'), primary_key=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), primary_key=True)
    team_id = Column(Integer, ForeignKey('teams.team_id'))
    
    screen_assists = Column(Integer)
    deflections = Column(Integer)
    loose_balls_recovered = Column(Integer)
    charges_drawn = Column(Integer)
    contested_shots = Column(Integer)
    box_outs = Column(Integer)

class TrackingStats(Base):
    """
    Speed & Distance (From LeagueDashPtStats)
    """
    __tablename__ = 'tracking_stats'
    
    game_id = Column(String(20), ForeignKey('games.game_id'), primary_key=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), primary_key=True)
    
    dist_miles = Column(Float)       # API: DIST_MILES
    avg_speed = Column(Float)        # API: AVG_SPEED
    avg_speed_off = Column(Float)    # API: AVG_SPEED_OFF
    avg_speed_def = Column(Float)    # API: AVG_SPEED_DEF

class GameRotation(Base):
    """
    Visualizing substitutions and lineup durations
    """
    __tablename__ = 'game_rotations'
    
    # FIX: Made row_id the ONLY primary key to allow Autoincrement
    row_id = Column(Integer, primary_key=True, autoincrement=True) 
    
    # game_id is now just a Foreign Key, not part of the Primary Key
    game_id = Column(String(20), ForeignKey('games.game_id'))
    
    player_id = Column(Integer, ForeignKey('players.player_id'))
    team_id = Column(Integer, ForeignKey('teams.team_id'))
    
    in_time_real = Column(Float)  # API: IN_TIME_REAL
    out_time_real = Column(Float) # API: OUT_TIME_REAL
    pt_diff = Column(Float)       # API: PT_DIFF (Plus/Minus for this specific shift)

class PlayerMatchups(Base):
    """
    Who guarded whom? (From BoxScoreMatchupsV3)
    """
    __tablename__ = 'player_matchups'
    
    game_id = Column(String(20), ForeignKey('games.game_id'), primary_key=True)
    off_player_id = Column(Integer, ForeignKey('players.player_id'), primary_key=True)
    def_player_id = Column(Integer, ForeignKey('players.player_id'), primary_key=True)
    
    matchup_minutes = Column(Float) # Time spent guarding this player
    points_allowed = Column(Integer)
    matchup_ast = Column(Integer)
    matchup_tov = Column(Integer)
    matchup_blk = Column(Integer)

# ==========================================
# INITIALIZATION
# ==========================================

def init_db(db_url):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    print(f"✅ Platinum Schema created at: {db_url}")

if __name__ == "__main__":
    init_db('sqlite:///nba_analysis.db')