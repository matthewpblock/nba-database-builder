import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import cross_val_score, KFold
import os

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
# Include Regular Season, Playoffs, Play-In
TARGET_SEASON_IDS = "('22024', '42024', '52024')"
OUTPUT_DIR = 'reports'

def get_data():
    engine = create_engine(DB_URL)
    print(f"   Fetching 2024-25 data...")
    
    query = f"""
    WITH q3_scores AS (
        SELECT game_id, score_home, score_away
        FROM play_by_play
        WHERE period = 3 
        GROUP BY game_id HAVING event_num = MAX(event_num)
    ),
    final_scores AS (
        SELECT game_id, score_home, score_away
        FROM play_by_play
        WHERE period = 4
        GROUP BY game_id HAVING event_num = MAX(event_num)
    )
    SELECT 
        g.game_id,
        t_home.abbreviation as home_team,
        t_away.abbreviation as away_team,
        q3.score_home as q3_h,
        q3.score_away as q3_a,
        final.score_home as f_h,
        final.score_away as f_a
    FROM games g
    JOIN q3_scores q3 ON g.game_id = q3.game_id
    JOIN final_scores final ON g.game_id = final.game_id
    JOIN teams t_home ON g.home_team_id = t_home.team_id
    JOIN teams t_away ON g.away_team_id = t_away.team_id
    WHERE g.season_id IN {TARGET_SEASON_IDS}
    """
    
    try:
        raw_df = pd.read_sql(query, engine)
        if raw_df.empty: return pd.DataFrame()

        cols = ['q3_h', 'q3_a', 'f_h', 'f_a']
        for c in cols: raw_df[c] = pd.to_numeric(raw_df[c], errors='coerce')
        
        raw_df[cols] = raw_df.groupby('game_id')[cols].ffill()
        raw_df = raw_df.dropna()

        raw_df['Q3_Margin'] = raw_df['q3_h'] - raw_df['q3_a']
        raw_df['Final_Margin'] = raw_df['f_h'] - raw_df['f_a']
        
        # Stack Data
        home_df = raw_df[['home_team', 'Q3_Margin', 'Final_Margin']].copy()
        home_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        away_df = raw_df[['away_team', 'Q3_Margin', 'Final_Margin']].copy()
        away_df['Q3_Lead'] = away_df['Q3_Margin'] * -1
        away_df['Final_Result'] = away_df['Final_Margin'] * -1
        
        # --- FIX: Select 'away_team' BEFORE renaming ---
        away_df = away_df[['away_team', 'Q3_Lead', 'Final_Result']]
        away_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        return pd.concat([home_df, away_df], ignore_index=True)
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def analyze_team_fits():
    print("--- 🔬 ANALYZING TEAM REGRESSION TYPES ---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    df = get_data()
    if df.empty:
        print("No data found.")
        return

    teams = sorted(df['Team'].unique())
    print(f"   Testing models for {len(teams)} teams...")
    
    results = []
    
    for team in teams:
        team_df = df[df.Team == team]
        X = team_df[['Q3_Lead']].values
        y = team_df['Final_Result'].values
        
        # Skip teams with too little data to be significant
        if len(team_df) < 10:
            continue
            
        team_result = {'Team': team, 'Games': len(team_df)}
        
        # 5-Fold Cross Validation (or less if not enough data)
        splits = min(5, len(team_df) // 2)
        if splits < 2: splits = 2
        cv = KFold(n_splits=splits, shuffle=True, random_state=42)
        
        best_mse = float('inf')
        best_degree = 1
        
        # Test Degrees 1, 2, 3
        for d in [1, 2, 3]:
            model = make_pipeline(PolynomialFeatures(d), LinearRegression())
            # Scoring is neg_mean_squared_error, so we negate it back to positive MSE
            scores = cross_val_score(model, X, y, scoring='neg_mean_squared_error', cv=cv)
            mse = -scores.mean()
            
            team_result[f'MSE_D{d}'] = mse
            
            # We look for the lowest error
            if mse < best_mse:
                best_mse = mse
                best_degree = d
        
        team_result['Best_Degree'] = best_degree
        results.append(team_result)
        
        # Progress dot
        print(".", end="", flush=True)
    
    print("\n")
    results_df = pd.DataFrame(results)
    
    # Save Results
    csv_path = os.path.join(OUTPUT_DIR, "team_best_fits.csv")
    results_df.to_csv(csv_path, index=False)
    
    # --- REPORTING ---
    print(f"✅ Analysis Complete. Saved to: {csv_path}")
    
    # Count distribution
    dist = results_df['Best_Degree'].value_counts().sort_index()
    print("\n📊 TEAM DISTRIBUTION BY COMPLEXITY:")
    print(dist)
    
    print("\n📝 TEAMS WITH NON-LINEAR (COMPLEX) BEHAVIOR (Degrees 2 & 3):")
    complex_teams = results_df[results_df['Best_Degree'] > 1]['Team'].tolist()
    print(", ".join(complex_teams))

    # --- VISUALIZATION ---
    plt.figure(figsize=(10, 6))
    sns.countplot(x='Best_Degree', data=results_df, palette='viridis')
    plt.title("Which Regression Fits Each Team Best?")
    plt.xlabel("Polynomial Degree (1=Linear, 2=Quadratic, 3=Cubic)")
    plt.ylabel("Number of Teams")
    
    plot_path = os.path.join(OUTPUT_DIR, "team_fits_distribution.png")
    plt.savefig(plot_path)
    print(f"📈 Chart saved to: {plot_path}")

if __name__ == "__main__":
    analyze_team_fits()