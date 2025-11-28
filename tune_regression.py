import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sklearn.model_selection import cross_val_score, KFold
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
import os

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
# Target Regular Season (2), Playoffs (4), and Play-In (5)
TARGET_SEASON_IDS = "('22024', '42024', '52024')"
OUTPUT_DIR = 'reports'

def get_closing_data():
    engine = create_engine(DB_URL)
    print(f"   Fetching data for Season IDs {TARGET_SEASON_IDS}...")
    
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
        
        # --- Home Team Stack ---
        home_df = raw_df[['home_team', 'Q3_Margin', 'Final_Margin']].copy()
        home_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        # --- Away Team Stack ---
        away_df = raw_df[['away_team', 'Q3_Margin', 'Final_Margin']].copy()
        away_df['Q3_Lead'] = away_df['Q3_Margin'] * -1
        away_df['Final_Result'] = away_df['Final_Margin'] * -1
        
        # FIX: Select 'away_team' (which exists) BEFORE renaming to 'Team'
        away_df = away_df[['away_team', 'Q3_Lead', 'Final_Result']]
        away_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        return pd.concat([home_df, away_df], ignore_index=True)
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def tune_polynomial():
    print("--- 🎛️ TUNING REGRESSION MODEL (Including Postseason) ---")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    df = get_closing_data()
    if df.empty: 
        print("No data found.")
        return

    X = df[['Q3_Lead']].values
    y = df['Final_Result'].values

    degrees = [1, 2, 3, 4, 5]
    results = []

    print(f"\nPerforming 5-Fold Cross-Validation on {len(df)} games...")
    print(f"{'Degree':<10} | {'CV MSE (Lower is Better)':<25} | {'R2 Score':<10}")
    print("-" * 55)

    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    
    for d in degrees:
        model = make_pipeline(PolynomialFeatures(d), LinearRegression())
        
        neg_mse_scores = cross_val_score(model, X, y, scoring='neg_mean_squared_error', cv=cv)
        r2_scores = cross_val_score(model, X, y, scoring='r2', cv=cv)
        
        avg_mse = -neg_mse_scores.mean()
        avg_r2 = r2_scores.mean()
        
        results.append({'degree': d, 'mse': avg_mse, 'r2': avg_r2})
        print(f"{d:<10} | {avg_mse:<25.4f} | {avg_r2:.4f}")

    best_model = min(results, key=lambda x: x['mse'])
    print(f"\n🏆 BEST FIT: Polynomial Degree {best_model['degree']}")

    # --- VISUALIZATION ---
    plt.figure(figsize=(10, 6))
    plt.scatter(X, y, alpha=0.1, color='gray', label='Data')
    
    x_plot = np.linspace(-45, 45, 100).reshape(-1, 1)
    
    colors = ['red', 'orange', 'green', 'blue', 'purple']
    for i, d in enumerate(degrees):
        model = make_pipeline(PolynomialFeatures(d), LinearRegression())
        model.fit(X, y)
        y_plot = model.predict(x_plot)
        
        style = '--' if d != best_model['degree'] else '-'
        width = 1.5 if d != best_model['degree'] else 3
        label = f"Degree {d}"
        if d == best_model['degree']: label += " (Winner)"
        
        plt.plot(x_plot, y_plot, color=colors[i], linestyle=style, linewidth=width, label=label)

    plt.title("Model Tuning: Checking for Overfitting at Edges")
    plt.xlabel("Q3 Lead")
    plt.ylabel("Predicted Final Margin")
    plt.ylim(-50, 50)
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    outfile = os.path.join(OUTPUT_DIR, "tuning_results.png")
    plt.savefig(outfile)
    print(f"📈 Visualization saved to: {outfile}")

if __name__ == "__main__":
    tune_polynomial()