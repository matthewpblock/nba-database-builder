import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from scipy.optimize import curve_fit

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
TARGET_SEASON_ID = '22024'

def get_closing_data():
    engine = create_engine(DB_URL)
    print(f"   Fetching data for Season {TARGET_SEASON_ID}...")
    
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
    WHERE g.season_id = '{TARGET_SEASON_ID}'
    """
    
    try:
        raw_df = pd.read_sql(query, engine)
        if raw_df.empty: return pd.DataFrame()

        cols = ['q3_h', 'q3_a', 'f_h', 'f_a']
        for c in cols: raw_df[c] = pd.to_numeric(raw_df[c], errors='coerce')
        
        # Fill Forward
        raw_df[cols] = raw_df.groupby('game_id')[cols].ffill()
        raw_df = raw_df.dropna()

        # Calculate Margins
        raw_df['Q3_Margin'] = raw_df['q3_h'] - raw_df['q3_a']
        raw_df['Final_Margin'] = raw_df['f_h'] - raw_df['f_a']
        
        # Stack
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

# --- MODELS ---

def sigmoid_func(x, L, x0, k, b):
    # Generalized Logistic Function
    return L / (1 + np.exp(-k*(x-x0))) + b

def compare_models():
    print("--- 📊 REGRESSION MODEL SHOWDOWN ---")
    df = get_closing_data()
    if df.empty:
        print("No data found.")
        return

    X = df['Q3_Lead'].values
    y = df['Final_Result'].values
    
    # 1. Linear Regression
    lin_model = LinearRegression()
    lin_model.fit(X.reshape(-1, 1), y)
    y_pred_lin = lin_model.predict(X.reshape(-1, 1))
    r2_lin = r2_score(y, y_pred_lin)
    
    # 2. Polynomial Regression (Degree 3 - S-shapedish)
    poly_coeffs = np.polyfit(X, y, 3)
    poly_func = np.poly1d(poly_coeffs)
    y_pred_poly = poly_func(X)
    r2_poly = r2_score(y, y_pred_poly)
    
    # 3. Sigmoid Regression
    # Good initial guesses are critical for curve_fit
    p0 = [max(y)-min(y), np.median(X), 0.1, min(y)]
    
    sigmoid_success = False
    try:
        popt, _ = curve_fit(sigmoid_func, X, y, p0=p0, maxfev=10000)
        y_pred_sig = sigmoid_func(X, *popt)
        r2_sig = r2_score(y, y_pred_sig)
        sigmoid_success = True
    except:
        r2_sig = -1.0
        print("⚠️ Sigmoid fit failed to converge.")

    # --- REPORT CARD ---
    print(f"\nModel Performance (R² Score):")
    print(f"1. Linear:     {r2_lin:.4f}")
    print(f"2. Poly (D3):  {r2_poly:.4f}")
    if sigmoid_success:
        print(f"3. Sigmoid:    {r2_sig:.4f}")
    
    # Determine Winner
    scores = {'Linear': r2_lin, 'Polynomial': r2_poly}
    if sigmoid_success: scores['Sigmoid'] = r2_sig
    
    winner = max(scores, key=scores.get)
    print(f"\n🏆 WINNER: {winner} is the best fit for this data.")

    # --- VISUALIZATION ---
    plt.figure(figsize=(10, 6))
    plt.scatter(X, y, alpha=0.1, color='gray', label='Actual Data')
    
    # Generate smooth lines for plotting
    x_range = np.linspace(min(X), max(X), 200)
    
    # Plot Linear
    plt.plot(x_range, lin_model.predict(x_range.reshape(-1, 1)), 
             color='blue', linestyle='--', label=f'Linear (R²={r2_lin:.2f})')
             
    # Plot Poly
    plt.plot(x_range, poly_func(x_range), 
             color='orange', linewidth=2, label=f'Poly D3 (R²={r2_poly:.2f})')
             
    # Plot Sigmoid
    if sigmoid_success:
        plt.plot(x_range, sigmoid_func(x_range, *popt), 
                 color='green', linewidth=2, label=f'Sigmoid (R²={r2_sig:.2f})')

    plt.title(f"Model Comparison: Q3 Lead vs Final Margin (Winner: {winner})")
    plt.xlabel("Q3 Lead")
    plt.ylabel("Final Margin")
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    filename = "model_comparison.png"
    plt.savefig(filename)
    print(f"📈 Chart saved to: {filename}")

if __name__ == "__main__":
    compare_models()