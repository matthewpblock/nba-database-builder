import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import math

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
TARGET_SEASON_ID = '22024'

def get_closing_data():
    engine = create_engine(DB_URL)
    print(f"   Fetching granular data for Season {TARGET_SEASON_ID}...")
    
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
        
        # Fill Forward & Dropna
        raw_df[cols] = raw_df.groupby('game_id')[cols].ffill()
        raw_df = raw_df.dropna()

        # Margins (Home)
        raw_df['Q3_Margin'] = raw_df['q3_h'] - raw_df['q3_a']
        raw_df['Final_Margin'] = raw_df['f_h'] - raw_df['f_a']
        
        # Stack Data
        home_df = raw_df[['home_team', 'Q3_Margin', 'Final_Margin']].copy()
        home_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        away_df = raw_df[['away_team', 'Q3_Margin', 'Final_Margin']].copy()
        away_df['Q3_Lead'] = away_df['Q3_Margin'] * -1
        away_df['Final_Result'] = away_df['Final_Margin'] * -1
        
        # --- FIX: Select 'away_team' (the column that actually exists) ---
        away_df = away_df[['away_team', 'Q3_Lead', 'Final_Result']]
        # -----------------------------------------------------------------
        
        away_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        return pd.concat([home_df, away_df], ignore_index=True)
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def analyze_situational():
    print(f"--- 🧩 SITUATIONAL CLOSING ANALYSIS ---")
    
    df = get_closing_data()
    if df.empty:
        print("No data found.")
        return

    # 1. CREATE LEAGUE BASELINE (Polynomial Degree 3)
    X = df['Q3_Lead'].values
    y = df['Final_Result'].values
    
    poly_coeffs = np.polyfit(X, y, 3)
    league_curve = np.poly1d(poly_coeffs)
    
    # Calculate Residuals
    df['Expected_Result'] = league_curve(df['Q3_Lead'])
    df['Performance_Vs_Avg'] = df['Final_Result'] - df['Expected_Result']

    # 2. DEFINE SITUATIONS (BINS)
    bins = [-100, -15, -6, 6, 15, 100]
    labels = ['Big Deficit\n(<-15)', 'Moderate Deficit\n(-15 to -6)', 'Close Game\n(-6 to +6)', 'Moderate Lead\n(+6 to +15)', 'Big Lead\n(>+15)']
    
    df['Situation'] = pd.cut(df['Q3_Lead'], bins=bins, labels=labels)

    # --- PLOT 0: LEAGUE-WIDE OVERVIEW ---
    plt.figure(figsize=(12, 8))
    sns.regplot(
        data=df, 
        x='Q3_Lead', 
        y='Final_Result', 
        order=3,  # Polynomial degree 3
        ci=None,  # Don't show confidence interval
        line_kws={'color': 'red', 'label': 'League Trend (Poly D3)', 'linewidth': 2.5},
        scatter_kws={'alpha': 0.1, 'color': 'gray'}
    )
    plt.plot([-40, 40], [-40, 40], color='blue', linestyle='--', alpha=0.5, label='Lead Maintained (y=x)')
    plt.axhline(0, color='k', linewidth=0.5, linestyle='-')
    plt.axvline(0, color='k', linewidth=0.5, linestyle='-')
    plt.title("League-Wide Closing Ability: Q3 Lead vs. Final Margin (2024-25)")
    plt.xlabel("Lead/Deficit at End of Q3")
    plt.ylabel("Final Margin")
    plt.grid(True, alpha=0.2)
    plt.legend()
    plt.xlim(-30, 30)
    plt.ylim(-30, 30)
    plt.savefig("reports/closing_league_overview_2024_25.png")
    print("✅ League overview chart saved to: reports/closing_league_overview_2024_25.png")

    # 3. GENERATE HEATMAP DATA
    heatmap_data = df.groupby(['Team', 'Situation'], observed=False)['Performance_Vs_Avg'].mean().unstack()
    heatmap_data = heatmap_data.sort_values('Close Game\n(-6 to +6)', ascending=False)

    # --- PLOT 1: HEATMAP ---
    plt.figure(figsize=(12, 10))
    sns.heatmap(heatmap_data, annot=True, fmt=".1f", cmap="RdYlGn", center=0, linewidths=.5)
    plt.title("Team Closing 'Personality': Points Gained/Lost vs. League Average")
    plt.xlabel("Situation at End of Q3")
    plt.ylabel("Team")
    plt.tight_layout()
    plt.savefig("reports/closing_heatmap_2024_25.png")
    print("✅ Heatmap saved to: reports/closing_heatmap_2024_25.png")

    # --- PLOT 2: SHAPE GRID ---
    teams = sorted(df['Team'].unique())
    n_teams = len(teams)
    cols = 5
    rows = math.ceil(n_teams / cols)
    
    fig, axes = plt.subplots(rows, cols, figsize=(20, 4 * rows))
    axes = axes.flatten()
    
    x_range = np.linspace(-25, 25, 100)
    y_league = league_curve(x_range)

    for i, team in enumerate(teams):
        ax = axes[i]
        team_data = df[df.Team == team]
        
        # League Baseline (Black)
        ax.plot(x_range, y_league, color='black', alpha=0.3, linestyle='--', linewidth=1.5, label='League Avg')
        
        # Team Curve (Blue)
        if len(team_data) > 4:
            try:
                team_coeffs = np.polyfit(team_data['Q3_Lead'], team_data['Final_Result'], 3)
                team_curve = np.poly1d(team_coeffs)
                ax.plot(x_range, team_curve(x_range), color='blue', linewidth=2, label=team)
            except:
                pass
        
        # Scatter Dots
        colors = ['green' if x > 0 else 'red' for x in team_data['Final_Result']]
        ax.scatter(team_data['Q3_Lead'], team_data['Final_Result'], c=colors, alpha=0.6, s=20)
        
        ax.set_title(f"{team}", fontweight='bold')
        ax.axhline(0, color='gray', linewidth=0.5)
        ax.axvline(0, color='gray', linewidth=0.5)
        ax.set_xlim(-25, 25)
        ax.set_ylim(-25, 25)

    for j in range(i + 1, len(axes)):
        axes[j].axis('off')

    plt.suptitle("Team Closing Shapes vs. League Baseline (2024-25)", fontsize=20, y=1.01)
    plt.tight_layout()
    plt.savefig("reports/closing_shapes_2024_25.png")
    print("✅ Shape Grid saved to: reports/closing_shapes_2024_25.png")

if __name__ == "__main__":
    analyze_situational()