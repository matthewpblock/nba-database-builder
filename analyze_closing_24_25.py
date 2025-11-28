import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
TARGET_SEASON_ID = '22024' # 2 = Reg Season, 2024 = Start Year

def get_closing_data():
    engine = create_engine(DB_URL)
    print(f"   Fetching closing data for Season {TARGET_SEASON_ID}...")
    
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
        if raw_df.empty:
            return pd.DataFrame()

        # Clean & Convert
        cols = ['q3_h', 'q3_a', 'f_h', 'f_a']
        for c in cols:
            raw_df[c] = pd.to_numeric(raw_df[c], errors='coerce')
        
        # Fill Forward logic for missing scores (End of Period rows)
        raw_df[cols] = raw_df.groupby('game_id')[cols].ffill()
        
        # Snapshot extraction
        raw_df = raw_df.dropna()

        # Calculate Margins (Home Perspective)
        raw_df['Q3_Margin'] = raw_df['q3_h'] - raw_df['q3_a']
        raw_df['Final_Margin'] = raw_df['f_h'] - raw_df['f_a']
        
        # Stack Data (Home & Away)
        home_df = raw_df[['home_team', 'Q3_Margin', 'Final_Margin']].copy()
        home_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        # Away Rows (Calculations)
        away_df = raw_df[['away_team', 'Q3_Margin', 'Final_Margin']].copy()
        away_df['Q3_Lead'] = away_df['Q3_Margin'] * -1
        away_df['Final_Result'] = away_df['Final_Margin'] * -1
        
        # --- THE FIX: Select the 3 columns BEFORE renaming ---
        away_df = away_df[['away_team', 'Q3_Lead', 'Final_Result']]
        # -----------------------------------------------------
        
        away_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        return pd.concat([home_df, away_df], ignore_index=True)
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def analyze_closing():
    print(f"--- 🏁 CLOSING ABILITY ANALYSIS (2024-25) ---")
    
    df = get_closing_data()
    if df.empty:
        print("No data found for 2024-25. (Did you run 'ingest_season.py' with TARGET_SEASON='2024-25'?)")
        return

    print(f"   Analyzing {len(df)} team performances...")

    # League Baseline Model
    X = df[['Q3_Lead']].values
    y = df['Final_Result'].values
    league_model = LinearRegression()
    league_model.fit(X, y)
    
    df['Expected_Result'] = league_model.predict(X)
    df['Points_Gained_In_4th'] = df['Final_Result'] - df['Expected_Result']
    
    # Rankings
    rankings = df.groupby('Team')['Points_Gained_In_4th'].mean().sort_values(ascending=False)
    
    print("\n🏆 BEST CLOSERS (Avg Points Gained vs Expectation):")
    print(rankings.head(5).to_string(float_format="+.2f"))
    print("\n❄️ WORST CLOSERS (Avg Points Lost vs Expectation):")
    print(rankings.tail(5).to_string(float_format="+.2f"))

    # Plot
    plt.figure(figsize=(12, 8))
    sns.regplot(x='Q3_Lead', y='Final_Result', data=df, scatter=False, color='black', 
                line_kws={'linestyle':'--', 'alpha':0.5, 'label':'League Avg'})
    
    # Highlight specific interesting teams (Top/Bottom)
    for team, color in [(rankings.index[0], 'green'), (rankings.index[-1], 'red')]:
        sns.regplot(x='Q3_Lead', y='Final_Result', data=df[df.Team == team], 
                    scatter=False, ci=None, color=color, label=team)

    plt.scatter(df['Q3_Lead'], df['Final_Result'], alpha=0.15, color='gray')
    plt.axhline(0, color='k', linewidth=0.5); plt.axvline(0, color='k', linewidth=0.5)
    plt.plot([-30, 30], [-30, 30], color='blue', alpha=0.1, linestyle=':', label='Held Lead')
    
    plt.title("2024-25 Closing Ability: Q3 Lead vs Final Margin")
    plt.xlabel("Lead/Deficit after Q3"); plt.ylabel("Final Margin")
    plt.legend(); plt.grid(True, alpha=0.2)
    plt.savefig("reports/closing_analysis_2024_25.png")
    print(f"\n📈 Chart saved to: reports/closing_analysis_2024_25.png")

if __name__ == "__main__":
    analyze_closing()