import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'

def get_closing_data():
    engine = create_engine(DB_URL)
    print("   Fetching Q3 and Final scores from PlayByPlay...")
    
    query = """
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
    """
    
    try:
        raw_df = pd.read_sql(query, engine)
        
        # 1. CLEAN DATA
        cols = ['q3_h', 'q3_a', 'f_h', 'f_a']
        for c in cols:
            raw_df[c] = pd.to_numeric(raw_df[c], errors='coerce')
        
        raw_df = raw_df.dropna()

        # 2. CALCULATE TRUE MARGINS (Home Perspective)
        raw_df['Q3_Margin'] = raw_df['q3_h'] - raw_df['q3_a']
        raw_df['Final_Margin'] = raw_df['f_h'] - raw_df['f_a']
        
        # 3. STACK DATA
        
        # Home Rows
        home_df = raw_df[['home_team', 'Q3_Margin', 'Final_Margin']].copy()
        home_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        # Away Rows (Calculations)
        away_df = raw_df[['away_team', 'Q3_Margin', 'Final_Margin']].copy()
        away_df['Q3_Lead'] = away_df['Q3_Margin'] * -1
        away_df['Final_Result'] = away_df['Final_Margin'] * -1
        
        # --- THE FIX IS HERE ---
        # We explicitly select ONLY the 3 columns we want to keep
        away_df = away_df[['away_team', 'Q3_Lead', 'Final_Result']]
        # -----------------------
        
        away_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        # Combine
        full_df = pd.concat([home_df, away_df], ignore_index=True)
        return full_df
        
    except Exception as e:
        print(f"Error getting data: {e}")
        return pd.DataFrame()

def analyze_closing():
    print("--- 🏁 CLOSING ABILITY ANALYSIS ---")
    
    df = get_closing_data()
    if df.empty:
        print("No data found.")
        return

    print(f"   Analyzing {len(df)} team performances...")

    # 2. LEAGUE BASELINE MODEL
    X = df[['Q3_Lead']].values
    y = df['Final_Result'].values
    
    league_model = LinearRegression()
    league_model.fit(X, y)
    
    df['Expected_Result'] = league_model.predict(X)
    df['Points_Gained_In_4th'] = df['Final_Result'] - df['Expected_Result']
    
    # 3. RANK TEAMS
    rankings = df.groupby('Team')['Points_Gained_In_4th'].mean().sort_values(ascending=False)
    
    print("\n🏆 BEST CLOSING TEAMS (Avg Points Gained vs League Exp):")
    print(rankings.head(5).to_string(float_format="+.2f"))
    
    print("\n❄️ WORST CLOSING TEAMS (Avg Points Lost vs League Exp):")
    print(rankings.tail(5).to_string(float_format="+.2f"))

    # 4. VISUALIZATION
    plt.figure(figsize=(12, 8))
    
    # League Baseline
    sns.regplot(x='Q3_Lead', y='Final_Result', data=df, 
                scatter=False, color='black', line_kws={'linestyle':'--', 'alpha': 0.7, 'label':'League Baseline'})
    
    # Top Team
    top_team = rankings.index[0]
    sns.regplot(x='Q3_Lead', y='Final_Result', data=df[df.Team == top_team], 
                scatter=False, ci=None, color='green', label=f"{top_team} (Best)")
    
    # Bottom Team
    bot_team = rankings.index[-1]
    sns.regplot(x='Q3_Lead', y='Final_Result', data=df[df.Team == bot_team], 
                scatter=False, ci=None, color='red', label=f"{bot_team} (Worst)")

    # Scatter Background
    plt.scatter(df['Q3_Lead'], df['Final_Result'], alpha=0.1, color='gray', s=15)

    plt.axhline(0, color='k', linewidth=0.5)
    plt.axvline(0, color='k', linewidth=0.5)
    plt.plot([-30, 30], [-30, 30], color='blue', alpha=0.2, linestyle=':', label='Maintained Lead')

    plt.title("Closing Ability: Q3 Lead vs Final Margin (2024-25)")
    plt.xlabel("Lead/Deficit after 3 Quarters")
    plt.ylabel("Final Margin")
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    filename = "closing_analysis_final.png"
    plt.savefig(filename)
    print(f"\n📈 Chart saved to: {filename}")

if __name__ == "__main__":
    analyze_closing()