import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'

def plot_closing_scatter():
    engine = create_engine(DB_URL)
    print("--- 🔍 VISUAL DATA INSPECTION ---")
    
    # 1. FETCH SCORES (Not Margin)
    # We grab the raw score strings ("105") and cast them to integers later
    print("   Fetching raw scores from PlayByPlay...")
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
        q3.score_home as q3_home,
        q3.score_away as q3_away,
        final.score_home as f_home,
        final.score_away as f_away
    FROM games g
    JOIN q3_scores q3 ON g.game_id = q3.game_id
    JOIN final_scores final ON g.game_id = final.game_id
    JOIN teams t_home ON g.home_team_id = t_home.team_id
    JOIN teams t_away ON g.away_team_id = t_away.team_id
    """
    
    try:
        raw_df = pd.read_sql(query, engine)
        
        # 2. CALCULATE TRUE MARGINS
        # Convert strings to integers and calculate: (Home - Away)
        cols = ['q3_home', 'q3_away', 'f_home', 'f_away']
        for c in cols:
            raw_df[c] = pd.to_numeric(raw_df[c])
            
        raw_df['Q3_Margin_Home'] = raw_df['q3_home'] - raw_df['q3_away']
        raw_df['Final_Margin_Home'] = raw_df['f_home'] - raw_df['f_away']
        
        # 3. STACK DATA (Home & Away Perspectives)
        # Home Perspective
        home_df = raw_df[['home_team', 'Q3_Margin_Home', 'Final_Margin_Home']].copy()
        home_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        # Away Perspective (Flip the signs!)
        away_df = raw_df[['away_team', 'Q3_Margin_Home', 'Final_Margin_Home']].copy()
        away_df['Q3_Lead'] = away_df['Q3_Margin_Home'] * -1
        away_df['Final_Result'] = away_df['Final_Margin_Home'] * -1
        away_df = away_df[['away_team', 'Q3_Lead', 'Final_Result']]
        away_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        full_df = pd.concat([home_df, away_df], ignore_index=True)
        
        print(f"   Plotting {len(full_df)} data points...")
        
        # 4. PLOT SCATTER
        plt.figure(figsize=(10, 8))
        
        # Reference Line (y=x)
        # If a point is ON this line, they held the lead perfectly.
        # Above line = Extended lead. Below line = Lost lead.
        plt.plot([-30, 30], [-30, 30], color='black', linestyle='--', alpha=0.5, label='Lead Maintained')
        
        # The Scatter
        sns.scatterplot(
            data=full_df, 
            x='Q3_Lead', 
            y='Final_Result', 
            alpha=0.6, 
            hue='Team', 
            legend=False # Too many teams for a legend right now
        )
        
        plt.title("Closing Ability: Q3 Lead vs. Final Result")
        plt.xlabel("Lead after Q3 (Points)")
        plt.ylabel("Final Margin (Points)")
        plt.axhline(0, color='gray', alpha=0.3)
        plt.axvline(0, color='gray', alpha=0.3)
        plt.grid(True, alpha=0.2)
        
        # Limit axes to realistic basketball scores
        plt.xlim(-40, 40)
        plt.ylim(-40, 40)
        
        filename = "reports/closing_scatter_check_2024_25.png"
        plt.savefig(filename)
        print(f"   ✅ Scatterplot saved to: {filename}")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    plot_closing_scatter()