import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
TARGET_SEASON_ID = '22024'
NUM_CLUSTERS = 5  # How many "Archetypes" do you want to find?

def get_closing_data():
    # Reuse the robust data fetcher from the previous script
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
        
        # Explicit Select before Rename
        away_df = away_df[['away_team', 'Q3_Lead', 'Final_Result']]
        away_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        return pd.concat([home_df, away_df], ignore_index=True)
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def run_clustering():
    print(f"--- 🕵️ TEAM CLUSTERING ANALYSIS ---")
    
    # 1. GET DATA & CALCULATE METRICS
    df = get_closing_data()
    if df.empty:
        print("No data found. Wait for ingestion to finish!")
        return

    # Polynomial Baseline
    X = df['Q3_Lead'].values
    y = df['Final_Result'].values
    poly_coeffs = np.polyfit(X, y, 3)
    league_curve = np.poly1d(poly_coeffs)
    
    df['Performance_Vs_Avg'] = df['Final_Result'] - league_curve(df['Q3_Lead'])

    # 2. FEATURE ENGINEERING (Pivot Table)
    # We define the 5 specific situations
    bins = [-100, -15, -6, 6, 15, 100]
    labels = ['Big Deficit', 'Mod Deficit', 'Close Game', 'Mod Lead', 'Big Lead']
    df['Situation'] = pd.cut(df['Q3_Lead'], bins=bins, labels=labels)

    # Create a matrix: Row=Team, Col=Situation, Value=Avg Performance
    team_features = df.groupby(['Team', 'Situation'], observed=False)['Performance_Vs_Avg'].mean().unstack()
    
    # Fill NaNs with 0 (if a team hasn't faced a situation, assume they are "Average")
    team_features = team_features.fillna(0)
    
    # Normalize (Scaling is important for K-Means)
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(team_features)

    # 3. K-MEANS CLUSTERING
    kmeans = KMeans(n_clusters=NUM_CLUSTERS, random_state=42)
    clusters = kmeans.fit_predict(features_scaled)
    
    team_features['Cluster_ID'] = clusters

    # 4. VISUALIZE PROFILES (The "DNA" of each cluster)
    # We calculate the average stats for each cluster center to see what makes them unique
    cluster_profiles = pd.DataFrame(
        scaler.inverse_transform(kmeans.cluster_centers_), 
        columns=team_features.columns[:-1]
    )
    cluster_profiles.index.name = 'Cluster'

    plt.figure(figsize=(10, 6))
    sns.heatmap(cluster_profiles, annot=True, fmt=".1f", cmap="RdYlGn", center=0)
    plt.title(f"The {NUM_CLUSTERS} Archetypes of NBA Closing Teams")
    plt.ylabel("Cluster ID")
    plt.xlabel("Performance vs. Expectation (Points)")
    plt.savefig("cluster_archetypes.png")
    print("\n✅ Cluster Heatmap saved to: cluster_archetypes.png")

    # 5. PRINT RESULTS
    print("\n--- CLUSTER MEMBERSHIP ---")
    for i in range(NUM_CLUSTERS):
        members = team_features[team_features['Cluster_ID'] == i].index.tolist()
        print(f"\n📁 Cluster {i}:")
        print(f"   {', '.join(members)}")
        
        # Simple text description based on the profile
        profile = cluster_profiles.iloc[i]
        best_sit = profile.idxmax()
        worst_sit = profile.idxmin()
        print(f"   📝 Profile: Best at '{best_sit}', Worst at '{worst_sit}'")

if __name__ == "__main__":
    run_clustering()