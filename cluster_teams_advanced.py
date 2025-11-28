import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import os

# CONFIG
DB_URL = 'sqlite:///nba_analysis.db'
TARGET_SEASON_IDS = "('22024', '42024', '52024')"
NUM_CLUSTERS = 6 
OUTPUT_DIR = 'reports'
FITS_FILE = os.path.join(OUTPUT_DIR, 'team_best_fits.csv')

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
        
        # Stack Data
        home_df = raw_df[['home_team', 'Q3_Margin', 'Final_Margin']].copy()
        home_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        away_df = raw_df[['away_team', 'Q3_Margin', 'Final_Margin']].copy()
        away_df['Q3_Lead'] = away_df['Q3_Margin'] * -1
        away_df['Final_Result'] = away_df['Final_Margin'] * -1
        
        # Select correct column before renaming
        away_df = away_df[['away_team', 'Q3_Lead', 'Final_Result']]
        away_df.columns = ['Team', 'Q3_Lead', 'Final_Result']
        
        return pd.concat([home_df, away_df], ignore_index=True)
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def run_clustering():
    print(f"--- 🕵️ ADVANCED CLUSTERING (Performance + Complexity) ---")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    df = get_closing_data()
    if df.empty:
        print("No data found.")
        return

    # 1. LOAD COMPLEXITY DNA
    if not os.path.exists(FITS_FILE):
        print(f"❌ Error: Could not find '{FITS_FILE}'. Run 'analyze_team_fits.py' first.")
        return
    
    fits_df = pd.read_csv(FITS_FILE)[['Team', 'Best_Degree']]
    print(f"   Loaded regression profiles for {len(fits_df)} teams.")

    # 2. CALCULATE BASELINE (Linear League Avg)
    X = df['Q3_Lead'].values
    y = df['Final_Result'].values
    poly_coeffs = np.polyfit(X, y, 1) 
    league_line = np.poly1d(poly_coeffs)
    
    df['Performance'] = df['Final_Result'] - league_line(df['Q3_Lead'])

    # 3. FEATURE ENGINEERING
    bins = [-100, -15, -6, 6, 15, 100]
    labels = ['Big Deficit', 'Mod Deficit', 'Close Game', 'Mod Lead', 'Big Lead']
    df['Situation'] = pd.cut(df['Q3_Lead'], bins=bins, labels=labels)

    # Pivot: Situational Performance
    team_features = df.groupby(['Team', 'Situation'], observed=False)['Performance'].mean().unstack()
    team_features = team_features.fillna(0)
    
    # --- FIX: FLATTEN COLUMNS FOR MERGE ---
    # Convert Categorical columns to simple Strings
    team_features.columns = team_features.columns.astype(str)
    # Move 'Team' from Index to Column
    team_features = team_features.reset_index()
    # --------------------------------------

    # 4. MERGE FEATURES
    final_features = pd.merge(team_features, fits_df, on='Team')
    final_features = final_features.set_index('Team')
    
    # 5. SCALE & CLUSTER
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(final_features)

    kmeans = KMeans(n_clusters=NUM_CLUSTERS, random_state=42)
    clusters = kmeans.fit_predict(features_scaled)
    final_features['Cluster_ID'] = clusters

    # 6. VISUALIZE PROFILES
    cluster_profiles = pd.DataFrame(
        scaler.inverse_transform(kmeans.cluster_centers_), 
        columns=final_features.columns[:-1]
    )
    cluster_profiles.index.name = 'Cluster'

    plt.figure(figsize=(12, 7))
    sns.heatmap(cluster_profiles, annot=True, fmt=".1f", cmap="RdYlGn", center=0)
    plt.title(f"The {NUM_CLUSTERS} Archetypes (Including Curve Complexity)")
    plt.ylabel("Cluster ID")
    plt.xlabel("Feature Score (Avg)")
    
    outfile = os.path.join(OUTPUT_DIR, "cluster_advanced.png")
    plt.savefig(outfile)
    print(f"\n✅ Advanced Heatmap saved to: {outfile}")

    # 7. REPORT
    print("\n--- CLUSTER ARCHETYPES ---")
    for i in range(NUM_CLUSTERS):
        group = final_features[final_features['Cluster_ID'] == i]
        members = group.index.tolist()
        
        avg_degree = group['Best_Degree'].mean()
        complexity_label = "Linear" if avg_degree < 1.5 else "Complex"
        
        print(f"\n📁 Cluster {i}: The {complexity_label} Group ({len(members)} Teams)")
        print(f"   Teams: {', '.join(sorted(members))}")
        
        prof = cluster_profiles.iloc[i].drop('Best_Degree')
        best_sit = prof.idxmax()
        worst_sit = prof.idxmin()
        print(f"   📝 DNA: Strongest at '{best_sit}', Weakest at '{worst_sit}'")
        print(f"   📈 Avg Complexity Degree: {avg_degree:.1f}")

if __name__ == "__main__":
    run_clustering()