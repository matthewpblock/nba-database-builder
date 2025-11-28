# nba-database-builder
Pulls NBA data and stores in a database for advanced analysis

## How the Migration Will Look (Step-by-Step)
When you decide to move to Google Cloud in a few months, here is exactly what you will do:

Create the Database: Go to Google Cloud Console and create a "Cloud SQL for PostgreSQL" instance.

Update Config: Open your .env file and paste the connection string Google gives you (formatted like the example above).

Run the Script: Run python get_nba_data.py again.

Because your data comes from an API, you don't even need to migrate the data manually. You just point the script at the new database, run it, and it will re-download and populate your fresh Cloud database from scratch.

Why this is better
Security: You never accidentally commit passwords to GitHub because they stay in .env (which you should add to your .gitignore).

Flexibility: You can switch back and forth between "Development" (SQLite) and "Production" (Postgres) instantly.

## Key Features
*   **Comprehensive Data Ingestion**: Pulls multiple data types for each game to create a rich, relational dataset.
*   **Robust & Resilient**: Automatically retries on API timeouts with an incremental backoff strategy to handle rate limiting.
*   **Data Integrity**: Intelligently checks for already-ingested games to prevent duplication and only downloads what's missing.
*   **Detailed Logging**: All operations, warnings, and errors are logged to both the console and a persistent `nba_database_builder.log` file for easy monitoring and debugging.
*   **Modular Schema**: Uses SQLAlchemy to define the database structure, making it easy to understand, modify, and extend.
*   **Maintenance Utilities**: Includes helper scripts to reset individual data tables without affecting the rest of the database.

## Database Schema
This project creates a relational database with the following primary tables, defined in `models.py`:  
*   `player_game_stats`: Traditional and advanced box score statistics for each player in a game.
*   `play_by_play`: A detailed log of every event that occurs during a game.
*   `hustle_stats`: "Dirty work" stats like screen assists, deflections, and charges drawn.
*   `player_matchups`: Data on which player guarded whom and for how long.
*   `game_rotations`: Substitution patterns, showing when players entered and exited the game.
*   `games`, `players`, `teams`: Core dimension tables (currently placeholders, to be populated by other scripts).


## Getting Started

### 1. Installation

Clone the repository and install the required packages.

```bash
git clone <your-repo-url>
cd nba-database-builder
pip install -r requirements.txt
```

### 2. Initial Database Setup

Before you can ingest data, you need to create the database file and its table structure. The `models.py` script can do this for you.

```bash
python models.py
```

This will create an `nba_analysis.db` file in your project directory with all the necessary tables.

### 3. Running the Ingestion

The main script for fetching data is `ingest_season.py`.

1.  **Configure the Season**: Open `ingest_season.py` and set the `TARGET_SEASON` variable (e.g., `'2023-24'`).
2.  **Run the script**:
    ```bash
    python ingest_season.py
    ```

The script will:
1.  Fetch the schedule for the target season.
2.  Check your database for games that have already been ingested.
3.  Loop through all missing games and download the full suite of data for each one.
4.  Log all progress to the console and to `nba_database_builder.log`.

## Maintenance

### Resetting a Table

If you need to re-fetch data for a specific table (e.g., after a schema change or to fix bad data), you can use the `fix_table.py` script.

1.  **Configure the Target**: Open `fix_table.py` and set the `TARGET_TABLE` variable to the name of the table you want to reset (e.g., `'player_matchups'`).
2.  **Run the script**:
    ```bash
    python fix_table.py
    ```

This will safely drop the specified table and immediately recreate it, ready for the main ingestion script to repopulate it.

## Deployment & Configuration

The current setup uses a local SQLite database for simplicity. However, it is designed for easy migration to a more robust production database like PostgreSQL.

### How to Migrate to a Cloud Database (e.g., Google Cloud SQL)

When you are ready to move to a production environment, the process is straightforward:

1.  **Create the Database**: In your cloud provider's console (like Google Cloud), create a new database instance (e.g., "Cloud SQL for PostgreSQL").
2.  **Update Config**: The scripts are designed to pull the database URL from environment variables. You would create a `.env` file and place the connection string provided by your cloud host inside it.
    ```
    # .env file
    DATABASE_URL="postgresql://user:password@host:port/database"
    ```
    *(Note: The scripts would need a minor modification to load `python-dotenv` and read this variable.)*
3.  **Run the Scripts**: Run `python models.py` to set up the schema in the new database, then run `python ingest_season.py` to populate it.

Because all data is sourced from the API, you don't need to perform a manual data migration. You simply point the scripts at the new database, and they will build it from scratch.

### Why this is a good approach

*   **Security**: You avoid committing credentials to version control. The `.env` file should be added to your `.gitignore`.
*   **Flexibility**: You can easily switch between a local "development" database (SQLite) and a remote "production" database (PostgreSQL) without changing the core code.
