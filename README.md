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