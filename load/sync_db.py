# load/database_loader.py
import sqlite3
import pandas as pd
import os
from typing import Dict

class DatabaseLoader:
    def __init__(self, db_path: str = "../database/spotify_data.db"):
        self.db_path = db_path
        self._ensure_database_exists()
    
    def _ensure_database_exists(self):
        """Create database and tables if they don't exist"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        
        # Create tables
        conn.execute('''
            CREATE TABLE IF NOT EXISTS user_profile (
                user_id TEXT PRIMARY KEY,
                display_name TEXT,
                email TEXT,
                country TEXT,
                product TEXT,
                followers INTEGER,
                extracted_at DATETIME
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS listening_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                track_id TEXT,
                track_name TEXT,
                artist_id TEXT,
                artist_name TEXT,
                album_id TEXT,
                album_name TEXT,
                popularity INTEGER,
                duration_ms INTEGER,
                explicit BOOLEAN,
                source TEXT,
                played_at DATETIME,
                extracted_at DATETIME
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def load_spotify_data(self, transformed_data: Dict):
        """Load transformed data into database"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Load profile data (upsert)
            profile_df = pd.DataFrame(transformed_data['profile'])
            profile_df.to_sql('user_profile', conn, if_exists='replace', index=False)
            
            # Load listening history (append)
            listening_data = transformed_data['top_tracks'] + transformed_data['recently_played']
            listening_df = pd.DataFrame(listening_data)
            
            if not listening_df.empty:
                listening_df.to_sql('listening_history', conn, if_exists='append', index=False)
            
            conn.commit()
            print(f"Loaded {len(profile_df)} profiles and {len(listening_df)} listening records")
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()