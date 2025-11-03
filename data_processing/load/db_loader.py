# load/database_loader_bulk.py
from sqlalchemy import insert, update
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert
import pandas as pd
import os
from typing import Dict, List
import logging
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

# Import your SQLAlchemy models
from database.db import Base, Artist, TopTrack, TopArtist, ListeningHistory

logger = logging.getLogger(__name__)

class DatabaseLoaderBulk:
    def __init__(self, db_url: str = None):
        if db_url is None:
            db_path = "../database/spotify_data.db"
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            db_url = f"sqlite:///{db_path}"
        
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(bind=self.engine)
    
    def load_spotify_data(self, transformed_data: Dict):
        """Load data using bulk operations"""
        session = self.Session()
        
        try:
            # Bulk upsert artists
            if transformed_data['artists']:
                self._bulk_upsert_artists(session, transformed_data['artists'])
            
            # Bulk insert top tracks (no conflicts expected due to historical nature)
            if transformed_data['top_tracks']:
                session.bulk_insert_mappings(TopTrack, transformed_data['top_tracks'])
            
            # Bulk insert top artists
            if transformed_data['top_artists']:
                session.bulk_insert_mappings(TopArtist, transformed_data['top_artists'])
            
            # Bulk insert listening history
            if transformed_data['listening_history']:
                session.bulk_insert_mappings(ListeningHistory, transformed_data['listening_history'])
            
            session.commit()
            logger.info("Successfully loaded all Spotify data using bulk operations")
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Bulk database loading error: {e}")
            raise
        finally:
            session.close()
    
    def _bulk_upsert_artists(self, session, artists: List[Dict]):
        """Bulk upsert artists using SQLAlchemy Core for better performance"""
        if not artists:
            return
        
        # For SQLite, we can use INSERT ... ON CONFLICT
        stmt = sqlite_upsert(Artist.__table__).values(artists)
        
        stmt = stmt.on_conflict_do_update(
            index_elements=['id'],
            set_={
                'name': stmt.excluded.name,
                'genre': stmt.excluded.genre,
                'popularity': stmt.excluded.popularity,
                'followers': stmt.excluded.followers,
                'spotify_url': stmt.excluded.spotify_url,
                'image_url': stmt.excluded.image_url,
                'updated_at': stmt.excluded.updated_at
            }
        )
        
        session.execute(stmt)