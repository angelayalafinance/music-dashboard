# scripts/reset_database.py
from database.db import engine, Base
from scripts import setup_database

def reset_database():
    """Drop and recreate all tables with new schema"""
    
    # Drop all tables
    Base.metadata.drop_all(bind=engine)
    
    # Create all tables with new schema
    setup_database.create_tables()

if __name__ == "__main__":
    reset_database()