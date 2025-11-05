### create_tables.py ###
from database.db import Base, engine  
from database.models import *      

def create_tables():
    """Creates all database tables defined in models."""
    Base.metadata.create_all(bind=engine)
    print("All tables created successfully.")

if __name__ == "__main__":
    create_tables()  # Run directly when needed