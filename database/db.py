# db.py
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from utils.config import DB_PATH
from sqlalchemy.orm import sessionmaker

SQLALCHEMY_DATABASE_URI = f'sqlite:///{DB_PATH}'
SQLALCHEMY_TRACK_MODIFICATIONS = False

engine = create_engine(SQLALCHEMY_DATABASE_URI)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Dependency for sessions (optional)
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()