### To Do: Create the SqlAlchemy Bases for the database tables.
from app.db import Base
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Numeric

class ClassMusicVenue(Base):
    __tablename__ = 'music_venues'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    location = Column(String, nullable=False)
    capacity = Column(Integer)

class ShowEvent(Base):
    __tablename__ = 'show_events'
    
    id = Column(Integer, primary_key=True, index=True)
    venue_id = Column(Integer, ForeignKey('music_venues.id'), nullable=False)
    date = Column(DateTime, nullable=False)
    artist = Column(String, nullable=False)
    ticket_price = Column(Numeric)