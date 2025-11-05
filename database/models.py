from database.db import Base
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Numeric, Boolean, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

class MusicVenue(Base):
    __tablename__ = 'music_venues'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    location = Column(String, nullable=False)
    capacity = Column(Integer)
    latitude = Column(Numeric(10, 6))  # For maps
    longitude = Column(Numeric(10, 6))  # For maps
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationship
    shows = relationship("ShowEvent", back_populates="venue")

class ShowEvent(Base):
    __tablename__ = 'show_events'
    
    id = Column(Integer, primary_key=True, index=True)
    venue_id = Column(Integer, ForeignKey('music_venues.id'), nullable=False)
    date = Column(DateTime, nullable=False)
    ticket_price = Column(Numeric)
    notes = Column(Text)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    venue = relationship("MusicVenue", back_populates="shows")
    artists = relationship("ShowArtist", back_populates="show")


class ShowArtist(Base):
    __tablename__ = 'show_artists'
    
    id = Column(Integer, primary_key=True, index=True)
    artist_id = Column(String, ForeignKey('artists.id'), nullable=False)
    show_id = Column(Integer, ForeignKey('show_events.id'), nullable=False)
    set_order = Column(Integer)  # Performance order
    is_headliner = Column(Boolean, default=False)
    set_rating = Column(Integer)  # Rating out of 10
    notes = Column(Text)
    created_at = Column(DateTime, server_default=func.now())
    
    # Relationships
    artist = relationship("Artist", back_populates="show_appearances")
    show = relationship("ShowEvent", back_populates="artists")

class Artist(Base):
    __tablename__ = 'artists'
    
    id = Column(String, primary_key=True)  # Spotify artist ID
    name = Column(String, nullable=False)
    genre = Column(String)
    popularity = Column(Integer)
    followers = Column(Integer)
    spotify_url = Column(String)
    image_url = Column(String)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    
    # Relationships
    top_tracks = relationship("TopTrack", back_populates="artist")
    top_artist_rankings = relationship("TopArtist", back_populates="artist")
    listening_history = relationship("ListeningHistory", back_populates="artist")
    show_appearances = relationship("ShowArtist", back_populates="artist")

class TopTrack(Base):
    __tablename__ = 'top_tracks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    track_id = Column(String, nullable=False)
    name = Column(String, nullable=False)
    artist_id = Column(String, ForeignKey('artists.id'), nullable=False)
    album_name = Column(String)
    album_id = Column(String)
    popularity = Column(Integer)
    duration_ms = Column(Integer)
    explicit = Column(Boolean)
    extracted_date = Column(DateTime, nullable=False)
    time_range = Column(String, nullable=False)
    rank = Column(Integer)
    created_at = Column(DateTime)
    
    # Relationship
    artist = relationship("Artist", back_populates="top_tracks")

class TopArtist(Base):
    __tablename__ = 'top_artists'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    artist_id = Column(String, ForeignKey('artists.id'), nullable=False)
    extracted_date = Column(DateTime, nullable=False)
    time_range = Column(String, nullable=False)
    rank = Column(Integer, nullable=False)
    created_at = Column(DateTime)
    
    # Relationship
    artist = relationship("Artist", back_populates="top_artist_rankings")

class ListeningHistory(Base):
    __tablename__ = 'listening_history'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    track_id = Column(String, nullable=False)
    track_name = Column(String, nullable=False)
    artist_id = Column(String, ForeignKey('artists.id'))
    artist_name = Column(String)
    played_at = Column(DateTime, nullable=False)
    context = Column(String)
    extracted_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime)
    
    # Relationship
    artist = relationship("Artist", back_populates="listening_history")