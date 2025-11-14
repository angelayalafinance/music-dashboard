import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import os
from data_processing.extract.geocoder import VenueGeocoder
from database.models import MusicVenue, ShowArtist, ShowEvent, Artist
from database.db_manager import DatabaseManager
from utils.config import ROOT_DIR
from data_processing.extract.artist_extract import add_artist_not_in_db

TEST_DATA_PATH = os.path.join(ROOT_DIR, 'storage', 'test_data')

def process_venue_data(df):
    """Process and geocode venue data from a DataFrame"""
    geocoder = VenueGeocoder()
    venues_df = geocoder.geocode_venue_dataframe(df)

    venues_df['location'] = venues_df.apply(
        lambda row: f"{row['venue_address']}, {row['venue_city']}, {row['venue_state']}", axis=1
    )
    return venues_df\
        [['venue_name', 'location', 'latitude', 'longitude']]\
        .rename(columns={'venue_name': 'name'})\
        .to_dict(orient='records')


def get_csv_file(file_name: str) -> pd.DataFrame:
    """Load a CSV file from the test data directory"""
    file_path = os.path.join(TEST_DATA_PATH, file_name)
    return pd.read_csv(file_path)

def insert_venues(db):
    """Insert music venues into the database"""
    venues = get_csv_file('venues.csv')
    venues_df = process_venue_data(venues)
    db.bulk_insert(venues_df, MusicVenue)

def process_show_data(df):
    """Process show data from a DataFrame"""
    shows_df = df[['venue_id', 'show_name', 'show_date', 'ticket_price', 'is_festival']]
    shows_df.columns = ['venue_id', 'event', 'date', 'ticket_price', 'is_festival']
    shows_df['date'] = pd.to_datetime(shows_df['date'])
    shows_df['is_festival'] = shows_df['is_festival'].fillna(False).astype(bool)
    return shows_df.to_dict(orient='records')

def insert_show_events(db):
    """Insert show events into the database"""
    shows = get_csv_file('shows.csv')
    shows_df = process_show_data(shows)
    db.bulk_insert(shows_df, ShowEvent)


def process_show_artist_data(df):
    """Process show artist data from a DataFrame"""
    df = pd.read_csv(os.path.join(TEST_DATA_PATH, 'show_artists.csv'))

    show_artists = df['artist'].str.lower()\
                                .str.replace(' ', '_')\
                                .unique()\
                                .tolist()
    
    db = DatabaseManager()

    artists_list = db.get_all(Artist)  # List of Artist objects
    artists_df = pd.DataFrame([{'id': artist.id, 'name': artist.name} for artist in artists_list])


    existing_artsits = artists_df['name'].str.lower()\
                                      .str.replace(' ', '_')\
                                      .tolist()
    
    artists_to_fetch = list(set(show_artists) - set(existing_artsits))

    for artist_name in artists_to_fetch:
        name = artist_name.replace('_', ' ').title()
        add_artist_not_in_db(name, db)
        break


    show_artists_df.columns = ['show_id', 'name', 'performance_time', 'is_headliner']
    show_artists_df['performance_time'] = pd.to_datetime(show_artists_df['performance_time'])
    show_artists_df['is_headliner'] = show_artists_df['is_headliner'].fillna(False).astype(bool)
    return show_artists_df.to_dict(orient='records')



def main():
    db = DatabaseManager()
    insert_venues(db)
    insert_show_events(db)
    insert_show_artists(db)