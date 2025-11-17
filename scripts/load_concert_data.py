import pandas as pd
import os
from data_processing.extract.geocoder import VenueGeocoder
from database.models import MusicVenue, ShowArtist, ShowEvent, Artist
from database.db_manager import DatabaseManager
from utils.config import ROOT_DIR
from data_processing.extract.artist_extract import add_artist_not_in_db
from data_processing.transform.utils import clean_artist_name
from database.db_manager import DatabaseManager
from data_processing.transform.spotify_transform import SpotifyDataTransformer

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


def get_existing_artists(db: DatabaseManager) -> set:
    """Retrieve existing artist names from the database"""
    return db.get_all(Artist)

def get_existing_artist_names(db: DatabaseManager) -> set:
    """Retrieve existing artist names from the database"""
    existing_artists = db.get_all(Artist)
    return set(existing_artists['name'].apply(clean_artist_name).tolist())

def add_unmatched_artists(artist_name: str, db: DatabaseManager):
    """Add artists not already in the database"""
    transform = SpotifyDataTransformer()
    record_to_add = transform.handle_artist_not_found(artist_name)
    db.bulk_insert([record_to_add], Artist)


def insert_show_artists(db):
    """Insert show artists into the database"""
    df = get_csv_file('show_artists.csv')
    show_artists_df = process_show_artist_data(df)
    db.bulk_insert(show_artists_df, ShowArtist)


def process_show_artist_data(df):
    """Process show artist data from a DataFrame"""
    df['cleaned_name'] = df['artist'].apply(clean_artist_name)
    show_artists = df['cleaned_name'].unique().tolist()
    
    db = DatabaseManager()

    # existing_artist_list = get_existing_artist_names(db)
    
    # artists_to_fetch = list(set(show_artists) - set(existing_artist_list))

    # for artist_name in artists_to_fetch:
    #     name = artist_name.replace('_', ' ').title()
    #     add_artist_not_in_db(name, db)

    # # Fetch updated artist list from DB
    # existing_artist_list = get_existing_artist_names(db)

    # # Get the list of artists that don't have a match
    # still_missing = list(set(show_artists) - set(existing_artist_list))
    # still_missing_df = df[df['cleaned_name'].isin(still_missing)]
    # for artist_name in still_missing_df['artist'].unique().tolist():
    #     add_unmatched_artists(artist_name, db)

    # Now map artist names to IDs
    existing_artists = db.get_all(Artist)
    existing_artists['cleaned_name'] = existing_artists['name'].apply(clean_artist_name)

    df = df.merge(existing_artists[['id', 'cleaned_name']], on='cleaned_name', how='left', suffixes=('', '_db'))
    df['is_headliner'] = df['is_headliner'].fillna(False).astype(bool)

    df.rename(columns={'id': 'artist_id', 'show_id': 'show_id'}, inplace=True)
    df.drop(columns=['artist', 'cleaned_name'], inplace=True)

    return df.to_dict(orient='records')


def main():
    db = DatabaseManager()
    insert_venues(db)
    insert_show_events(db)
    insert_show_artists(db)
