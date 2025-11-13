import pandas as pd
import os
from data_processing.extract.geocoder import VenueGeocoder
from database.db import SessionLocal, engine
from database.models import MusicVenue, ShowArtist, ShowEvent

TEST_DATA_PATH = os.path.join('storage', 'test_data')\

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

def bulk_insert_records(data, model) -> None:
    session = SessionLocal()
    try:
        session.bulk_insert_mappings(model, data)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error inserting venues: {e}")
    finally:
        session.close()

def insert_venues():
    # Load the legacy venues
    file_path = os.path.join(TEST_DATA_PATH, 'venues.csv')
    venues = pd.read_csv(file_path)

    venues_df = process_venue_data(venues)

    bulk_insert_records(venues_df, MusicVenue)

def process_show_data(df):
    """Process show data from a DataFrame"""
    shows_df = df[['venue_id', 'name', 'show_date', 'ticket_price', 'is_festival']]
    shows_df.columns = ['venue_id', 'date', 'ticket_price', 'is_festival']
    shows_df['date'] = pd.to_datetime(shows_df['date'])
    shows_df['is_festival'] = shows_df['is_festival'].fillna(False).astype(bool)
    return shows_df.to_dict(orient='records')



def insert_show_events():
    # Load the legacy show events
    file_path = os.path.join(TEST_DATA_PATH, 'shows.csv')
    shows = pd.read_csv(file_path)
    shows_df = process_show_data(shows)

    bulk_insert_records(shows_df, ShowEvent)