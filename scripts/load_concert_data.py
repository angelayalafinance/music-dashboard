import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))



import pandas as pd
import os
from data_processing.extract.geocoder import VenueGeocoder
from database.db import SessionLocal, engine
from database.models import MusicVenue, ShowArtist, ShowEvent, Artist
from data_processing.extract.spotify_extract import SpotifyDataExtractor

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

def process_show_artist_data(df):
    """Process show artist data from a DataFrame"""
    df = pd.read_csv(os.path.join(TEST_DATA_PATH, 'show_artists.csv'))
    show_artists_df = df[['show_id', 'artist_name', 'performance_time', 'is_headliner']]


    #### TO do: Query the Database for Artists,
    artists = SessionLocal().query(Artist).all()
    artists = pd.DataFrame([{
        'id': artist.id,
        'name': artist.name
    } for artist in artists])
    ### If artist not found, use the Spotify Extractor to search for the artist
    extract = SpotifyDataExtractor()

    def add_artist_not_in_db(artist_name: str):
        result = extract.search_artist(artist_name)
        if result['artists']['items']:
            artist_info = result['artists']['items'][0]
            new_artist = {
                'id': artist_info['id'],
                'name': artist_info['name'],
                'genre': ', '.join(artist_info['genres']),
                'popularity': artist_info['popularity'],
                'followers': artist_info['followers']['total'],
                'spotify_url': artist_info['external_urls']['spotify'],
                'image_url': artist_info['images'][0]['url'] if artist_info['images'] else None
            }
            bulk_insert_records([new_artist], Artist)
            return new_artist['id']
        return None



    show_artists = show_artists_df['artist_name'].str.lower()\
                                                 .str.replace(' ', '_')\
                                                 .unique()\
                                                 .tolist()

    existing_artsits = artists['name'].str.lower()\
                                      .str.replace(' ', '_')\
                                      .tolist()
    
    artists_to_fetch = list(set(show_artists) - set(existing_artsits))

    fetched_artists = []
    for artist_name in artists_to_fetch:
        artist_name = artist_name.replace('_', ' ').title()
        result = extract.search_artist(artist_name)
        if result['artists']['items']:
            artist_info = result['artists']['items'][0]
            new_artist = {
                'id': artist_info['id'],
                'name': artist_info['name'],
                'genre': ', '.join(artist_info['genres']),
                'popularity': artist_info['popularity'],
                'followers': artist_info['followers']['total'],
                'spotify_url': artist_info['external_urls']['spotify'],
                'image_url': artist_info['images'][0]['url'] if artist_info['images'] else None
            }
            fetched_artists.append(new_artist)


            bulk_insert_records([new_artist], Artist)





    show_artists_df.columns = ['show_id', 'name', 'performance_time', 'is_headliner']
    show_artists_df['performance_time'] = pd.to_datetime(show_artists_df['performance_time'])
    show_artists_df['is_headliner'] = show_artists_df['is_headliner'].fillna(False).astype(bool)
    return show_artists_df.to_dict(orient='records')