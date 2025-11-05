


from data_processing.extract.spotify_extract import SpotifyDataExtractor
from data_processing.load.db_loader import DatabaseLoader
from data_processing.transform.spotify_transform import SpotifyDataTransformer
from utils.config import DB_URL
import json

def test_spotify_etl_pipeline():
    # Initialize extractor
    extractor = SpotifyDataExtractor()
    
    # Extract data
    raw_data = extractor.extract_all_data()

    assert 'profile' in raw_data
    assert 'top_tracks' in raw_data
    assert 'top_artists' in raw_data
    
    # Transform data
    transformer = SpotifyDataTransformer()
    transformed_data = transformer.transform_all_data(raw_data)
    assert 'artists' in transformed_data
    assert 'top_tracks' in transformed_data
    assert 'top_artists' in transformed_data
    assert 'listening_history' in transformed_data
    
    # Load data
    loader = DatabaseLoader(db_url=DB_URL)
    loader.load_spotify_data(transformed_data)