# data_processing/extract/artist_extractor.py
from database.models import Artist
from database.db_manager import DatabaseManager
from data_processing.extract.spotify_extract import SpotifyDataExtractor

def search_and_extract_artist(artist_name: str) -> dict:
    """Search for an artist by name and extract relevant info"""
    extract = SpotifyDataExtractor()
    result = extract.search_artist(artist_name)
    if result['artists']['items']:
        artist_info = result['artists']['items'][0]
        return {
            'id': artist_info['id'],
            'name': artist_info['name'],
            'genre': ', '.join(artist_info['genres']),
            'popularity': artist_info['popularity'],
            'followers': artist_info['followers']['total'],
            'spotify_url': artist_info['external_urls']['spotify'],
            'image_url': artist_info['images'][0]['url'] if artist_info['images'] else None
        }
    return None


def add_artist_not_in_db(artist_name: str, db_manager: DatabaseManager) -> str:
    """Search for an artist and add to the database if not found"""
    result = search_and_extract_artist(artist_name)
    if result:
        success = db_manager.bulk_insert([result], Artist)
        return result['id'] if success else None