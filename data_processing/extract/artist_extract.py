# data_processing/extract/artist_extractor.py
from database.models import Artist
from database.db_manager import DatabaseManager
from data_processing.extract.spotify_extract import SpotifyDataExtractor


def clean_artist_name(artist_name: str) -> str:
    """Clean and standardize artist name for searching"""
    return artist_name.strip()\
                      .lower()\
                      .replace(' ', '_')\
                      .replace('-', '_')\
                      .replace("'", '')

def find_artist_match(artist_results: dict, artist_name: str) -> dict:
    """Find the best matching artist from search results"""
    artist_name_cleaned = clean_artist_name(artist_name)
    for artist in artist_results['artists']['items']:
        if clean_artist_name(artist['name']) == artist_name_cleaned:
            return artist
    return None

def search_and_extract_artist(artist_name: str) -> dict:
    """Search for an artist by name and extract relevant info"""
    extract = SpotifyDataExtractor()
    result = extract.search_artist(artist_name)
    if result['artists']['items']:
        # Check if there are any artist matches 
        artist = find_artist_match(result, artist_name)
        if artist:
            return {
                'id': artist['id'],
                'name': artist['name'],
                'genre': ', '.join(artist['genres']),
                'popularity': artist['popularity'],
                'followers': artist['followers']['total'],
                'spotify_url': artist['external_urls']['spotify'],
                'image_url': artist['images'][0]['url'] if artist['images'] else None
            }
        return None


def add_artist_not_in_db(artist_name: str, db_manager: DatabaseManager) -> str:
    """Search for an artist and add to the database if not found"""
    result = search_and_extract_artist(artist_name)
    print(f"Extracted artist info: {result}")
    if result:
        success = db_manager.bulk_insert([result], Artist)
        return result['id'] if success else None