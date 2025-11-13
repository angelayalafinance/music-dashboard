# extract/spotify_extract.py
import requests
from typing import Dict, List
from data_processing.extract.auth import SpotifyAuth

class SpotifyDataExtractor:
    """Class to extract Spotify user data using SpotifyAuth"""

    def __init__(self):
        self.auth = SpotifyAuth()
        self.base_url = "https://api.spotify.com/v1"
        # Try to load tokens, if not authenticated, run automatic authentication
        if not self.auth.load_tokens():
            print("No valid tokens found. Starting authentication...")
            success = self.auth.automatic_user_authentication()
            if not success:
                raise ValueError("Spotify authentication failed. Cannot proceed.")

    def make_spotify_request(self, endpoint: str, params: Dict = None):
        """Make authenticated request to Spotify API"""
        
        headers = {
            **self.auth.get_auth_header(),
            "Content-Type": "application/json"
        }
        print(headers)

        response = requests.get(f"{self.base_url}{endpoint}", headers=headers, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def search_artist(self, artist_name: str) -> Dict:
        """Search for an artist by name"""
        params = {'q': artist_name, 'type': 'artist', 'limit': 1}
        return self.make_spotify_request("/search", params)
    
    def extract_all_data(self, time_ranges: List[str] = None) -> Dict:
        """
        Extract all Spotify data for transformation
        """
        if time_ranges is None:
            time_ranges = ['short_term', 'medium_term', 'long_term']
        
        data = {}
        
        try:
            # User profile
            data['profile'] = self.make_spotify_request("/me")
            
            # Top tracks for each time range
            data['top_tracks'] = []
            for time_range in time_ranges:
                tracks = self.make_spotify_request(
                    "/me/top/tracks", 
                    {'limit': 50, 'time_range': time_range}
                )['items']
                data['top_tracks'].extend(tracks)
            
            # Top artists for each time range
            data['top_artists'] = []
            for time_range in time_ranges:
                artists = self.make_spotify_request(
                    "/me/top/artists",
                    {'limit': 50, 'time_range': time_range}
                )['items']
                data['top_artists'].extend(artists)
            
            # Recently played
            recently_played = self.make_spotify_request(
                "/me/player/recently-played",
                {'limit': 50}
            )
            data['recently_played'] = recently_played['items']
            
            # Saved tracks
            saved_tracks = self.make_spotify_request(
                "/me/tracks",
                {'limit': 50}
            )
            data['saved_tracks'] = [item['track'] for item in saved_tracks['items']]
            
            return data
            
        except Exception as e:
            print(f"Extraction error: {e}")
            raise