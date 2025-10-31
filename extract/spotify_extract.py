# extract/spotify_extract.py
import requests
import os
from typing import Dict
from extract.auth import SpotifyAuth

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
    
    def extract_all_data(self) -> Dict:
        """Extract all Spotify data"""
        data = {}

        # Top tracks
        data['top_tracks'] = self.make_spotify_request(
            "/me/top/tracks", 
            {'limit': 50, 'time_range': 'medium_term'}
        )['items']

        # Top artists
        data['top_artists'] = self.make_spotify_request(
            "/me/top/artists",
            {'limit': 50, 'time_range': 'medium_term'}
        )['items']

        # Recently played
        data['recently_played'] = self.make_spotify_request(
            "/me/player/recently-played",
            {'limit': 50}
        )['items']
        
        return data 