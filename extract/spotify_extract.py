# extract/spotify_extract.py
import requests
import base64
import json
import os
from datetime import datetime
from typing import Dict, List, Optional

class SpotifyDataExtractor:
    def __init__(self, tokens_path: str = "../tokens/spotify_tokens.json"):
        self.tokens_path = tokens_path
        self.base_url = "https://api.spotify.com/v1"
        self.load_tokens()
    
    def load_tokens(self):
        """Load tokens from tokens directory"""
        try:
            with open(self.tokens_path, 'r') as f:
                self.tokens = json.load(f)
        except FileNotFoundError:
            raise Exception(f"Token file not found at {self.tokens_path}")
    
    def refresh_token(self):
        """Refresh access token if needed"""
        if datetime.now() >= datetime.fromisoformat(self.tokens['token_expires']):
            auth_string = f"{self.tokens['client_id']}:{self.tokens['client_secret']}"
            auth_bytes = auth_string.encode('utf-8')
            auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')
            
            headers = {
                "Authorization": f"Basic {auth_base64}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.tokens['refresh_token']
            }
            
            response = requests.post("https://accounts.spotify.com/api/token", headers=headers, data=data)
            response.raise_for_status()
            
            new_tokens = response.json()
            self.tokens['access_token'] = new_tokens['access_token']
            self.tokens['token_expires'] = (datetime.now() + timedelta(seconds=3600)).isoformat()
            
            # Save updated tokens
            with open(self.tokens_path, 'w') as f:
                json.dump(self.tokens, f, indent=2)
    
    def make_spotify_request(self, endpoint: str, params: Dict = None):
        """Make authenticated request to Spotify API"""
        self.refresh_token()
        
        headers = {
            "Authorization": f"Bearer {self.tokens['access_token']}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(f"{self.base_url}{endpoint}", headers=headers, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def extract_all_data(self) -> Dict:
        """Extract all Spotify data"""
        data = {}
        
        # User profile
        data['profile'] = self.make_spotify_request("/me")
        
        # Top tracks
        data['top_tracks'] = self.make_spotify_request(
            "/me/top/tracks", 
            {'limit': 50, 'time_range': 'medium_term'}
        )['items']
        
        # Recently played
        data['recently_played'] = self.make_spotify_request(
            "/me/player/recently-played",
            {'limit': 50}
        )['items']
        
        # Saved tracks
        data['saved_tracks'] = self.make_spotify_request(
            "/me/tracks",
            {'limit': 50}
        )['items']
        
        return data