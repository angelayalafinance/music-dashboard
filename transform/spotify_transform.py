# transform/spotify_transform.py
import pandas as pd
from datetime import datetime
from typing import Dict, List

class DataTransformer:
    def __init__(self):
        pass
    
    def transform_spotify_data(self, raw_data: Dict) -> Dict:
        """Transform raw Spotify data into structured format"""
        transformed = {}
        
        # Transform profile
        profile = raw_data['profile']
        transformed['profile'] = [{
            'user_id': profile['id'],
            'display_name': profile.get('display_name', ''),
            'email': profile.get('email', ''),
            'country': profile.get('country', ''),
            'product': profile.get('product', ''),
            'followers': profile['followers']['total'],
            'extracted_at': datetime.now().isoformat()
        }]
        
        # Transform top tracks
        transformed['top_tracks'] = [
            self._transform_track_data(track, 'top_tracks') 
            for track in raw_data['top_tracks']
        ]
        
        # Transform recently played
        transformed['recently_played'] = [
            self._transform_recent_play(item)
            for item in raw_data['recently_played']
        ]
        
        return transformed
    
    def _transform_track_data(self, track: Dict, source: str) -> Dict:
        """Transform individual track data"""
        return {
            'track_id': track['id'],
            'track_name': track['name'],
            'artist_id': track['artists'][0]['id'],
            'artist_name': track['artists'][0]['name'],
            'album_id': track['album']['id'],
            'album_name': track['album']['name'],
            'popularity': track['popularity'],
            'duration_ms': track['duration_ms'],
            'explicit': track['explicit'],
            'source': source,
            'extracted_at': datetime.now().isoformat()
        }
    
    def _transform_recent_play(self, item: Dict) -> Dict:
        """Transform recently played item"""
        track = item['track']
        return {
            'track_id': track['id'],
            'track_name': track['name'],
            'artist_name': track['artists'][0]['name'],
            'played_at': item['played_at'],
            'extracted_at': datetime.now().isoformat()
        }