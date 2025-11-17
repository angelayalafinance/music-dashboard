# transform/spotify_transform.py
from datetime import datetime, timedelta
from typing import Dict, List, Any
from utils.logger import etl_logger
from uuid import uuid4

class SpotifyDataTransformer:
    def __init__(self):
        self.execution_date = datetime.now()
    
    def transform_all_data(self, raw_data: Dict, time_range: str = 'medium_term') -> Dict:
        """
        Transform all Spotify raw data into database-ready format
        """
        transformed = {
            'artists': [],
            'top_tracks': [],
            'top_artists': [],
            'listening_history': []
        }
        
        try:
            # Transform artists from multiple sources
            transformed['artists'] = self._transform_artists(raw_data)
            
            # Transform top tracks with ranking
            if 'top_tracks' in raw_data:
                transformed['top_tracks'] = self._transform_top_tracks(
                    raw_data['top_tracks'], 
                    time_range
                )
            
            # Transform top artists with ranking
            if 'top_artists' in raw_data:
                transformed['top_artists'] = self._transform_top_artists(
                    raw_data['top_artists'],
                    time_range
                )
            
            # Transform listening history
            if 'recently_played' in raw_data:
                transformed['listening_history'] = self._transform_listening_history(
                    raw_data['recently_played']
                )
            
            etl_logger.info(f"Transformed {len(transformed['artists'])} artists, "
                       f"{len(transformed['top_tracks'])} top tracks, "
                       f"{len(transformed['top_artists'])} top artists, "
                       f"{len(transformed['listening_history'])} listening records")
            
            return transformed
            
        except Exception as e:
            etl_logger.error(f"Transformation failed: {e}")
            raise
    
    def _transform_artists(self, raw_data: Dict) -> List[Dict]:
        """
        Extract and transform artist data from all sources
        Deduplicates artists across top tracks, top artists, and recently played
        """
        artists_map = {}
        
        # Extract from top tracks
        if 'top_tracks' in raw_data:
            for track in raw_data['top_tracks']:
                for artist in track['artists']:
                    self._add_artist_to_map(artists_map, artist)
        
        # Extract from top artists
        if 'top_artists' in raw_data:
            for artist_data in raw_data['top_artists']:
                self._add_artist_to_map(artists_map, artist_data)
        
        # Extract from recently played
        if 'recently_played' in raw_data:
            for item in raw_data['recently_played']:
                for artist in item['track']['artists']:
                    self._add_artist_to_map(artists_map, artist)
        
        # Convert to list and format for database
        artists_list = []
        for artist_id, artist_data in artists_map.items():
            artists_list.append({
                'id': artist_id,
                'name': artist_data['name'],
                'genre': ', '.join(artist_data.get('genres', []))[:100],  # Truncate if too long
                'popularity': artist_data.get('popularity'),
                'followers': artist_data.get('followers', {}).get('total'),
                'spotify_url': artist_data.get('external_urls', {}).get('spotify'),
                'image_url': self._get_artist_image(artist_data),
                'created_at': self.execution_date,
                'updated_at': self.execution_date
            })
        
        return artists_list
    
    def _add_artist_to_map(self, artists_map: Dict, artist_data: Dict):
        """Add artist to map, keeping the most complete data"""
        artist_id = artist_data['id']
        
        if artist_id not in artists_map:
            artists_map[artist_id] = artist_data
        else:
            # Merge data, preferring non-null values
            existing = artists_map[artist_id]
            for key, value in artist_data.items():
                if value is not None and (existing.get(key) is None or key == 'popularity'):
                    existing[key] = value
    
    def _get_artist_image(self, artist_data: Dict) -> str:
        """Extract the best available artist image URL"""
        images = artist_data.get('images', [])
        if images:
            # Prefer medium size, fall back to first available
            medium_images = [img for img in images if img.get('height', 0) in [300, 320, 400]]
            if medium_images:
                return medium_images[0]['url']
            return images[0]['url']
        return None
    
    def _transform_top_tracks(self, top_tracks: List[Dict], time_range: str) -> List[Dict]:
        """Transform top tracks data with ranking"""
        transformed_tracks = []
        
        for rank, track in enumerate(top_tracks, 1):
            # Use the first artist as primary (most common case)
            primary_artist = track['artists'][0]
            
            transformed_tracks.append({
                'track_id': track['id'],
                'name': track['name'],
                'artist_id': primary_artist['id'],
                'album_name': track['album']['name'],
                'album_id': track['album']['id'],
                'popularity': track['popularity'],
                'duration_ms': track['duration_ms'],
                'explicit': track['explicit'],
                'extracted_date': self.execution_date,
                'time_range': time_range,
                'rank': rank,
                'created_at': self.execution_date
            })
        
        return transformed_tracks
    
    def _transform_top_artists(self, top_artists: List[Dict], time_range: str) -> List[Dict]:
        """Transform top artists data with ranking"""
        transformed_artists = []
        
        for rank, artist_data in enumerate(top_artists, 1):
            transformed_artists.append({
                'artist_id': artist_data['id'],
                'extracted_date': self.execution_date,
                'time_range': time_range,
                'rank': rank,
                'created_at': self.execution_date
            })
        
        return transformed_artists
    
    def _transform_listening_history(self, recently_played: List[Dict]) -> List[Dict]:
        """Transform recently played tracks into listening history"""
        history = []
        
        for item in recently_played:
            track = item['track']
            # Use the first artist
            primary_artist = track['artists'][0]
            
            history.append({
                'track_id': track['id'],
                'track_name': track['name'],
                'artist_id': primary_artist['id'],
                'artist_name': primary_artist['name'],
                'played_at': datetime.fromisoformat(item['played_at'].replace('Z', '+00:00')),
                'extracted_at': self.execution_date,
                'created_at': self.execution_date
            })
        
        return history
    
    def handle_artist_not_found(self, artist_name: str) -> Dict:
        """
        Handle cases where an artist is not found in Spotify data
        Returns a placeholder artist record
        """
        etl_logger.warning(f"Artist '{artist_name}' not found in Spotify data.")
        return {
            'id': str(uuid4()),
            'name': artist_name,
            'genre': None,
            'popularity': None,
            'followers': None,
            'spotify_url': None,
            'image_url': None,
            'created_at': self.execution_date,
            'updated_at': self.execution_date
        }