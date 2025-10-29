"""
Spotify User Data Extractor - Automated Version
Handles OAuth callback automatically with local server
"""

import os
from auth import SpotifyUserData
import requests
import pandas as pd
from typing import Dict, Any, List

# Add your data extraction methods here
def get_my_profile(self) -> Dict[str, Any]:
    """Get current user's profile"""
    headers = self.get_auth_header()
    response = requests.get(f"{self.base_url}/me", headers=headers)
    response.raise_for_status()
    return response.json()

def get_my_top_tracks(self, limit: int = 20, time_range: str = 'long_term') -> List[Dict]:
    """Get user's top tracks"""
    headers = self.get_auth_header()
    params = {
        'limit': limit,
        'time_range': time_range  # short_term, medium_term, long_term
    }
    
    response = requests.get(f"{self.base_url}/me/top/tracks", headers=headers, params=params)
    response.raise_for_status()
    return response.json()['items']

def extract_all_my_data(self, limit: int = 50) -> Dict[str, pd.DataFrame]:
    """Extract all available user data"""
    print("📊 Extracting your Spotify data...")
    
    data = {}
    
    try:
        # Get user profile
        profile = self.get_my_profile()
        data['profile'] = pd.DataFrame([{
            'user_id': profile['id'],
            'display_name': profile.get('display_name', ''),
            'email': profile.get('email', ''),
            'country': profile.get('country', ''),
            'product': profile.get('product', ''),
            'followers': profile['followers']['total']
        }])
        
        # Get top tracks
        top_tracks = self.get_my_top_tracks(limit=limit)
        tracks_data = []
        for track in top_tracks:
            tracks_data.append({
                'track_id': track['id'],
                'track_name': track['name'],
                'artist_name': track['artists'][0]['name'],
                'artist_id': track['artists'][0]['id'],
                'album_name': track['album']['name'],
                'popularity': track['popularity'],
                'duration_ms': track['duration_ms'],
                'explicit': track['explicit']
            })
        data['top_tracks'] = pd.DataFrame(tracks_data)
        
    except Exception as e:
        print(f"❌ Error extracting data: {e}")
    
    return data

def main():
    """Main function to extract your Spotify data"""
    
    # Initialize
    spotify = SpotifyUserData()
    
    # Try to load existing tokens first
    if not spotify.load_tokens():
        print("🔐 No valid tokens found. Starting automatic authentication...")
        if not spotify.automatic_user_authentication():
            print("❌ Authentication failed. Please check your credentials and try again.")
            return
    
    # If we have tokens (either loaded or newly authenticated), get data
    if spotify.access_token:
        try:
            # Test authentication
            profile = spotify.get_my_profile()
            print(f"🎉 Successfully authenticated as: {profile.get('display_name', 'Unknown')}")
            
            # Extract data
            my_data = spotify.extract_all_my_data(limit=20)
            
            # Show top tracks
            if not my_data['top_tracks'].empty:
                print(f"\n🎵 Your Top Tracks:")
                for i, (_, track) in enumerate(my_data['top_tracks'].head(5).iterrows(), 1):
                    print(f"   {i}. {track['track_name']} - {track['artist_name']}")
            
            # Save data to files
            for data_type, df in my_data.items():
                filename = f"spotify_{data_type}.csv"
                df.to_csv(filename, index=False)
                print(f"💾 Saved {data_type} to {filename}")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            print("🔧 Re-authenticating...")
            if spotify.automatic_user_authentication():
                print("✅ Re-authentication successful!")
            else:
                print("❌ Re-authentication failed.")


if __name__ == "__main__":
    main()