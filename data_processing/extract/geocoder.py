# data_processing/extract/geocoder.py
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
from typing import Tuple, Optional

class VenueGeocoder:
    """Simple geocoding service for music venues"""
    
    def __init__(self):
        self.geolocator = Nominatim(user_agent="music_dashboard_app")
        
    def geocode_venue(self, venue_name: str, address: str = None, 
                     city: str = None, state: str = None) -> Tuple[Optional[float], Optional[float]]:
        """Geocode a venue with multiple fallback strategies """
        # Try different address strategies
        strategies = [
            f"{address}, {city}, {state}" if address and city and state else None,
            f"{venue_name}, {city}, {state}" if city and state else None,
            f"{venue_name}, {city}" if city else None,
            venue_name
        ]
        
        for search_query in strategies:
            if not search_query:
                continue
                
            try:
                coords = self._safe_geocode(search_query)
                if coords:
                    return coords
                    
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                print(f"Geocoding error for '{search_query}': {e}")
                continue
        
        return None, None
    
    def geocode_venue_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Geocode all venues in a DataFrame"""
        df = df.copy()
        df['latitude'] = None
        df['longitude'] = None
        
        for idx, row in df.iterrows():
            lat, lon = self.geocode_venue(
                venue_name=row['venue_name'],
                address=row.get('venue_address'),
                city=row.get('venue_city'),
                state=row.get('venue_state')
            )
            
            df.at[idx, 'latitude'] = lat
            df.at[idx, 'longitude'] = lon
        
        return df
    
    def _safe_geocode(self, query: str) -> Optional[Tuple[float, float]]:
        """Safe geocoding with error handling"""
        try:
            location = self.geolocator.geocode(query)
            return (location.latitude, location.longitude) if location else None
        except GeocoderTimedOut:
            return None