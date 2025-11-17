def clean_artist_name(artist_name: str) -> str:
    """Clean and standardize artist name for searching and comparison"""
    return artist_name.strip()\
                      .lower()\
                      .replace(' ', '_')\
                      .replace('-', '_')\
                      .replace("'", '')