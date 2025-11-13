import os
from dotenv import load_dotenv

# Get the root directory one step back from the current directory
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Load environment variables
load_dotenv(os.path.join(ROOT_DIR, '.env'))

# Database path
DB_PATH = os.path.join(ROOT_DIR, 'storage', 'database', 'data.db')
DB_URL = f"sqlite:///{DB_PATH}"

# Logs directory path
LOG_DIR = os.path.join(ROOT_DIR, 'log')

# Tokens directory path
TOKENS_DIR = os.path.join(ROOT_DIR, 'tokens')
SPOTIFY_TOKEN_PATH = os.path.join(TOKENS_DIR, 'spotify_token.json')

# Make sure directories exist
os.makedirs(TOKENS_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)


SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")