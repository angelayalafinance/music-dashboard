import os
from dotenv import load_dotenv

# Get the root directory one step back from the current directory
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

# Load environment variables
load_dotenv(os.path.join(ROOT_DIR, '.env'))

############################################################################
# FLASK API CONFIGURATION
############################################################################
API_TOKEN = os.getenv('API_TOKEN')
API_URL = os.getenv('API_URL')


load_dotenv()

SPOTIFY_CK = os.getenv("SPOTIFY_CK")
SPOTIFY_CS = os.getenv("SPOTIFY_CS")