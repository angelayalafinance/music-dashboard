import base64
import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode, urlparse
from typing import Dict
import webbrowser
import time
import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from utils.config import SPOTIFY_TOKEN_PATH, SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET

class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """HTTP handler to capture OAuth callback"""
    def __init__(self, request, client_address, server):
        self.server: OAuthCallbackServer = server
        super().__init__(request, client_address, server)

    def do_GET(self):
        """Handle GET request from Spotify OAuth redirect"""
        try:
            from urllib.parse import parse_qs, urlparse

            # Parse the full URL and extract the query parameters
            query = urlparse(self.path).query
            params = parse_qs(query)

            if 'error' in params:
                self.server.auth_error = params['error'][0]
                self._send_response(400, "❌ Authentication failed. You can close this tab.")
            elif 'code' in params:
                self.server.auth_code = params['code'][0]
                self._send_response(200, "✅ Authentication successful! You can close this tab.")
            else:
                self._send_response(400, "❌ Missing authorization code.")
        except Exception as e:
            self._send_response(500, f"❌ Error processing request: {e}")

    def log_message(self, format, *args):
        # Suppress console logging from HTTP server
        return

    def _send_response(self, code, message):
        """Send a simple HTML response to the browser"""
        self.send_response(code)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(f"<html><body><h2>{message}</h2></body></html>".encode("utf-8"))

class OAuthCallbackServer(HTTPServer):
    """Custom server to store authentication results"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.auth_code = None
        self.auth_error = None
        self.timeout_reached = False

class SpotifyAuth:
    """Extract your personal Spotify data with automatic authentication """
    def __init__(self, client_id: str = None, client_secret: str = None, redirect_uri: str = None):
        self.client_id = client_id or SPOTIFY_CLIENT_ID
        self.client_secret = client_secret or SPOTIFY_CLIENT_SECRET

        # Use a valid redirect URI - you MUST register this in your Spotify app settings
        self.redirect_uri = redirect_uri or "http://127.0.0.1:8050/"
        self.auth_url = "https://accounts.spotify.com/api/token"
        
        self.access_token = None
        self.refresh_token = None
        self.token_expires = None
        
        if not self.client_id or not self.client_secret:
            raise ValueError("Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET environment variables")

    def automatic_user_authentication(self, timeout: int = 120) -> bool:
        """
        Automatic authentication with local callback server
        Returns True if successful, False otherwise
        """
        
        # Define scopes for the data we want to access
        scopes = [
            'user-read-private',
            'user-read-email',
            'user-read-recently-played', 
            'user-top-read',
            'user-library-read',
            'playlist-read-private',
            'playlist-read-collaborative'
        ]
        
        # Generate the authorization URL
        params = {
            'client_id': self.client_id,
            'response_type': 'code',
            'redirect_uri': self.redirect_uri,
            'scope': ' '.join(scopes),
            'show_dialog': 'false'
        }
        
        auth_url = f"https://accounts.spotify.com/authorize?{urlencode(params)}"
        
        # Parse redirect URI to get port
        redirect_host = 'localhost'
        redirect_port = 8080
        
        try:
            parsed_uri = urlparse(self.redirect_uri)
            redirect_host = parsed_uri.hostname or 'localhost'
            redirect_port = parsed_uri.port or 8080
        except:
            pass
        
        # Start local server to handle callback
        server = OAuthCallbackServer((redirect_host, redirect_port), OAuthCallbackHandler)
        
        def start_server():
            try:
                server.serve_forever()
            except Exception as e:
                print(f"❌ Server error: {e}")
        
        server_thread = threading.Thread(target=start_server, daemon=True)
        server_thread.start()
        
        time.sleep(1)  # Give server a moment to start
        
        # Open browser for authentication
        webbrowser.open(auth_url)
        
        # Wait for authentication with timeout
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            if server.auth_code or server.auth_error:
                break
            time.sleep(0.5)
        
        if server.auth_code:
            server.shutdown()
            return self._exchange_code_for_token(server.auth_code)
        elif server.auth_error:
            server.shutdown()
            return False
        else:
            server.shutdown()
            return False

    def _exchange_code_for_token(self, auth_code: str) -> bool:
        """Exchange authorization code for access token"""
        
        auth_string = f"{self.client_id}:{self.client_secret}"
        auth_bytes = auth_string.encode('utf-8')
        auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')
        
        headers = {
            "Authorization": f"Basic {auth_base64}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        data = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": self.redirect_uri
        }
        
        try:
            response = requests.post(self.auth_url, headers=headers, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            self.refresh_token = token_data.get('refresh_token')
            
            expires_in = token_data.get('expires_in', 3600)
            self.token_expires = datetime.now() + timedelta(seconds=expires_in - 300)
            
            # Save tokens to file for future use
            self._save_tokens()
            return True
            
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                print(f"❌ Token exchange failed: {e.response.status_code} - {e.response.text}")
            return False

    def _save_tokens(self) -> None:
        """Save tokens to file for future use"""
        token_data = {
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'token_expires': self.token_expires.isoformat() if self.token_expires else None,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'redirect_uri': self.redirect_uri
        }
        
        with open(SPOTIFY_TOKEN_PATH, 'w') as f:
            json.dump(token_data, f, indent=2)

    def load_tokens(self) -> bool:
        """Load tokens from file"""
        try:
            with open(SPOTIFY_TOKEN_PATH, 'r') as f:
                token_data = json.load(f)
            
            self.access_token = token_data['access_token']
            self.refresh_token = token_data['refresh_token']
            self.token_expires = datetime.fromisoformat(token_data['token_expires'])
            
            # Only use saved client credentials if not provided
            if not self.client_id:
                self.client_id = token_data['client_id']
            if not self.client_secret:
                self.client_secret = token_data['client_secret']
            if not self.redirect_uri:
                self.redirect_uri = token_data['redirect_uri']
            
            return True
        except (FileNotFoundError, KeyError, ValueError) as e:
            return False

    def _refresh_access_token(self) -> None:
        """Refresh access token using refresh token"""
        if not self.refresh_token:
            raise ValueError("No refresh token available. Please re-authenticate.")
        
        auth_string = f"{self.client_id}:{self.client_secret}"
        auth_bytes = auth_string.encode('utf-8')
        auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')
        
        headers = {
            "Authorization": f"Basic {auth_base64}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token
        }
        
        response = requests.post(self.auth_url, headers=headers, data=data)
        response.raise_for_status()
        
        token_data = response.json()
        self.access_token = token_data['access_token']
        
        expires_in = token_data.get('expires_in', 3600)
        self.token_expires = datetime.now() + timedelta(seconds=expires_in - 300)
        
        # Save updated tokens
        self._save_tokens()

    def get_auth_header(self) -> Dict[str, str]:
        """Get authorization header for requests"""
        if not self.access_token or datetime.now() >= self.token_expires:
            if self.refresh_token:
                self._refresh_access_token()
            else:
                raise ValueError("Not authenticated. Please run automatic_user_authentication() first.")
        return {"Authorization": f"Bearer {self.access_token}"}