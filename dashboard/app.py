from dash import Dash, dcc, html
from flask import Flask
from dash.dependencies import Input, Output, State
from flask_sqlalchemy import SQLAlchemy
import dash_bootstrap_components as dbc
from utils.config import DB_URL
from dashboard.src.components.header import Header

db = SQLAlchemy()

def create_app():
    # Create the Flask server
    server = Flask(__name__)

    # Configure the SQLite database URI
    server.config['SQLALCHEMY_DATABASE_URI'] = DB_URL
    server.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    # Initialize the database
    db.init_app(server)

    app = Dash(
        __name__,
        server=server,
        meta_tags=[{'name': 'viewport', 'content': 'width=device-width'}],
        external_stylesheets=[dbc.icons.FONT_AWESOME, dbc.themes.LITERA]
    )

    app.title = "Music Dashboard"

    # Suppress callback exceptions
    app.config.suppress_callback_exceptions = True

    # Describe the layout/ UI of the app
    app.layout = html.Div(
        [dcc.Location(id="url", refresh=False), Header(app)]
    )

    # Import and register page layouts and callbacks
    from dashboard.src.pages import (
        listening,
        # live
    )

    # Create page layouts
    @app.callback(Output("page-content", "children"), [Input("url", "pathname")])
    def display_page(pathname):
        if pathname == "/listening":
            return listening.create_layout(app)
        elif pathname == "/live":
            print("Loading Live Page Layout")
            # return live.create_layout(app)
        else:
            return html.Div([
                html.H2("Welcome to the Music Dashboard!"),
                html.P("Use the navigation bar to explore different sections.")
            ], style={'padding': '20px'})
        
    # Set up callbacks
    listening.setup_callbacks(app)
    # live.setup_callbacks(app)

    return app
