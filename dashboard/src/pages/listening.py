from dash import html, dcc
from datetime import datetime, timedelta
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from sqlalchemy import func
from database.db import get_db, SessionLocal
from database.models import Artist, TopTrack, TopArtist, ListeningHistory


# Spotify Analytics Page
def create_layout(app):
    return html.Div([
        html.H2("🎧 Spotify Listening Analytics"),
        
        # Filters
        html.Div([
            dcc.Dropdown(
                id='time-range-filter',
                options=[
                    {'label': 'Last 4 Weeks', 'value': 'short_term'},
                    {'label': 'Last 6 Months', 'value': 'medium_term'},
                    {'label': 'All Time', 'value': 'long_term'}
                ],
                value='medium_term',
                style={'width': '200px', 'display': 'inline-block', 'marginRight': '20px'}
            ),
            dcc.DatePickerRange(
                id='date-range-filter',
                start_date=datetime.now() - timedelta(days=30),
                end_date=datetime.now(),
                style={'display': 'inline-block'}
            )
        ], style={'marginBottom': '30px'}),
        
        # Stats Cards
        html.Div(id='stats-cards', style={'marginBottom': '30px'}),
        
        # Charts
        dcc.Tabs([
            dcc.Tab(label='Top Tracks', children=[
                html.Div([
                    dcc.Graph(id='top-tracks-chart'),
                    html.Div(id='top-tracks-table')
                ])
            ]),
            dcc.Tab(label='Top Artists', children=[
                html.Div([
                    dcc.Graph(id='top-artists-chart'),
                    html.Div(id='top-artists-table')
                ])
            ]),
            dcc.Tab(label='Listening History', children=[
                dcc.Graph(id='listening-timeline'),
                dcc.Graph(id='genre-distribution')
            ]),
            dcc.Tab(label='Audio Features', children=[
                dcc.Graph(id='popularity-distribution'),
                dcc.Graph(id='explicit-content')
            ])
        ])
    ])


def setup_callbacks(app):
    stats_card_style = {
        'padding': '20px',
        'border': '1px solid #ddd',
        'borderRadius': '5px',
        'textAlign': 'center',
        'width': '23%'
    }

    # Spotify Analytics Callbacks
    @app.callback(
        [Output('stats-cards', 'children'),
        Output('top-tracks-chart', 'figure'),
        Output('top-artists-chart', 'figure')],
        [Input('time-range-filter', 'value'),
        Input('date-range-filter', 'start_date'),
        Input('date-range-filter', 'end_date')]
    )
    def update_spotify_analytics(time_range, start_date, end_date):
        session = SessionLocal()
        try:
            # Get latest data for the selected time range
            latest_date = session.query(func.max(TopTrack.extracted_date)).scalar()
            
            # Stats Cards
            total_artists = session.query(Artist).count()
            total_tracks = session.query(TopTrack).filter(
                TopTrack.time_range == time_range,
                TopTrack.extracted_date == latest_date
            ).count()
            
            recent_plays = session.query(ListeningHistory).filter(
                ListeningHistory.played_at >= start_date,
                ListeningHistory.played_at <= end_date
            ).count()
            
            top_genre = session.query(
                Artist.genre,
                func.count(Artist.id).label('count')
            ).group_by(Artist.genre).order_by(func.count(Artist.id).desc()).first()
            
            stats_cards = html.Div([
                html.Div([
                    html.H3(f"{total_artists}"),
                    html.P("Total Artists")
                ], className='stats-card', style=stats_card_style),
                html.Div([
                    html.H3(f"{total_tracks}"),
                    html.P("Top Tracks")
                ], className='stats-card', style=stats_card_style),
                html.Div([
                    html.H3(f"{recent_plays}"),
                    html.P("Recent Plays")
                ], className='stats-card', style=stats_card_style),
                html.Div([
                    html.H3(f"{top_genre[0] if top_genre else 'N/A'}"),
                    html.P("Top Genre")
                ], className='stats-card', style=stats_card_style)
            ], style={'display': 'flex', 'justifyContent': 'space-between', 'marginBottom': '20px'})
            
            # Top Tracks Chart
            top_tracks = session.query(
                TopTrack.name,
                TopTrack.popularity,
                Artist.name.label('artist_name'),
                TopTrack.rank
            ).join(Artist).filter(
                TopTrack.time_range == time_range,
                TopTrack.extracted_date == latest_date
            ).order_by(TopTrack.rank).limit(20).all()
            
            tracks_fig = px.bar(
                pd.DataFrame([{
                    'Track': f"{t.name} - {t.artist_name}",
                    'Popularity': t.popularity,
                    'Rank': t.rank
                } for t in top_tracks]),
                x='Popularity',
                y='Track',
                orientation='h',
                title=f'Top Tracks ({time_range.replace("_", " ").title()})',
                color='Rank',
                color_continuous_scale='viridis'
            )
            tracks_fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            
            # Top Artists Chart
            top_artists = session.query(
                Artist.name,
                Artist.popularity,
                Artist.followers,
                TopArtist.rank
            ).join(TopArtist).filter(
                TopArtist.time_range == time_range,
                TopArtist.extracted_date == latest_date
            ).order_by(TopArtist.rank).limit(15).all()
            
            artists_fig = px.scatter(
                pd.DataFrame([{
                    'Artist': a.name,
                    'Popularity': a.popularity,
                    'Followers': a.followers,
                    'Rank': a.rank
                } for a in top_artists]),
                x='Popularity',
                y='Followers',
                size='Popularity',
                color='Rank',
                hover_name='Artist',
                title=f'Top Artists ({time_range.replace("_", " ").title()})',
                log_y=True
            )
            
            return stats_cards, tracks_fig, artists_fig
            
        finally:
            session.close()