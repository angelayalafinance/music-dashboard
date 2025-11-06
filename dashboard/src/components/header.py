from dash import html, dcc
import dash_bootstrap_components as dbc

def Header(app):
    return html.Div([
        dbc.Navbar(
            dbc.Container(
                [
                    dbc.NavbarToggler(id="navbar-toggler"),
                    dbc.Collapse(
                        dbc.Nav(
                            [
                                dbc.NavItem(dbc.NavLink("Home", href='home')),
                                dbc.NavItem(dbc.NavLink("Listening", href='listening')),
                                dbc.NavItem(dbc.NavLink("Live", href='live')),
                            ],
                            className="ms-auto",
                            navbar=True,
                        ),
                        id="navbar-collapse",
                        navbar=True,
                    ),
                ],
                fluid=True,
            ),
            color="white",
            dark=False,
            fixed="top",
            className="navbar",
        ),
        html.Div(id='page-content', className="page-content")
    ])