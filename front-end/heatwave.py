import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas as pd
import pathlib
import os
import psycopg2
import pandas.io.sql as sqlio
from dash.dependencies import Input, Output, State
import json

f = open('creds.json', 'rt')
j = json.load(f)

psql_user = j['username']
psql_pw = j['password']

# Connect to your postgres DB
conn = psycopg2.connect(host='10.0.0.14', user=psql_user, password=psql_pw, dbname='heatwave')

# Set default table
d = {'col1': [1, 2], 'col2': [3, 4]}
df = pd.DataFrame(data=d)

BINS = [
    "0-2",
    "2.1-4",
    "4.1-6",
    "6.1-8",
    "8.1-10",
    "10.1-12",
    "12.1-14",
    "14.1-16",
    "16.1-18",
    "18.1-20",
    "20.1-22",
    "22.1-24",
    "24.1-26",
    "26.1-28",
    "28.1-30",
    ">30",
]

DEFAULT_COLORSCALE = [
    "#f2fffb",
    "#bbffeb",
    "#98ffe0",
    "#79ffd6",
    "#6df0c8",
    "#69e7c0",
    "#59dab2",
    "#45d0a5",
    "#31c194",
    "#2bb489",
    "#25a27b",
    "#1e906d",
    "#188463",
    "#157658",
    "#11684d",
    "#10523e",
]

DEFAULT_OPACITY = 0.8

mapbox_access_token = "pk.eyJ1IjoicGxvdGx5bWFwYm94IiwiYSI6ImNrOWJqb2F4djBnMjEzbG50amg0dnJieG4ifQ.Zme1-Uzoi75IaFbieBDl3A"
mapbox_style = "mapbox://styles/plotlymapbox/cjvprkf3t1kns1cqjxuxmwixz"

#external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

#app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app = dash.Dash(
    __name__,
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}
    ],
)

server = app.server

app.layout = html.Div(
    id="root",
    children=[
        html.Div(
            id="header",
            children=[
                html.Img(id="logo", src=app.get_asset_url("dash-logo.png")),
                html.H4(children="Heatwave"),
                html.P(
                    id="description",
                    children="By Romain Fardel - Insight Data Engineering project",
                ),
            ],
        ),
        html.Div(
            id="app-container",
            children=[
                html.Div(
                    id="left-column",
                    children=[
                        html.Div(
                            id="slider-container",
                            children=[
                                html.P(
                                    id="slider-text",
                                    children="Drag the slider to change the year:",
                                ),
                                dcc.Slider(
                                    id="years-slider",
                                    min=1968,
                                    max=1988,
                                    value=1973,
                                    marks={
                                        str(year): {
                                            "label": str(year),
                                            "style": {"color": "#7fafdf"},
                                        }
                                        for year in range(1968, 1988)
                                    },
                                ),
                            ],
                        ),
                        html.Div(
                            id="graph-container",
                            children=[
                                html.P(id="chart-selector", children="Select table to view:"),
                                dcc.Dropdown(
                                    options=[
                                        # {
                                        #     "label": "Weather data",
                                        #     "value": "select_weather",
                                        # },
                                        # {
                                        #     "label": "Weather by county",
                                        #     "value": "select_wbc",
                                        # },
                                        # {
                                        #     "label": "Mortality data",
                                        #     "value": "select_mortality",
                                        # },
                                        {
                                            "label": "Temperature and mortality",
                                            "value": "select_T",
                                        },
                                        {
                                            "label": "Weather stations",
                                            "value": "select_stacoun",
                                        },
                                    ],
                                    value="select_stacoun",
                                    id="chart-dropdown",
                                ),
                                html.Div(id='data-table', children='None'),
                            ],
                        ),
                    ],
                ),

            ],
        ),
    ],
)


# App callback

@app.callback(
    Output("data-table", "children"),
    [
        Input("chart-dropdown", "value"),
        Input("years-slider", "value"),
    ],
)
def generate_table(chart_dropdown, year_slider):
    max_rows = 12;
    year_filter = " WHERE date_part('year', agg_date) = " + str(year_slider)

    # Open a cursor to perform database operations
    cur = conn.cursor()
    if chart_dropdown == "select_stacoun":
        sql = "select * from stacoun limit 100;"

    if chart_dropdown == "select_T":
        sql = "select * from combined" + year_filter + \
              " order by state, county_name, agg_date limit 100;"

    if chart_dropdown == "select_wbc":
            sql = "select * from weatherbycounty" + \
                  " order by state, county_name, date limit 100;"
    if chart_dropdown == "select_mortality":
        sql = "select * from mortality" + \
              " WHERE date_part('year', date) = " + str(year_slider) + \
              " order by state, county_fips, date limit 100;"
    if chart_dropdown == "select_weather":
        sql = "select * from weather" + \
              " WHERE date_part('year', date) = " + str(year_slider) + \
              " order by station, date limit 100;"

    dataframe = sqlio.read_sql_query(sql, conn)
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])

# end app callback


if __name__ == '__main__':
    app.run_server(debug=True)
