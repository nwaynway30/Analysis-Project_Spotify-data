
from google.cloud import bigquery
from google.auth import load_credentials_from_file
from google.cloud.bigquery import Client
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

#load BigQuery credentials and data
credentials, project_id = load_credentials_from_file('/Users/thyneminhtetaungaung/Music Project All file /service_account.json')

client = Client(
    project=project_id,
    credentials=credentials
)

#load data function
def load_data(table):
    query = f"SELECT * FROM `da26-python.music_data.{table}`"
    load_job = client.query(query)
    return load_job.to_dataframe()

#Data Processing Functions
def load_and_merge_data():
    #Load all dataframes
    audio_features = load_data("audio_features")
    chart_positions = load_data("chart_positions")
    artists = load_data("artists")
    tracks = load_data("tracks")
    tracks_artists = load_data("tracks_artists_mapping")

    
    #merge songs data with audio features and chart positions
    songs = (
    tracks
    .merge(audio_features, on='track_id', how='left')
    .merge(chart_positions, on='track_id', how='left')
    .merge(
        tracks_artists,
        on='track_id',
        how='left',
        suffixes=('', '_artist')  #Adding a suffix for columns from tracks_artists
    )
)
    
    #convert release_date to datetime
    def parse_date(date_str):
        try:
            return pd.to_datetime(date_str)
        except:
            try:
                if len(str(date_str)) >= 4:
                    year = int(str(date_str)[:4])
                    return pd.to_datetime(f"{year}-01-01")
            except:
                return pd.NaT

    songs['release_date'] = songs['release_date'].apply(parse_date)
    songs['release_year'] = songs['release_date'].dt.year
    
    # convert chart_week to datetime
    songs['chart_week'] = pd.to_datetime(songs['chart_week'])
    songs['chart_year'] = songs['chart_week'].dt.year
    
    
    #add 0s to missing values
    songs['list_position'] = songs['list_position'].fillna(0)
    
    #build a dataframe of artists
    singers = (
        artists
        .merge(tracks_artists, on='artist_id', how='left')
        .groupby('artist_id')
        .agg({
            'name': 'first',
            'popularity': 'first',
            'followers': 'first',
            'track_id': 'count'
        })
        .rename(columns={'track_id': 'total_tracks'})
        .reset_index()
    )
    
    return singers, songs

singers, songs = load_and_merge_data()

chart_position_1_songs = songs[songs['list_position'] == 1]
#Merge chart_position_analysis with tracks_artists to get artist_id
chart_list1_with_artists = chart_position_1_songs.merge(
    singers, on='artist_id', how='left'
).rename(columns={'name_x':'song_name','name_y':'artist_name'})


#Merge chart_position_analysis with tracks_artists to get artist_id
st.header ("Chart-Topping Hits: #1 Tracks and Artists")
year = st.selectbox("Select Year",[x for x in range(2000,2025)])
chart_list1_with_artists = chart_position_1_songs.merge(
    singers, on='artist_id', how='left'
).rename(columns={'name_x':'song_name','name_y':'artist_name'})
active_year = chart_list1_with_artists.loc[chart_list1_with_artists.chart_year == year]
#Group by track and chart year, then count occurrences
chart_position_with_art_analysis = (
    active_year
    .groupby(['song_name','artist_name','chart_year'], as_index=False)
    .agg(total_weeks_at_1=('list_position', 'count'))
)

chart_position_with_art_analysis = (
    chart_position_with_art_analysis
    .groupby(['song_name', 'chart_year','total_weeks_at_1'], as_index=False)
    .agg(
        artist_name=('artist_name', lambda x: ', '.join(x.unique())),
        
    )

)
#Sort by chart year and total weeks at position 1 for better readability
chart_position_with_art_analysis = chart_position_with_art_analysis.sort_values(
    by=['chart_year', 'total_weeks_at_1'], ascending=[False, False]
)
st.dataframe(chart_position_with_art_analysis)

