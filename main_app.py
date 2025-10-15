import streamlit as st
from streamlit_folium import st_folium
import folium
from folium import plugins

import sqlite3
import pathlib
import os
import pandas as pd
import math
import datetime
from io import BytesIO
import random
import time

import plotly.express as px
import plotly.graph_objects as go

from datetime import datetime, timezone

def create_map_with_track_and_MOS(geo_df, add_MOS: bool = False) -> folium.Map:
    """
    Generates a folium.Map from geopandas.GeoDataFrame
    :param geo_df: geopandas.GeoDataFrame with lat/lon data
    :return: folium.Map
    """

    map = folium.Map(location=[geo_df["latitude"].mean(), geo_df["longitude"].mean()], zoom_start=16, height='90%',
                     prefer_canvas=True)
    plugins.PolyLineOffset(geo_df[["latitude", "longitude"]], color="blue", weight=3, opacity=0.8).add_to(map)

    # Add Data and Style Map

    # MarkerCluster feature in Folium to aggregate nearby markers together when the map is zoomed out.

    # for loop below configures a popup, styles the Plaque marker, and adds the feature to the MarkerCluster layer.

    # Create a marker for each plaque location. Format popup

    for index, row in geo_df.iterrows():

        html = f"""<strong>Time:</strong> {row['timestamp_utc']}<br>

            <br>

            <strong>Location:</strong> ({row['latitude'], row['longitude']})<br>

            <br>

            <strong>Speed:</strong> {row['speed']}<br>

            <br>

            <strong>Altitude:</strong> {row['altitude']}<br>

            <br>

            <strong>Altitude:</strong> {row['bearing']}<br>

                """

        iframe = folium.IFrame(html,

                               width=200,

                               height=200)

        popup = folium.Popup(iframe,

                             max_width=400)

        # TODO - replace this with MOS_score

        ###

        if add_MOS and row["MOS_score"] >= 75:
            folium.Circle(location=[row["latitude"], row["longitude"]],

                          radius=6,

                          color="red",

                          fill=True,

                          fill_color="red",

                          popup=popup).add_to(map)

    return map

# Convert Unix timestamp (in ms) to UTC datetime string (with ms precision)

def convert_to_utc(ts_ms):
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + ' UTC'  # Trim to milliseconds



def save_uploadedfile(uploaded_file, path: str):
    with open(os.path.join(path, uploaded_file.name), "wb") as f:
        f.write(uploaded_file.getbuffer())
    return st.success("Saved file: {} to {}".format(uploaded_file.name, path))

def retrieve_feedback_data(db_file, table_name = 'feedbackEventData'):
    """Retrieve survey feedback from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {1} AND ' \
                    f'sensorId = {800}')
    
    # Fetch all rows from the table
    data = cursor.fetchall()

    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(data, columns=["original_idx", "runID", "feelingDefinitionId", "causeDefinitionId",
                                     "feelingDescription", "causeDescription", "intensity", "note",
                                     "timestamp",  
                                     "latitude", "longitude", "altitude", "mslAltitude", "bearing", "speed", 
                                     "locationAccuracy", "bearingAccuracy", "speedAccuracy",
                                     "mslAltitudeAccuracy", "verticalAccuracy",
                                     "platformID", "sensorID"
                                     ])
    
    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df

def retrieve_location_data(db_file, table_name = 'locationEventData'):
    """Retrieve location from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {1} AND ' \
                    f'sensorId = {900}')
    
    # Fetch all rows from the table
    data = cursor.fetchall()
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(data, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", 
                                     "latitude", "longitude", "altitude", "mslAltitude", "bearing", "speed", 
                                     "locationAccuracy", "bearingAccuracy", "speedAccuracy",
                                     "mslAltitudeAccuracy", "verticalAccuracy"])
    
    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df

# Function to print the schema of the database
def print_schema(db_file):
    """Retrieve all tables in .db file and print their schema"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Get the schema of the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    
    # Fetch all the table names
    tables = cursor.fetchall()
    
    # Print schema for each table
    for table in tables:
        print(f"Schema for table: {table[0]}")
        cursor.execute(f"PRAGMA table_info({table[0]});")
        
        # Fetch and print column details for the current table
        columns = cursor.fetchall()
        for column in columns:
            print(f"Column Name: {column[1]}, Type: {column[2]}, Not Null: {column[3]}, Default Value: {column[4]}")
        print("-" * 40)
    
    # Close the connection
    conn.close()

    return tables


st.header("Select an eDiary2.0 database file (.db extension):")

path = pathlib.Path(__file__).parent.resolve()
st.markdown("---")

######## File uploader ########

uploaded_db_file = st.file_uploader("Drag and drop your .sqlite file here...", type=["db"])
st.info("Upload .sqlite files")

# the main, branching part of the application
if uploaded_db_file is not None:
    try:
        save_uploadedfile(uploaded_file=uploaded_db_file, path=path)
        st.write(f"Saving file {uploaded_db_file} to: {path}")
        
        st.sidebar.title("Select Participant and display information")

        location_data = retrieve_location_data(os.path.join(path, uploaded_db_file.name))

        participant_id_selection = st.sidebar.selectbox(
                    "Select participant ID:",
                    set(list(location_data["runID"].unique()))
                    #("Email", "Home phone", "Mobile phone"),
                )

        st.write("You selected:", participant_id_selection)

        ######### Optional eDiary Tables Display #########
        
        #preped_data = st.sidebar.checkbox("Show/Hide preprocessed data:")
        location_data_display = st.sidebar.checkbox("Display Locations:")
        feedback_data_display = st.sidebar.checkbox("Show/Hide Information contained in Feedback Table:")

        
        #st.write(location_data[location_data["runID"] == participant_id_selection])


        feedback_data = retrieve_feedback_data(os.path.join(path, uploaded_db_file.name))

        schema = print_schema(os.path.join(path, uploaded_db_file.name))

        if feedback_data_display:
            st.write("Feedback data: ", feedback_data)
        
        if location_data_display:
            st.write("Raw locations table Data: ", location_data[location_data["runID"] == participant_id_selection])

            map = create_map_with_track_and_MOS(location_data[location_data["runID"] == participant_id_selection])
            st_map = st_folium(map, width=1000)

    except:
        pass