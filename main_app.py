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

import ediary2_helpers as ediary2

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


def retrieve_run_data(db_file, table_name = 'run'):
    """Retrieve participant run information from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ')# \
                    #f'WHERE platformId = {1} AND ' \
                    #f'sensorId = {900}')
    
    # Fetch all rows from the table
    data = cursor.fetchall()
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(data, columns=["id", "start", "end", "study", 
                                     "name", "birthYear", "gender", "email", "note", "configurationID", 
                                     "appVersion", "androidSdk", "device"])
    
    df['timestamp_start_utc'] = df['start'].apply(convert_to_utc)
    df['timestamp_end_utc'] = df['end'].apply(convert_to_utc)

    return df[["id", "name", "study", "timestamp_start_utc", "timestamp_end_utc", 
                "birthYear", "gender", "email", "note", "start", "end", 
                "configurationID", "appVersion", "androidSdk", "device"]]

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

        run_data = retrieve_run_data(os.path.join(path, uploaded_db_file.name))
        run_data["timestamp_start_utc"] = pd.to_datetime(run_data["timestamp_start_utc"])
        run_data["timestamp_end_utc"] = pd.to_datetime(run_data["timestamp_end_utc"])
        st.write(run_data)

        location_data = retrieve_location_data(os.path.join(path, uploaded_db_file.name))
        location_data["timestamp_utc"] = pd.to_datetime(location_data["timestamp_utc"])

        participant_id_selection = st.sidebar.selectbox(
                    "Select participant ID:",
                    set(list(location_data["runID"].unique()))
                    #("Email", "Home phone", "Mobile phone"),
                )

        st.write("You selected:", participant_id_selection)

        st.header("Participant Run information")
        participant_run_data = run_data[run_data["id"] == participant_id_selection].reset_index()
        st.write(participant_run_data)

        run_duration = participant_run_data["timestamp_end_utc"][0] - participant_run_data["timestamp_start_utc"][0]
        st.write("Run duration: ", run_duration)
        st.sidebar.write("Run duration in seconds: ", run_duration.seconds)
        

        ######### Optional eDiary Tables Display #########
        
        #preped_data = st.sidebar.checkbox("Show/Hide preprocessed data:")
        location_data_display = st.sidebar.checkbox("Display Locations:")
        feedback_data_display = st.sidebar.checkbox("Display Feedback:")
        sensor_data_display = st.sidebar.checkbox("Display Sensor Data:")

        
        breathing_wave_amplitude = ediary2.retrieve_all_bwa_data(os.path.join(path, uploaded_db_file.name), table_name='doubleEventData')
        breathing_wave_amplitude["timestamp_utc"] = pd.to_datetime(breathing_wave_amplitude["timestamp_utc"])

        posture = ediary2.retrieve_all_posture_data(os.path.join(path, uploaded_db_file.name), table_name = 'longEventData')
        posture["timestamp_utc"] = pd.to_datetime(posture["timestamp_utc"])

        heart_rate = ediary2.retrieve_all_heart_rate_data(os.path.join(path, uploaded_db_file.name), table_name="longEventData")
        heart_rate["timestamp_utc"] = pd.to_datetime(heart_rate["timestamp_utc"])

        resp_rate = ediary2.retrieve_all_resp_rate_data(os.path.join(path, uploaded_db_file.name), table_name="doubleEventData")
        resp_rate["timestamp_utc"] = pd.to_datetime(resp_rate["timestamp_utc"])

        ecg_amplitude = ediary2.retrieve_all_ecg_amplitude_data(os.path.join(path, uploaded_db_file.name), table_name='doubleEventData')
        ecg_amplitude["timestamp_utc"] = pd.to_datetime(ecg_amplitude["timestamp_utc"])

        ecg_noise = ediary2.retrieve_all_ecg_noise_data(os.path.join(path, uploaded_db_file.name), table_name='doubleEventData')
        ecg_noise["timestamp_utc"] = pd.to_datetime(ecg_noise["timestamp_utc"])

        #st.write(location_data[location_data["runID"] == participant_id_selection])


        feedback_data = retrieve_feedback_data(os.path.join(path, uploaded_db_file.name))

        schema = print_schema(os.path.join(path, uploaded_db_file.name))

        if feedback_data_display:
            st.write("Feedback data: ", feedback_data[feedback_data["runID"] == participant_id_selection])
        
        if location_data_display:
            st.write("Raw locations table Data: ", location_data[location_data["runID"] == participant_id_selection])

            map = create_map_with_track_and_MOS(location_data[location_data["runID"] == participant_id_selection])
            st_map = st_folium(map, width=1000)

            #speed_fig = px.line(location_data[location_data["runID"] == participant_id_selection], 
            #        x = "timestamp_utc", y = "altitude", title = "Speed over time")
            
            #st.ploty_chart(speed_fig)

        if sensor_data_display:

            worn_status = ediary2.retrieve_all_worn_status_data(os.path.join(path, uploaded_db_file.name), table_name="longEventData")
            #st.write(worn_status)

            color_status = ediary2.retrieve_all_color_data(os.path.join(path, uploaded_db_file.name), table_name="longEventData")
            #st.write(color_status)

            battery_status = ediary2.retrieve_all_battery_data(os.path.join(path, uploaded_db_file.name), table_name="longEventData")
            #st.write(battery_status)

            vector_mag = ediary2.retrieve_all_vector_mag_units_data(os.path.join(path, uploaded_db_file.name), table_name="doubleEventData")
            #st.write(vector_mag)

            acc_min = ediary2.retrieve_all_acc_min_data(os.path.join(path, uploaded_db_file.name), table_name="doubleEventData")
            #st.write(acc_min)

            acc_peak = ediary2.retrieve_all_acc_peak_data(os.path.join(path, uploaded_db_file.name), table_name="doubleEventData")
            #st.write(acc_peak)


            ### BREATHING WAVE AMPLITUDE
            st.write("Breathing Wave Amplitude:")
            st.write("Nunber of recordings for BWA: ", len(breathing_wave_amplitude[breathing_wave_amplitude["runID"] == participant_id_selection]))
            st.write("Sampling Frequency (# of recordings / total seconds) is: ", len(breathing_wave_amplitude[breathing_wave_amplitude["runID"] == participant_id_selection]) / run_duration.seconds)
            # plot data
            bwa_fig = px.line(breathing_wave_amplitude[breathing_wave_amplitude["runID"] == participant_id_selection], 
                    x = "timestamp_utc", y = "BWA", title = "Breathing wave amplitude")
            st.plotly_chart(bwa_fig)

            ### POSTURE
            st.write("Posture:")
            st.write("Nunber of recordings for posture: ", len([posture[posture["runID"] == participant_id_selection]]))
            st.write("Sampling Frequency (# of recordings / total seconds) is: ", len(posture[posture["runID"] == participant_id_selection]) / run_duration.seconds)
            posture_fig = px.line(posture[posture["runID"] == participant_id_selection],
                                   x = "timestamp_utc", y = "posture", title = "Posture")
            st.plotly_chart(posture_fig)

            ### HEART RATE
            st.write("Heart Rate:")
            st.write("Nunber of recordings for heart rate: ", len(heart_rate[heart_rate["runID"] == participant_id_selection]))
            st.write("Sampling Frequency (# of recordings / total seconds) is: ", len(heart_rate[heart_rate["runID"] == participant_id_selection]) / run_duration.seconds)
            heart_rate_fig = px.line(heart_rate[heart_rate["runID"] == participant_id_selection],
                                      x = "timestamp_utc", y = "heart_rate", title = "Heart Rate")
            st.plotly_chart(heart_rate_fig)

            ### RESPIRATION RATE
            st.write("Respiration Rate:")
            st.write("Nunber of recordings for respiration rate: ", len(resp_rate[resp_rate["runID"] == participant_id_selection]))
            st.write("Sampling Frequency (# of recordings / total seconds) is: ", len(resp_rate[resp_rate["runID"] == participant_id_selection]) / run_duration.seconds)
            resp_rate_fig = px.line(resp_rate[resp_rate["runID"] == participant_id_selection], 
                    x = "timestamp_utc", y = "resp_rate", title = "Respiration Rate")
            st.plotly_chart(resp_rate_fig)

            ### ECG AMPLITUDE
            st.write("ECG Amplitude:")
            st.write("Nunber of recordings for ECG amplitude: ", len(ecg_amplitude[ecg_amplitude["runID"] == participant_id_selection]))
            st.write("Sampling Frequency (# of recordings / total seconds) is: ", len(ecg_amplitude[ecg_amplitude["runID"] == participant_id_selection]) / run_duration.seconds)
            ecg_amplitude_fig = px.line(ecg_amplitude[ecg_amplitude["runID"] == participant_id_selection],
                     x = "timestamp_utc", y = "ecg_ampl", title = "ECG Amplitude")
            st.plotly_chart(ecg_amplitude_fig)

            ### ECG NOISE
            st.write("ECG Noise:")
            st.write("Nunber of recordings for ECG noise: ", len(ecg_noise[ecg_noise["runID"] == participant_id_selection]))
            st.write("Sampling Frequency (# of recordings / total seconds) is: ", len(ecg_noise[ecg_noise["runID"] == participant_id_selection]) / run_duration.seconds)
            ecg_noise_fig = px.line(ecg_noise[ecg_noise["runID"] == participant_id_selection],
                     x = "timestamp_utc", y = "ecg_noise", title = "ECG Noise")
            st.plotly_chart(ecg_noise_fig)


    except:
        pass