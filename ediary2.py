
import sqlite3
import os
import pandas as pd

### General .db import and schema

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

# Function to retrieve all data from a specified table
def retrieve_all_data(db_file, table_name):
    """Retrieve all the data from a given table in the .db file --> returns a pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    cursor.execute(f"SELECT * FROM {table_name}")
    
    # Fetch all rows from the table
    rows = cursor.fetchall()

    # Close the connection
    conn.close()

    return pd.DataFrame(rows)


### Smartphone Data

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

    return df

### Feedback / Survey Data

def retrieve_feedback_data(db_file, table_name = 'feedbackEventData'):
    """Retrieve survey feedback from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {1} AND ' \
                    f'sensorId = {800}')
    
    # Fetch all rows from the table
    data = cursor.fetchall()

    conn.close()

    df = pd.DataFrame(data, columns=["original_idx", "runID", "feelingDefinitionId", "causeDefinitionId",
                                     "feelingDescription", "causeDescription", "intensity", "note",
                                     "timestamp",  
                                     "latitude", "longitude", "altitude", "mslAltitude", "bearing", "speed", 
                                     "locationAccuracy", "bearingAccuracy", "speedAccuracy",
                                     "mslAltitudeAccuracy", "verticalAccuracy",
                                     "platformID", "sensorID"
                                     ])

    return df

### Zephyr BioHarness chest strap data 

def retrieve_all_bwa_data(db_file, table_name = 'doubleEventData'):
    """Retrieve Breathing Wave Amplitude (BWA) from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {8}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Close the connection
    conn.close()

    data = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "BWA"])

    return data 

def retrieve_all_ecg_amplitude_data(db_file, table_name = 'doubleEventData'):
    """Retrieve ECG Amplitude from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {9}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()

    # Close the connection
    conn.close()

    data = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "ecg_ampl"])

    return data 

def retrieve_all_ecg_noise_data(db_file, table_name = 'doubleEventData'):
    """Retrieve ECG Noise from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {10}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Close the connection
    conn.close()

    data = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "ecg_noise"])

    return data 

def retrieve_all_resp_rate_data(db_file, table_name = 'doubleEventData'):
    """Retrieve respiration rate from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {2}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Close the connection
    conn.close()

    data = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "resp_rate"])

    return data 


def retrieve_all_heart_rate_data(db_file, table_name = 'longEventData'):
    """Retrieve respiration rate from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {1}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()

    
    # Close the connection
    conn.close()

    data = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "heart_rate"])

    return data


def retrieve_all_posture_data(db_file, table_name = 'longEventData'):
    """Retrieve posture from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {4}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()

    
    # Close the connection
    conn.close()

    data = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "posture"])

    return data 

def retrieve_all_color_data(db_file, table_name = 'longEventData'):
    """Retrieve BioHarness color (red, orange, green) from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {6}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()

    
    # Close the connection
    conn.close()

    data = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "color"])

    return data 

def retrieve_all_vector_mag_units_data(db_file, table_name = 'doubleEventData'):
    """Retrieve vector magnitude units from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {12}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()

    
    # Close the connection
    conn.close()

    data = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "vector_mag_u"])

    return data 

def retrieve_all_worn_status_data(db_file, table_name = 'longEventData'):
    """Retrieve status if device was worn from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {13}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()

    # Close the connection
    conn.close()

    data = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "worn_status"])

    return data 

def retrieve_all_peak_acc_data(db_file, table_name = 'doubleEventData'):
    """Retrieve peaks of acceleration from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {5}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Close the connection
    conn.close()

    data = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "peak_acc"])

    return data 

def retrieve_all_acc_min_data(db_file, table_name = 'doubleEventData'):
    """Retrieve mins of acceleration from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {101}')
    
    # Fetch all rows from the table
    rows_acc_x_min = cursor.fetchall()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {102}')

    rows_acc_y_min = cursor.fetchall()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {103}')

    rows_acc_z_min = cursor.fetchall()
    
    conn.close()

    data_x = pd.DataFrame(rows_acc_x_min, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "acc_min_x"])
    data_y = pd.DataFrame(rows_acc_y_min, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "acc_min_y"])
    data_z = pd.DataFrame(rows_acc_z_min, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "acc_min_z"])

    data_acc_min_xy = pd.merge(data_x, data_y[["timestamp", "acc_min_y"]])
    data_acc_min_xyz = pd.merge(data_acc_min_xy, data_z[["timestamp", "acc_min_z"]])

    return data_acc_min_xyz

def retrieve_all_acc_peak_data(db_file, table_name = 'doubleEventData'):
    """Retrieve respiration rate from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {111}')
    
    # Fetch all rows from the table
    rows_acc_x_min = cursor.fetchall()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {112}')

    rows_acc_y_min = cursor.fetchall()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {113}')

    rows_acc_z_min = cursor.fetchall()
    
    # Close the connection
    conn.close()

    data_x = pd.DataFrame(rows_acc_x_min, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "acc_peak_x"])
    data_y = pd.DataFrame(rows_acc_y_min, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "acc_peak_y"])
    data_z = pd.DataFrame(rows_acc_z_min, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "acc_peak_z"])

    data_acc_peak_xy = pd.merge(data_x, data_y[["timestamp", "acc_peak_y"]])
    data_acc_peak_xyz = pd.merge(data_acc_peak_xy, data_z[["timestamp", "acc_peak_z"]])

    return data_acc_peak_xyz

def retrieve_all_battery_data(db_file, table_name = 'longEventData'):
    """Retrieve battery level (0-100) from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {999}')
    
    # Fetch all rows from the table
    data_battery = cursor.fetchall()
    
    # Close the connection
    conn.close()

    df_battery = pd.DataFrame(data_battery, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "battery"])

    return df_battery