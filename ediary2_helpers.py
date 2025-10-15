import sqlite3
import pandas as pd
import os

from datetime import datetime, timezone

# Convert Unix timestamp (in ms) to UTC datetime string (with ms precision)

def convert_to_utc(ts_ms):
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + ' UTC'  # Trim to milliseconds

def retrieve_all_bwa_data(db_file, table_name = 'doubleEventData'):
    """Retrieve Breathing wave amplitude (BWA) from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {8}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "BWA"])

    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df 

def retrieve_all_ecg_amplitude_data(db_file, table_name = 'doubleEventData'):
    """Retrieve ECG Amplitude from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {9}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "ecg_ampl"])

    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df 

def retrieve_all_ecg_noise_data(db_file, table_name = 'doubleEventData'):
    """Retrieve ECG Noise from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {10}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "ecg_noise"])

    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df 

def retrieve_all_resp_rate_data(db_file, table_name = 'doubleEventData'):
    """Retrieve respiration rate from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {2}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "resp_rate"])

    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df 


def retrieve_all_heart_rate_data(db_file, table_name = 'longEventData'):
    """Retrieve respiration rate from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {1}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "heart_rate"])
    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df 


def retrieve_all_posture_data(db_file, table_name = 'longEventData'):
    """Retrieve respiration rate from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {4}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "posture"])

    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df 

def retrieve_all_color_data(db_file, table_name = 'longEventData'):
    """Retrieve color (red, orange, green) from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {6}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "color"])
    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df 

def retrieve_all_vector_mag_units_data(db_file, table_name = 'doubleEventData'):
    """Retrieve respiration rate from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {12}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "vector_mag_u"])
    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df 

def retrieve_all_worn_status_data(db_file, table_name = 'longEventData'):
    """Retrieve respiration rate from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {13}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "worn_status"])
    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df 

def retrieve_all_peak_acc_data(db_file, table_name = 'doubleEventData'):
    """Retrieve respiration rate from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {5}')
    
    # Fetch all rows from the table
    rows = cursor.fetchall()
    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(rows, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "peak_acc"])
    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df 

def retrieve_all_acc_min_data(db_file, table_name = 'doubleEventData'):
    """Retrieve respiration rate from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

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
    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    data_x = pd.DataFrame(rows_acc_x_min, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "acc_min_x"])
    data_y = pd.DataFrame(rows_acc_y_min, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "acc_min_y"])
    data_z = pd.DataFrame(rows_acc_z_min, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "acc_min_z"])
    data_x['timestamp_x_utc'] = data_x['timestamp'].apply(convert_to_utc)

    data_y['timestamp_y_utc'] = data_y['timestamp'].apply(convert_to_utc)

    data_z['timestamp_z_utc'] = data_z['timestamp'].apply(convert_to_utc)

    data_acc_min_xy = pd.merge(data_x, data_y[["timestamp", "timestamp_y_utc", "acc_min_y"]])
    data_acc_min_xyz = pd.merge(data_acc_min_xy, data_z[["timestamp", "timestamp_z_utc", "acc_min_z"]])

    return data_acc_min_xyz

def retrieve_all_acc_peak_data(db_file, table_name = 'doubleEventData'):
    """Retrieve respiration rate from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

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
    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    data_x = pd.DataFrame(rows_acc_x_min, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "acc_peak_x"])
    data_x['timestamp_x_utc'] = data_x['timestamp'].apply(convert_to_utc)
    data_y = pd.DataFrame(rows_acc_y_min, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "acc_peak_y"])
    data_y['timestamp_y_utc'] = data_y['timestamp'].apply(convert_to_utc)
    data_z = pd.DataFrame(rows_acc_z_min, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "acc_peak_z"])
    data_z['timestamp_z_utc'] = data_z['timestamp'].apply(convert_to_utc)

    data_acc_peak_xy = pd.merge(data_x, data_y[["timestamp", "timestamp_y_utc", "acc_peak_y"]])
    data_acc_peak_xyz = pd.merge(data_acc_peak_xy, data_z[["timestamp", "timestamp_z_utc", "acc_peak_z"]])

    return data_acc_peak_xyz

def retrieve_all_battery_data(db_file, table_name = 'longEventData'):
    """Retrieve battery from .db file and returns pd.DataFrame"""

    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    # Retrieve all data from the specified table
    #cursor.execute(f"SELECT * FROM {table_name}")

    cursor.execute(f'SELECT * FROM {table_name} ' \
                    f'WHERE platformId = {2} AND ' \
                    f'sensorId = {999}')
    
    # Fetch all rows from the table
    data_battery = cursor.fetchall()

    
    # Print the data
    #print(f"\nData from table: {table_name}")
    #for row in rows:
    #    print(row)
    
    # Close the connection
    conn.close()

    df = pd.DataFrame(data_battery, columns=["original_idx", "runID", "timestamp", "platformID", "sensorID", "battery"])

    df['timestamp_utc'] = df['timestamp'].apply(convert_to_utc)

    return df