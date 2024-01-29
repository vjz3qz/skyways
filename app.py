from datetime import timedelta

import pandas as pd
from pymavlink import mavutil


# *****************************************************************

# GITHUB LINK: https://github.com/vjz3qz/skyways
# github repo includes this code, the BIN file, and the CSV file

# *****************************************************************


log_file_path = './2024_swe_internship_vibe_analysis_assignment.BIN'


# Function to convert the dataflash log to a DataFrame
def convert_log_to_df(log_file_path, message_type):
    # Create a MAVLink connection
    mavlink_connection = mavutil.mavlink_connection(log_file_path)

    # Store data in a list
    data = []

    # Read all messages of the specified type
    while True:
        msg = mavlink_connection.recv_match(type=message_type, blocking=False)
        if msg is None:
            break
        if msg.get_type() == "BAD_DATA":
            if mavutil.all_printable(msg.data):
                continue
        data.append(msg.to_dict())

    # Convert to DataFrame
    return pd.DataFrame(data)


# The message type for vibration data is typically 'VIBE' in ArduPilot logs
vibration_data = convert_log_to_df(log_file_path, 'VIBE')


# print(vibration_data)  # Display the first few rows to understand the structure


def filter_high_vibration(vibration_data, threshold):
    """
    Filters the vibration data to find instances where vibration exceeds the threshold.

    :param vibration_data: DataFrame containing the vibration data.
    :param threshold: The vibration threshold in m/s².
    :return: DataFrame with high vibration instances.
    """
    # Filter the data where any of the vibration axes exceeds the threshold
    high_vibration_data = vibration_data[(vibration_data['VibeX'] > threshold) |
                                   (vibration_data['VibeY'] > threshold) |
                                   (vibration_data['VibeZ'] > threshold)].copy()
    # # Format the timestamp and select required columns
    high_vibration_data['timestamp'] = (high_vibration_data['TimeUS']
                                        .apply(lambda x: str(timedelta(microseconds=x))[:-3]))
    high_vibration_data['vibe_x'] = high_vibration_data['VibeX']
    high_vibration_data['vibe_y'] = high_vibration_data['VibeY']
    high_vibration_data['vibe_z'] = high_vibration_data['VibeZ']
    high_vibration_data = high_vibration_data[['timestamp', 'vibe_x', 'vibe_y', 'vibe_z']]

    return high_vibration_data


high_vibration_instances = filter_high_vibration(vibration_data, 35)
high_vibration_instances.to_csv('high_vibration_instances.csv', index=False)
