import pandas as pd
import numpy as np
import logging
from functools import partial
import os
import re
from openpyxl import load_workbook
import time

MOMM_MONTH_DAYS = np.array([31, 28.24, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])

logger = logging.getLogger(__name__)

def read_data(file_path, excel_file, sheet_name='1. Inputs Setup'):
    df_excel = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
    
    # Extract start and end times from Q7 and Q8 (indices 16, 17)
    start_time = df_excel.iloc[6, 16]  # Q7
    end_time = df_excel.iloc[7, 16]    # Q8
    print(f"starttime - {start_time}, endtime - {end_time}")

    # Convert them to datetime objects if needed
    if pd.notna(start_time):
        start_time = pd.to_datetime(start_time)
    else:
        start_time = pd.to_datetime('1900-01-01')  # Default to earliest possible date if cell is empty

    if pd.notna(end_time):
        end_time = pd.to_datetime(end_time)
    else:
        end_time = pd.to_datetime('2100-01-01')  # Default to latest possible date if cell is empty

    with open(file_path, 'r', encoding='latin1') as file:
        lines = file.readlines()

        start_index = next(i for i, line in enumerate(lines) if 'Date/Time' in line)
        latitude_line = next(i for i, line in enumerate(lines) if 'Latitude' in line)
        longitude_line = next(i for i, line in enumerate(lines) if 'Longitude' in line)
        elevation_line = next(i for i, line in enumerate(lines) if 'Elevation' in line)

        latitude = float(lines[latitude_line].split('=')[1].strip())
        longitude = float(lines[longitude_line].split('=')[1].strip())
        elevation = float(lines[elevation_line].split('=')[1].strip().split()[0])

    print("Reading Data from txt file..!!")
    data = pd.read_csv(file_path, sep='\t', skiprows=start_index, parse_dates=['Date/Time'], encoding='latin1')

    print("Filtering data for the given date range ", start_time, "-", end_time)
    mask = (data['Date/Time'] >= start_time) & (data['Date/Time'] <= end_time) if pd.notna(start_time) and pd.notna(end_time) else True
    filtered_data = data[mask]

    return filtered_data, latitude, longitude, elevation

def calculate_drr(series):
    valid_points = (series != 9999).sum()
    return round((valid_points / len(series)) * 100, 2)
    
    

def calculate_completeness_factor(valid_series, valid_months, time_steps_per_day=144):
    # Calculate the total number of valid data points for each month
    monthly_data_points = valid_series.groupby(valid_months).count()

    # Define the number of days for each month (approximate average)
    days_in_month = {
        1: 31,   
        2: 28.24,
        3: 31,   
        4: 30,   
        5: 31,   
        6: 30,   
        7: 31,   
        8: 31,   
        9: 30,   
        10: 31,  
        11: 30,  
        12: 31   
    }

    # Initialize an array to store the completeness factors for all 12 months
    completeness_factors = np.ones(12)

    # Loop through each month and calculate the completeness factor
    for month, N_i in monthly_data_points.items():
        # Get the number of days in the month
        psi_i = days_in_month.get(month, 30)  # Default to 30 days if month not found

        # Calculate the denominator (n * ψ_i)
        denominator = time_steps_per_day * psi_i

        # Calculate the completeness factor for the month
        completeness_factor = min(N_i / denominator, 1)

        # Store the result in the corresponding index (month - 1 since index is 0-based)
        completeness_factors[month - 1] = completeness_factor

    return completeness_factors

def calculate_momm(valid_series, valid_months):
    # Calculate completeness factor
    cf = calculate_completeness_factor(valid_series, valid_months)
    
    # If no valid data points, return 0 for MOMM
    if cf.sum() == 0:
        return round(valid_series.mean(), 3)
    
    # Calculate the monthly means
    monthly_means = valid_series.groupby(valid_months).mean()
    
    # Ensure the monthly_means has 12 values (one for each month), filling missing months with 0
    monthly_means_full = pd.Series([0.0]*12, index=np.arange(1, 13))  # Index 1 to 12 for each month
    monthly_means_full.update(monthly_means)  # Update with the actual monthly means

    # Calculate MOMM
    MOMM_MONTH_DAYS = np.array([31, 28.24, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])  # Days for each month
    return round((monthly_means_full * MOMM_MONTH_DAYS * cf).sum() / (MOMM_MONTH_DAYS * cf).sum(), 3)


def process_processed_column(series, date_series, data, current_col_name):

    # Make a copy of the series to avoid the warning
    series = series.copy()

    # Find the index of the current column in the DataFrame's columns
    col_idx = data.columns.get_loc(current_col_name)

    # Check if there is a next column in the DataFrame and if it contains 'flags' in its name
    if col_idx < len(data.columns) - 1:
        next_col_name = data.columns[col_idx + 1]
        if 'flags' in next_col_name.lower():
            next_col_values = data[next_col_name]

            # Iterate through the rows of both the current series and the next column
            for idx, (current_value, next_value) in enumerate(zip(series, next_col_values)):
                # Check if the value in the next column (flag column) is non-empty or non-NaN
                if pd.notna(next_value) and next_value != '':
                    # Modify the copied series
                    series.iloc[idx] = 9999

    # Process the current column (unchanged logic from your original code)
    series = pd.to_numeric(series, errors='coerce')  # Convert non-numeric to NaN
    valid_mask = series != 9999
    
    valid_series = series[valid_mask]
    valid_months = date_series[valid_mask].dt.month
    
    if valid_series.empty:
        return {
            'DRR': 0,
            'Mean': 0,
            'MOMM': 0,
            'Min': 0,
            'Max': 0
        }

    return {
        'DRR': calculate_drr(series),
        'Mean': round(valid_series.mean(), 3),
        'MOMM': calculate_momm(valid_series, valid_months),
        'Min': round(valid_series.min(), 3),
        'Max': round(valid_series.max(), 3)
    }



def process_column(series, date_series, data, current_col_name):
    series = pd.to_numeric(series, errors='coerce')  # Convert non-numeric to NaN
    valid_mask = series != 9999
    
    valid_series = series[valid_mask]
    valid_months = date_series[valid_mask].dt.month
    
    if valid_series.empty:
        return {
            'DRR': 0,
            'Mean': 0,
            'MOMM': 0,
            'Min': 0,
            'Max': 0
        }

    return {
        'DRR': calculate_drr(series),
        'Mean': round(valid_series.mean(), 3),
        'MOMM': calculate_momm(valid_series, valid_months),
        'Min': round(valid_series.min(), 3),
        'Max': round(valid_series.max(), 3)
    }

def process_data(data, data_type):
    # Partial function with date_series as the first argument
    if data_type == 'RAW':
        process_func = partial(process_column, date_series=data['Date/Time'], data=data)
    elif data_type == 'PROCESSED':
        process_func = partial(process_processed_column, date_series=data['Date/Time'], data=data)

    # Filter out columns that include 'Flag' in their name
    filtered_columns = [col for col in data.columns if 'flags' not in col and col != 'Date/Time' and col not in ['Latitude', 'Longitude', 'Elevation']]

    # Apply process_func to each column and pass its name as an argument
    results = {col: process_func(data[col], current_col_name=col) for col in filtered_columns}

    return results



def extract_heights(df):
    index_as_str = df.index.astype(str)
    heights = index_as_str.str.extract(r'(\d+)')  # Extract numeric values from the index
    return heights.astype(float)

def remove_brackets(df):
    # Apply column-wise map to ensure we operate on individual Series
    for col in df.columns:
        # Use regex to remove anything inside brackets, including the brackets themselves
        df[col] = df[col].map(lambda x: re.sub(r'\s*\[.*?\]', '', str(x)) if isinstance(x, str) else x)
    return df

def get_lat_long_elev(lat_input=None, lon_input=None, elev_input=None, excel_file='Inputs.xlsx', sheet_name='1. Inputs Setup'):
    
    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)  # Load with no header
    # Extract values starting from row 11 (index 10)
    latitudes = df.iloc[10:, 34].values  # AI is the 35th column (index 34)
    longitudes = df.iloc[10:, 35].values  # AJ is the 36th column (index 35)
    elevations = df.iloc[10:, 36].values   # AK is the 37th column (index 36)
    sheet_numbers = df.iloc[10:, 25].values  # Z is the 26th column (index 25)
    
    for lat, lon, elev, sheet in zip(latitudes, longitudes, elevations, sheet_numbers):
        # Check for matching inputs and not NaN
        if not pd.isna(lat) and not pd.isna(lon) and not pd.isna(elev) and not pd.isna(sheet):
            if (lat_input is None or lat == lat_input) and \
               (lon_input is None or lon == lon_input) and \
               (elev_input is None or elev == elev_input):
                result = {
                    'Latitude': float(lat),
                    'Longitude': float(lon),
                    'Elevation': float(elev),
                    'Sheet Number': str(sheet)
                }
                return result['Sheet Number']

def read_folders_from_excel(excel_file, sheet_name='1. Inputs Setup'):
    df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None) 

    folders = df.iloc[9:17, 7].values 
    read_flags = df.iloc[9:17, 10].values

    valid_folders = [
        folder for folder, flag in zip(folders, read_flags)
        if isinstance(folder, str) and isinstance(flag, str)
        and folder.strip() 
        and flag.strip().lower() == 'yes'
    ]

    return valid_folders


def main():
    start_time = time.time()
    excel_path = 'Inputs.xlsx'
    valid_folders = read_folders_from_excel(excel_path)
    print(f"Valid Folders - {valid_folders}")
    for folder_path in valid_folders:
        # Check if the folder exists before processing
        if not os.path.exists(folder_path):
            print(f"Folder not found: {folder_path}")
            continue
        
        txt_files = [f for f in os.listdir(folder_path) if f.endswith('.txt')]

        for file in txt_files:
            file_path = os.path.join(folder_path, file)
            data, latitude, longitude, elevation = read_data(file_path, excel_path)
            raw_results = process_data(data, 'RAW')
            processed_results = process_data(data, 'PROCESSED')
            

            df = pd.DataFrame.from_dict(raw_results, orient='index')
            p_df = pd.DataFrame.from_dict(processed_results, orient='index')
            df.index.name = 'Data Channel'
            p_df.index.name = 'Data Channel'

            # Remove rows with index starting with 'Unnamed'
            df = df[~df.index.str.startswith('Unnamed')]
            p_df = p_df[~p_df.index.str.startswith('Unnamed')]

            heights = extract_heights(df)
            p_heights = extract_heights(p_df)
            df['Heights'] = heights.values
            p_df['Heights'] = p_heights.values
            

            # Reorder columns to match the existing file structure
            df = df[['Heights', 'DRR', 'Mean', 'MOMM', 'Min', 'Max']]
            p_df = p_df[['Heights', 'DRR', 'Mean', 'MOMM', 'Min', 'Max']]

            # Get the matching sheet number
            sheet_number = get_lat_long_elev(lat_input=latitude, lon_input=longitude, elev_input=elevation)

            if sheet_number is not None:
                print(f"Matching sheet number for Latitude: {latitude}, Longitude: {longitude}, Elevation: {elevation} is Sheet: {sheet_number}")
            else:
                print(f"No matching sheet found for Latitude: {latitude}, Longitude: {longitude}, Elevation: {elevation}")
            
            # Specify the path to the output Excel file
            output_file = 'Inputs.xlsx'

            # Write new data starting from row 15, column M
            print("Writing data into output file - ", output_file)
            with pd.ExcelWriter(output_file, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                df.to_excel(writer, sheet_name=sheet_number, startrow=14, startcol=12, header=True, index=True)
                p_df.to_excel(writer, sheet_name=sheet_number, startrow=14, startcol=3, header=True, index=True)

            print(f"Data has been written to {output_file}")
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Total execution time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()

"""
1. Raw - Normal
2. Processed - incl Flag Status - if Cell is non-empty make it 9999 and omit that row 
"""