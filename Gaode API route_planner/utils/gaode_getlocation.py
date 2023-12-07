import pandas as pd
from pathlib import Path
import requests

# Read the CSV file
df = pd.read_csv(f"{Path(__file__).parent}/../gaode/7_21_address.csv", sep='\t', engine='python')

# Define the function to get the location data from the Gaode API
def get_location(address):
    url = "https://restapi.amap.com/v3/geocode/geo"
    params = {
        'key': '5879385d2232d983180f658baf62512e',  # Replace with your actual Amap API key
        'address': address
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if data['status'] == '1' and data['geocodes']:
            location = data['geocodes'][0]['location']
            return location
    return None

# Apply the function to the 'INCEPTADDR' column to get the location data
df['location'] = df['INCEPTADDR'].apply(get_location)

# Save the updated DataFrame to a new CSV file
df.to_csv(f"{Path(__file__).parent}/../gaode/7_21_address_with_location.csv", index=False)
