import requests
import json
import time
import random
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from multiprocessing import Pool, Queue
from itertools import combinations
from tqdm import tqdm
import os
import pickle
from itertools import product

# Create and configure logger
logging.basicConfig(filename=f"{Path(__file__).parent}/gaode_travel_time.log",
                    format='%(asctime)s %(message)s',
                    filemode='w', 
                    encoding='utf-8')
logger = logging.getLogger()

logger.setLevel(logging.INFO) # 设置logging等级
STRATEGY = 0  # 速度优先, 其他选择见https://lbs.amap.com/api/webservice/guide/api/direction/#driving
THREAD = 8 # 使用线程数量
A_EQUAL_B = True # 假设A到B等于B到A
KEY = ['540213f9b13ededa0bcbdac8cabf7ab6',
       '470fdf698e3aab758d4cb026244f5194',
       '740f50c6fabd5801d0fad1cba62446d9',
       '4328d392605802de34406045b9701bb8',
       'a46ad0b9e7f771b6dca9e5a1496dca64',
       'd97173b555725243fea3fa3eff7529b6',
       'e7bb2a93c7c4a7fe26bf4c4272108e8c',
       'ba7c608dccdfcbbec50441ddb88466e3',
       'bb0667770abae0b69c421bb49437a27e',
       '2a923256be339bbaf521e8c31f472cc7',
       'd7bb98e7185300250dd5f918c12f484b',
       'db146b37ef8d9f34473828f12e1e85ad',
       '5762aacbfb03b463119fc10a2172d115',
       '08da3ea9e0e79d355556aead7314f7b2',
       '1acb0a2dce52292cd64fb5cc246b4824',
       '5879385d2232d983180f658baf62512e',
       'd5f8a7905cda4ee111c0d49cc5eeb50b',
       'ae392e4dc8fcd4abec709efebb6eba09',
       '20b95a4ec325126c5dc4894a93b8d635',
       'd6566c3f61dd35aa370d3f8a760ada8a'] 
    

def ceildiv(a, b=60):
    """Round up the division. Set the divider to 60 seconds to get the duration in minutes."""
    return -(a // -b)


def request_api(url):
    """Request url and get content as json."""
    try:
        r = requests.get(url=url, timeout=30)
        if r.status_code == 200:
            result_json = json.loads(r.text)
            return result_json
        return None
    except requests.RequestException:
        logger.debug('请求url异常')
        logger.debug(f"异常url:{url}")
        return None

def get_travel_time(args):
    """Get travel time between two locations."""
    originID, destinationID, origin, destination, keys = args
    random.shuffle(keys)
    i = 0
    while i < len(keys):
        key = keys[i]
        url = f'https://restapi.amap.com/v3/direction/driving?key={key}&origin={origin}&destination={destination}&strategy={STRATEGY}'
        result_json = request_api(url)
        # 无法请求url: 15秒后重试
        if result_json is None:
            time.sleep(10)
        # 访问过于频繁: 61秒后重试
        elif 'infocode' in result_json and result_json['infocode'] in ['10004', '10016', '10019', '10020', '10021', '10029']:
            logger.info(f"等待重试({key})")
            logger.info(result_json)
            time.sleep(61)
        # 达到上限: 更换key后重试
        elif 'infocode' in result_json and result_json['infocode'] in ['10044', '10009']:
            logger.info(f"更换key({key})")
            logger.info(result_json)
            i += 1
        # 其他错误
        elif 'infocode' not in result_json or result_json['infocode'] != '10000':
            logger.info(f"等待重试({key})")
            logger.info(result_json)
            time.sleep(15)
        # API访问正常
        else:
            # Drop unwanted keys here, for example:
            unwanted_keys = ['strategy', 'tolls', 'toll_distance', 'traffic_lights', 'steps', 'restriction'] # replace with the actual keys you want to drop
            for unwanted_key in unwanted_keys:
                if unwanted_key in result_json['route']['paths'][0]:
                    del result_json['route']['paths'][0][unwanted_key]

            # Add origin and destination info
            result_json['route']['paths'][0]['origin'] = originID
            result_json['route']['paths'][0]['destination'] = destinationID
            return pd.DataFrame({k: pd.Series([v]) for k, v in result_json['route']['paths'][0].items()})
    logger.info("网络连接问题或者所有KEY都不能用")
    return None
    
def save_progress(dataframes, remaining_tasks):
    """Save crawled data and remaining tasks to a checkpoint."""
    with open(CHECKPOINT_FILE, 'wb') as f:
        pickle.dump((dataframes, remaining_tasks), f)


def load_progress():
    """Load progress from a checkpoint file if it exists."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'rb') as f:
            dataframes, remaining_tasks = pickle.load(f)
            return dataframes, remaining_tasks
    return [], []

def get_location_pairs(df):
    """Get unique location pairs from the dataframe."""
    origins = df['origin_location'].unique()
    destinations = df['destination_location'].unique()
    return list(product(origins, destinations))

def update_dataframes_with_api_results(dataframes, location_pairs, output_filepath):
    """Update the given dataframes with the results from the API."""
    updated_dataframes = []

    origin_destination_param = [(df['origin'].iloc[0], df['destination'].iloc[0], origin, destination, KEY)
                                for df in dataframes for origin, destination in location_pairs
                                if df['origin_location'].iloc[0] == origin and df['destination_location'].iloc[0] == destination]
    
    pbar = tqdm(total=len(origin_destination_param))
    try:
        with Pool(THREAD) as pool:
            for res in pool.imap_unordered(get_travel_time, origin_destination_param):
                if res is not None:
                    # process your result here
                    # e.g., update your dataframes
                    updated_dataframes.append(res)
                    # saving progress (optional)
                    # df_to_save = pd.concat(updated_dataframes).reset_index(drop=True)
                    # df_to_save.to_csv(CHECKPOINT_CSV, index=False)
                pbar.update(1)
        # Concatenate the updated dataframes and save to a new CSV file
        final_df = pd.concat(updated_dataframes).reset_index(drop=True)
        final_df.to_csv(output_filepath, index=False)
        pbar.close()
    except KeyboardInterrupt:
        print("\nProgress saved. You can resume later.")
        pbar.close()
        # Saving logic goes here if needed
        return

    for df in dataframes:
        origin = df['origin_location'].iloc[0]
        destination = df['destination_location'].iloc[0]
        for pair in location_pairs:
            if origin == pair[0] and destination == pair[1]:
                # Use your existing get_travel_time function
                result = get_travel_time((df['origin'].iloc[0], df['destination'].iloc[0], origin, destination, KEY))
                if result is not None:
                    distance = result['distance'].iloc[0]
                    duration = result['duration'].iloc[0]
                else:
                    distance = np.nan
                    duration = np.nan

                # Insert values directly after the 'destination_location' column
                idx_distance = df.columns.get_loc('destination_location') + 1
                df.insert(idx_distance, 'distance', distance)
                idx_duration = df.columns.get_loc('distance') + 1
                df.insert(idx_duration, 'duration', duration)
                break
        updated_dataframes.append(df)
    
    return updated_dataframes

def main_new_csv():
    """Update the new CSV with distance and duration from the API."""
    input_filepath = f"{Path(__file__).parent}/../gaode/9.1-停车点400mlocations.csv"  # Update this path to your new CSV file
    output_filepath = f"{Path(__file__).parent}/../gaode/9.1-停车点400mlocations_nd.csv"  # Name of the output CSV file

    # Load the new CSV
    df = pd.read_csv(input_filepath)

    # Get unique location pairs
    location_pairs = get_location_pairs(df)

    # Split the dataframe into smaller dataframes based on unique location pairs
    dataframes = []
    for pair in location_pairs:
        sub_df = df[(df['origin_location'] == pair[0]) & (df['destination_location'] == pair[1])]
        if not sub_df.empty:
            dataframes.append(sub_df)

    # Update the dataframes with the results from the API
    updated_dataframes = update_dataframes_with_api_results(dataframes, location_pairs, output_filepath)

    # Concatenate the updated dataframes and save to a new CSV file
    final_df = pd.concat(updated_dataframes).reset_index(drop=True)
    final_df.to_csv(output_filepath, index=False)

if __name__ == "__main__":
    main_new_csv()