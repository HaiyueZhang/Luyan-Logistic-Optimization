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


def main():
    """Get travel time of all locations."""
    now = datetime.now().strftime("%Y%m%d_%H%M")
    input_filepath = f"{Path(__file__).parent}/../gaode/Xiamen_Unique_locations最新有仓库.csv"
    input_filename_without_ext = Path(input_filepath).stem

    output_directory = f"{Path(__file__).parent}/../gaode/"
    # Ensure the output directory exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    CHECKPOINT_CSV = os.path.join(output_directory, f"progress_checkpoint_{input_filename_without_ext}.csv")
    
    locations = pd.read_csv(input_filepath)
    locations = locations.dropna(subset=['location']).reset_index(drop=True)
    n_locations = locations.shape[0]
    
    # Check if checkpoint CSV exists and load its content
    dataframes = []
    if os.path.exists(CHECKPOINT_CSV):
        df_checkpoint = pd.read_csv(CHECKPOINT_CSV)
        dataframes.append(df_checkpoint)

    # Use the provided code segment
    start_origin = locations.index[0]  # get the first index of the cleaned dataframe
    origin_destination_idx = [(start_origin, i) for i in locations.index if i > start_origin]
    origin_destination_param = [(ori_id, des_id, locations['location'][ori_id], locations['location'][des_id], KEY) for ori_id, des_id in origin_destination_idx]

    # Exclude already processed pairs
    processed_pairs = [(row['origin'], row['destination']) for _, row in df_checkpoint.iterrows()] if dataframes else []
    origin_destination_param = [param for param in origin_destination_param if (param[0], param[1]) not in processed_pairs]
    
    pbar = tqdm(total=len(origin_destination_param))
    try:
        with Pool(THREAD) as pool:
            for res in pool.imap_unordered(get_travel_time, origin_destination_param):
                if res is not None:
                    dataframes.append(res)
                    df_to_save = pd.concat(dataframes).reset_index(drop=True)
                    df_to_save.to_csv(CHECKPOINT_CSV, index=False)
                pbar.update(1)
        pbar.close()
    except KeyboardInterrupt:
        print("\nProgress saved. You can resume later.")
        return

    # If you wish, rename or save the checkpoint CSV to another final name here
    final_csv_name = f"{Path(__file__).parent}/../gaode/[{start_origin}]trip_details_{input_filename_without_ext}_{now}.csv"
    os.rename(CHECKPOINT_CSV, final_csv_name)

if __name__ == "__main__":
    main()