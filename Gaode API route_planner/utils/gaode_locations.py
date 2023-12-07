import requests
import json
import math
import pandas as pd
from pathlib import Path

# 网络上泄露的KEY, 不一定可用
# '4ebb849f151dddb3e9aab7abe6e344e2'
# '470fdf698e3aab758d4cb026244f5194'
# '740f50c6fabd5801d0fad1cba62446d9'
# '4328d392605802de34406045b9701bb8'
# 'a46ad0b9e7f771b6dca9e5a1496dca64'
# 'd97173b555725243fea3fa3eff7529b6'
# 'e7bb2a93c7c4a7fe26bf4c4272108e8c'

KEY = "高德KEEY"
CITY = "厦门"  # 或者0592也表示厦门市, 见https://www.showapi.com/book/view/3761/5
KEYWORDS = "鹭燕"


def request_api(url):
    """Request url and get content as json."""
    try:
        r = requests.get(url=url, timeout=30)
        if r.status_code == 200:
            result_json = json.loads(r.text)
            # check if infocode is 10000 (https://lbs.amap.com/api/webservice/guide/tools/info/)
            if 'infocode' in result_json and result_json['infocode'] == '10000':
                return result_json
            else:
                print('请求url异常')
                print(f"异常url:{url}")
                print(result_json)
                return None
        return None
    except requests.RequestException:
        print('请求url异常')
        print(f"异常url:{url}")
        return None


def main():
    """Search keywords and save locations."""
    offset = 20
    index_url = f'https://restapi.amap.com/v3/place/text?key={KEY}&city={CITY}&keywords={KEYWORDS}&offset={offset}&page=1'
    index_result = request_api(index_url)
    if index_result is None:
        return
    pages = math.ceil(int(index_result['count']) / offset)
    loc_info = pd.DataFrame(index_result['pois'])

    # the search result is 1 page
    if pages == 1:
        loc_info.to_csv(Path(__file__).parent / "../gaode/locations.csv")
        return

    # the search result has more than 1 page
    for page in range(2, pages + 1):
        url = f'https://restapi.amap.com/v3/place/text?key={KEY}&city={CITY}&keywords={KEYWORDS}&offset={offset}&page={page}&extensions=base'
        result = request_api(url)
        if index_result is None:
            break
        result_info = pd.DataFrame(result['pois'])
        loc_info = pd.concat([loc_info, result_info], ignore_index=True)
    loc_info.to_csv(Path(__file__).parent / "../gaode/locations.csv")


if __name__ == '__main__':
    main()
