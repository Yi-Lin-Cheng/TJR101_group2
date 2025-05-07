import json
import numpy as np
import requests
import pandas as pd

counties = [
                "台北", "新北", "桃園", "台中",
                "台南", "高雄", "苗栗", "彰化",
                "南投", "雲林", "嘉義", "屏東",
                "宜蘭", "花蓮", "台東", "澎湖",
                "金門", "連江", "新竹", "嘉義",
            ]

def main():
    df = pd.read_csv("klook/data_test/01_klook_data_transform_county.csv")

    for hash, item in df.iterrows():
        county = item["county"] if item["county"] in counties else None
        if pd.notna(item['lng']) and  pd.notna(item['lat']) and county:
            print(item["lng"], item["lat"])
            
            # CALL SERVER API - create exhibit
            
            address = None
            if pd.notna(item["address"]) or pd.notna(item["location"]):
                address = item["address"] if pd.notna(item["address"]) else item["location"]
            print(item["county"])
            print(item["county"] in counties)
            resp = requests.post("http://localhost:8087/exhibit", data=json.dumps({
                "e_name": item["title"],
                "s_time": item["s_time"],
                "e_time": item["e_time"],
                "county": county,
                "address": address,
                "lat": item["lat"],
                "lng": item["lng"],
                "pic_url": item["image_url"],
                "klook_url": item["event_url"],
                "type": "aaa"
            }))
            print(resp.json())
            
            
          
    

if __name__ == "__main__":
    main()