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
    df = pd.read_csv("klook/crawler_data/accomo.csv")
    # print(df[df["region"].str.len() > 4]["region"])
    # return
    for hash, item in df.iterrows():
        # county = item["county"] if item["county"] in counties else None
        
        if pd.notna(item['lng']) and  pd.notna(item['lat']) and item["region"]:
            print(item["lng"], item["lat"])
            
            # CALL SERVER API - create exhibit
            
            # print(item["county"])
            # print(item["county"] in counties)
            
            
            resp = requests.post("http://localhost:8087/accomo", data=json.dumps({
                "a_name": item["name"],
                "county": item['region'],
                "address": item["add"],
                "lat": item['lat'],
                "lng": item['lng'],
                "pic_url": item["img_url"],
                "booking_url": item["url"],
                "type": item["type"],
                "district": item["town"]
            }))
            print(resp.json())
            
            
          
    

if __name__ == "__main__":
    main()