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
    df = pd.read_csv("crawler_data\spot.csv")
    # print(df[df["region"].str.len() > 4]["region"])
    # return
    for hash, item in df.iterrows():
        # county = item["county"] if item["county"] in counties else None

        if pd.notna(item['geo_loc']) and item["area"] and item["county"]:
            lat = str(item["geo_loc"]).split(",")[1]
            lng = str(item["geo_loc"]).split(",")[0]            
            
            # CALL SERVER API - create exhibit
            
            # print(item["county"])
            # print(item["county"] in counties)
            
            
            resp = requests.post("http://localhost:8087/spot", data=json.dumps({
                "s_name": item["s_name"],
                "county": item['county'],
                "address": item["address"],
                "lat": lat,
                "lng": lng,
                "pic_url": item["pic_url"],
                "gmaps_url": item["gmaps_url"],
                "type": item["type"],
                "area": item["area"],
                "rate": item["rate"] if pd.notna(item["area"]) else None,
                "comm": item["comm"] if pd.notna(item["comm"]) else None,
                "b_hour": item["b_hour"] if pd.notna(item["b_hour"]) else None,
            }))
            print(resp.json())
            
            
          
    

if __name__ == "__main__":
    main()