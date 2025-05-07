import json
import pandas as pd
from pandas import DataFrame

def trim_location_from_title(row):
    title = str(row['title'])
    location = str(row['location'])
    if title.endswith(location):
        return title[: -len(location)].rstrip().rstrip('｜')
    return title

def t_title(df: DataFrame):
    # # 去除最前面縣市資訊
    df["title"] = df['title'].astype(str).str.split('|').str[1:].str.join("|")

    # # 去除最後面地理位置資訊
    df['title'] = df.apply(trim_location_from_title, axis=1)
    df.to_csv("klook/02_klook_transform_data.csv", encoding="utf-8-sig", index=False)
    
    
    
def main():
    df = pd.read_csv("klook/01_klook_data.csv", encoding="utf-8-sig")
    
    t_title(df)
    
    
    pass

if __name__ == "__main__":
    main()