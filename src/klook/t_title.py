import datetime
import json
import math
import random
import re
import time
import pandas as pd
import numpy as np
import requests
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/klook")
else:
    data_dir = Path("data/klook")

# region 清除 title 末端有包含 location 的資訊
def trim_location_from_title(row):
    title = str(row['title'])
    location = str(row['location'])
    if title.endswith(location):
        return title[: -len(location)].rstrip().rstrip('｜')
    
    return title    
# endregion 清除 title 末端有包含 location 的資訊

# region t_title
def t_title(df: pd.DataFrame):
    df["county"] = None
    
    # # 去除並擷取最前面縣市資訊
    df['county'] = df['title'].astype(str).str.split('|').str[0]
    df["title"] = df['title'].astype(str).str.split('|').str[1:].str.join("|")

    # # 去除最後面地理位置資訊
    df['title'] = df.apply(trim_location_from_title, axis=1)
    
    return df
# endregion t_title

# region main
def main():
    
    source_data_path = data_dir / "e_coordinate.csv"
    save_data_path = data_dir / "t_title.csv"
    
    if not Path.exists(source_data_path):
        print(f"Source File{source_data_path}:  Not Exists")
        return         
    
    # region 清理 title 資訊
    try:
        df = pd.read_csv(f"{source_data_path}", encoding="utf-8-sig")
        df = t_title(df)
        df.to_csv(f"{save_data_path}", encoding="utf-8-sig", index=False)
        pass
    except Exception as e:
        print(str(e))
    # endregion 清理 title 資訊
    
# endregion main

if __name__ == "__main__":
    main()