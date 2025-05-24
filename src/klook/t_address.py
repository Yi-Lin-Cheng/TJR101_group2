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

# region 清除 address 包含括號的資料
def trim_location_from_address(row):
    address = str(row['address'])
    if address and address != "nan":
        return address.split("（")[0]
    else:
        row['address'] = ""
    
    return row['address']   
# endregion 清除 address 包含括號的資料

# region 清除 address 內容為https url
def clear_url_as_arress(row):
    
    address = str(row['address'])
    if address.strip().startswith("http"):
        return ""  
    
    return row['address']
# endregion 清除 address 內容為https url

# region t_address_location
def t_address_location(df: pd.DataFrame):
    
    
    df["address"] = df["address"].astype(str).str.strip()
    
    # 清除括號內容
    df["address"] = df.apply(trim_location_from_address, axis=1)
    df["address"] = df["address"].astype(str).str.strip()
    
    # 清除地址為https URL連結
    df["address"] = df.apply(clear_url_as_arress, axis=1)
    
    return df
# endregion t_address_location

# region main
def main(source_file: str = "", save_file: str = ""):
    
    # source_data_path = data_dir / "e_data_detail.csv"
    # save_data_path = data_dir / "t_address.csv"
    
    source_data_path = data_dir / source_file
    save_data_path = data_dir / save_file     
    
    # region 清理地理位置資訊
    try:
        df = pd.read_csv(f"{source_data_path}", encoding="utf-8-sig") 
        df = t_address_location(df)
        df.to_csv(f"{save_data_path}", encoding="utf-8-sig", index=False)
    except Exception as e:
        print(str(e))    
    # endregion 清理地理位置資訊

    
# endregion main

if __name__ == "__main__":
    main()