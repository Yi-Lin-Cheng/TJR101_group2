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

def parse_time(date_str: str) -> datetime.datetime:
    dt = None
    match = re.match(r"(\d{4})年(\d{1,2})月(\d{1,2})日", date_str)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        dt = datetime.datetime(year, month, day)
    else:
        match = re.match(r"(\d{1,2})月(\d{1,2})日", date_str)
        if match:
            month = int(match.group(1))
            day = int(match.group(2))
            year = datetime.datetime.now().year  # 使用當前年份
            dt = datetime.datetime(year, month, day)

    return dt   

def parse_s_time(row):
    date = str(row['date'])
    s_time = date
    e_time = date
    
    if "-" in date:
        dates = date.split("-")
        s_time = dates[0].split("(")[0]
        e_time = dates[1].split("(")[0]
        
    else:
        s_time = date.split("(")[0]
        e_time = s_time
        
    return s_time

def parse_e_time(row):
    date = str(row['date'])
    s_time = date
    e_time = date
    
    if "-" in date:
        dates = date.split("-")
        s_time = dates[0].split("(")[0]
        e_time = dates[1].split("(")[0]
        
    else:
        s_time = date.split("(")[0]
        e_time = s_time
        
    return e_time     

def trim_s_time(row):
    date = str(row['s_time'])
    return date.strip()

def trim_e_time(row):
    date = str(row['e_time'])
    return date.strip()

def format_s_time(row):
    date = parse_time(row['s_time'])
    return date.strftime("%Y/%m/%d")

def format_e_time(row):
    date = parse_time(row['e_time'])
    return date.strftime("%Y/%m/%d")


# region t_date
def t_date(df: pd.DataFrame):
    
    df['s_time'] = None
    df['e_time'] = None
    
    for hash, item in df.iterrows():
        df["s_time"] = df.apply(parse_s_time, axis=1)
        df["e_time"] = df.apply(parse_e_time, axis=1)
        df["s_time"] = df.apply(trim_s_time, axis=1)
        df["e_time"] = df.apply(trim_e_time, axis=1)
        df["s_time"] = df.apply(format_s_time, axis=1)
        df["e_time"] = df.apply(format_e_time, axis=1)
    return df
# endregion t_data


# region main
def main():
    
    source_data_path = data_dir / "t_address.csv"
    save_data_path = data_dir / "final_data.csv"
    
    if not Path.exists(source_data_path):
        print(f"Source File{source_data_path}:  Not Exists")
        return         
    
    # region 清理日期格式
    try:
        
        df = pd.read_csv(f"{source_data_path}", encoding="utf-8-sig") 
        df = t_date(df)
        df.to_csv(f"{save_data_path}", encoding="utf-8-sig", index=False)
    except Exception as e:
        print(str(e))
    # endregion 清理日期格式
    
    

    
# endregion main

if __name__ == "__main__":
    main()