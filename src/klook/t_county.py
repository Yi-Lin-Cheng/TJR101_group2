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

data_dir = Path("data", "klook")


# region t_county
def t_county(df: pd.DataFrame):
    df["county"] = df["county"].astype(str).\
                                str.replace("臺", "台").\
                                str.replace(r'[縣市]', '', regex=True).\
                                str.strip()
    return df
# endregion t_county

# region main
def main():
    
    source_data_path = data_dir / "t_title.csv"
    save_data_path = data_dir / "t_address.csv"
    
    if not Path.exists(source_data_path):
        print(f"Source File{source_data_path}:  Not Exists")
        return     
    
    # region 轉換字(臺->台)(county)
    try:
        df = pd.read_csv(f"{source_data_path}", encoding="utf-8-sig") 
        df = t_county(df)
        df.to_csv(f"{save_data_path}", encoding="utf-8-sig", index=False)        
        
    except Exception as e:
        print(str(e))
    
    # endregion
    
# endregion main

if __name__ == "__main__":
    main()