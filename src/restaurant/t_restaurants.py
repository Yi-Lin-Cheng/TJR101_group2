import pandas as pd
import re

# 以下是地址的清理過程
def cleaned_address(text):
    if pd.isna(text):
        return ""
    
    text = re.sub(r"\s+", "", str(text).replace("-", "之").replace(",", "").replace("臺", "台"))
    text = re.sub(r"f", "樓", text, flags=re.IGNORECASE)

    city_pattern = (
        r"(台北市|新北市|桃園市|台中市|台南市|高雄市|基隆市|新竹市|嘉義市|新竹縣|"
        r"苗栗縣|彰化縣|南投縣|雲林縣|嘉義縣|屏東縣|台東縣|花蓮縣|宜蘭縣)"
    )
    area_pattern = (
        r"([\u4e00-\u9fa5]{2}(區|鄉|鎮|市))|北區|東區|南區|西區|中區|"
        r"那瑪夏區|阿里山鄉|三地門鄉|太麻里鄉"
    )

    text = re.sub(r"(台灣)?\d{3,6}$", "", text)
    text = re.sub(rf"^([0-9]{{3,6}})?台灣", "", text)
    text = re.sub(rf"({city_pattern})([0-9]{{3,5}})?", r"\1", text)

    text = re.sub(r"No\.?([0-9]+(?:之[0-9]+)?)", r"\1號", text, flags=re.IGNORECASE)
    text = re.sub(r"No", "", text, flags=re.IGNORECASE)

    pattern = rf"^{city_pattern}"
    if re.match(pattern, text):
        return text

    floor_match = re.search(r"([0-9]+樓)(?:之[0-9]+)?", text)
    floor = floor_match.group(1) if floor_match else ""

    city = re.search(city_pattern, text)
    city_str = city.group(0) if city else ""

    area = re.search(area_pattern, text)
    area_str = area.group(0) if area else ""

    no = re.search(r"[0-9]+(之[0-9]+)?號", text)
    no_str = no.group(0) if no else ""

    core = text
    for part in [city_str, area_str, no_str, floor]:
        core = core.replace(part, "")
    core = re.sub(r"(號)+", "號", core)

    return f"{city_str}{area_str}{core}{no_str}{floor}"

df = pd.read_csv("02_restaurants(compared)_raw_latest.csv")

# 以下是名稱的清理過程
mismatch = df['gov Name 景點名稱'] != df['名稱']

# 以名稱欄位為主，取代gov Name 景點名稱欄位
df.loc[mismatch, 'gov Name 景點名稱'] = df.loc[mismatch, '名稱']

# 刪除 gov Name 景點名稱欄位
df.drop(columns=['gov Name 景點名稱'], inplace=True)

# 插入地址欄位清洗
df["地址"] = df["地址"].apply(cleaned_address)

# 將更新後的資料寫入新的 CSV 檔案
df.to_csv("cleaned_restaurnants.csv",index = False)