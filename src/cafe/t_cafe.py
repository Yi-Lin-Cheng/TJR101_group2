import pandas as pd
import re
from datetime import datetime
import uuid

# -------- 地址清理函數 --------
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
        r"([\u4e00-\u9fa5]{2,4}(區|鄉|鎮|市))|北區|東區|南區|西區|中區|"
        r"那瑪夏區|阿里山鄉|三地門鄉|太麻里鄉"
    )

    text = re.sub(r"(台灣)?\d{3,6}$", "", text)
    text = re.sub(rf"^([0-9]{{3,6}})?台灣?", "", text)
    text = re.sub(rf"({city_pattern})([0-9]{{3,5}})?", r"\1", text)

    text = re.sub(r"No\.?([0-9]+(?:之[0-9]+)?)", r"\1號", text, flags=re.IGNORECASE)
    text = re.sub(r"No", "", text, flags=re.IGNORECASE)

    return text

# -------- 縣市/鄉鎮區 擷取函數 --------
def extract_county_area(address):
    county_pattern = r"(台北市|新北市|桃園市|台中市|台南市|高雄市|基隆市|新竹市|嘉義市|新竹縣|苗栗縣|彰化縣|南投縣|雲林縣|嘉義縣|屏東縣|台東縣|花蓮縣|宜蘭縣)"
    area_pattern = r"([\u4e00-\u9fa5]{2,4}(區|鄉|鎮|市))|北區|東區|南區|西區|中區|那瑪夏區|阿里山鄉|三地門鄉|太麻里鄉"

    county_match = re.search(county_pattern, address)
    area_match = re.search(area_pattern, address)

    county = county_match.group(0) if county_match else ""
    area = area_match.group(0) if area_match else ""

    return county, area

# -------- f_type 自動分類函數 --------
def classify_f_type(f_name):
    if pd.isna(f_name):
        return "餐廳"
    f_name_lower = str(f_name).lower()
    if any(keyword in f_name_lower for keyword in ["coffee", "cafe"]) or "咖啡" in f_name:
        return "咖啡廳"
    return "餐廳"

# -------- 主處理流程 --------
df = pd.read_csv("cafe_craw_googlemap.csv", encoding="utf-8", engine="python")

# 新增所需欄位並填充
df["food_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
df["f_type"] = df["f_name"].apply(classify_f_type)  # 自動分類

# 建立時間與更新時間
now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df["create_time"] = now_time
df["update_time"] = now_time

# 儲存結果
df.to_csv("cleaned_cafe_all.csv", index=False, encoding="utf-8")
