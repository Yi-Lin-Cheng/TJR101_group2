import re
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

from tasks import normalize_address
from utils import to_half_width

data_dir = Path("data", "food")


# 縣市/鄉鎮區擷取
def extract_county_area(address):
    county_pattern = r"(台北市|新北市|桃園市|台中市|台南市|高雄市|基隆市|新竹市|嘉義市|新竹縣|苗栗縣|彰化縣|南投縣|雲林縣|嘉義縣|屏東縣|台東縣|花蓮縣|宜蘭縣)"
    area_pattern = r"([\u4e00-\u9fa5]{2}(區|鄉|鎮|市))|北區|東區|南區|西區|中區|那瑪夏區|阿里山鄉|三地門鄉|太麻里鄉"

    county_match = re.search(county_pattern, address)
    county = county_match.group(0) if county_match else ""
    address = address.replace(county, "")

    area_match = re.search(area_pattern, address)
    area = area_match.group(0) if area_match else ""

    return county, area


def clean_name(matched_data):
    # 清理名稱
    matched_data = matched_data.copy()
    matched_data.loc[matched_data["f_name"].str.len() > 20, "f_name"] = (
        matched_data["f_name"].str.split(r"\||│|丨|｜|\-|－|/|／").str[0]
    )
    matched_data["f_name"] = matched_data["f_name"].apply(to_half_width)
    return matched_data

def loc_transform(geo_loc):
    geo_loc = geo_loc.replace("POINT(","").replace(")","")
    lng,lat = geo_loc.split()
    return f"POINT({lat} {lng})"
    


# 主流程
df = pd.read_csv(
    data_dir / "cafe_craw_googlemap.csv", encoding="utf-8", engine="python"
)

# 新增所需欄位並填空
df["food_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
df["f_type"] = "咖啡廳"
df.drop(columns=["region_town"], inplace=True)

# 地址清理
df = df[df["address"].notna()]
df["address"] = df["address"].apply(normalize_address)

# county, area 擷取
df[["county", "area"]] = df["address"].apply(extract_county_area).apply(pd.Series)


df["geo_loc"] = df["geo_loc"].apply(loc_transform)
# 清理名字
df = clean_name(df)
# 建立時間與更新時間
now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df["create_time"] = now_time
df["update_time"] = now_time

# 儲存結果
df.to_csv(data_dir / "cleaned_cafe_final.csv", index=False, encoding="utf-8")
