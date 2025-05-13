import re


indoor = r"(館|中心|堂|宅|共和國|廠|城|基地|空間|店|屋|家|書|合作社)"
outdoor = r"(自行車道|步道|森林|公園|登山|海灘|湖|露營|老街|夜市|廣場|農場|地理中心碑|義塚)"

def classify(name):
    name = str(name)
    if re.search(outdoor, name):
        return "戶外"
    elif re.search(indoor, name):
        return "室內"
    else:
        return "戶外"  # 預設歸為戶外

df["分類"] = df["名稱"].apply(classify)

df = df.sort_values("分類")

df[["名稱", "分類"]].to_csv("分類結果.csv", index=False)
print("存檔完畢")