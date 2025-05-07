import requests

# 景點
url = "https://media.taiwan.net.tw/XMLReleaseALL_public/Scenic_Spot_C_f.csv"
response = requests.get(url)

with open("Scenic_Spot_C_f(NEW).csv", "wb") as f:
    f.write(response.content)

# 餐廳
url = "https://media.taiwan.net.tw/XMLReleaseALL_public/Restaurant_C_f.csv"
response = requests.get(url)

with open("Restaurant_C_f(NEW).csv", "wb") as f:
    f.write(response.content)

# 住宿
url = "https://media.taiwan.net.tw/XMLReleaseALL_public/Hotel_C_f.csv"
response = requests.get(url)

with open("Hotel_C_f(NEW).csv", "wb") as f:
    f.write(response.content)
