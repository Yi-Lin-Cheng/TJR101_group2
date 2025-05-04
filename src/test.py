from utils import get_connection,close_connection


# 開啟連線
conn,cursor = get_connection()

# 關閉連線
close_connection(conn,cursor)

