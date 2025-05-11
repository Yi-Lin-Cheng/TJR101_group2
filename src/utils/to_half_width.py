def to_half_width(text):
    return "".join(
        (
            chr(ord(char) - 0xFEE0)
            if "！" <= char <= "～"
            else " " if char == "　" else char  # 全形空白
        )
        for char in text
    )
