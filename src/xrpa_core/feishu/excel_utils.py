def number_to_letters(n: int) -> str:
    """
    将数字转换为字母表示（类似 Excel 列号规则）
    :param n: int, 从 1 开始
    :return: str, 对应的字母
    """
    if n < 1:
        raise ValueError("数字必须 >= 1")

    result = []
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result.append(chr(65 + remainder))  # 65 是 'A'
    return "".join(reversed(result))


def letters_to_number(s: str) -> int:
    """
    将字母表示转换为数字（类似 Excel 列号规则）
    :param s: str, 列字母（如 "A", "Z", "AA", "ZZ"）
    :return: int, 对应的列号（从 1 开始）
    """
    if not s.isalpha():
        raise ValueError("输入必须是字母")

    s = s.upper()
    result = 0
    for char in s:
        result = result * 26 + (ord(char) - 64)  # 'A' = 65，所以减 64 得到 1
    return result
