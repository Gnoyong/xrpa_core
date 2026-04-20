import datetime
from datetime import date
from datetime import datetime as dt


def get_last_month_range():
    """
    获取上一个月的第一天和最后一天的日期。

    Returns:
        tuple: 包含两个 datetime.date 对象：
            - 第一个是上个月的第一天（如 2025-07-01）
            - 第二个是上个月的最后一天（如 2025-07-31）

    示例:
        >>> get_last_month_range()
        (datetime.date(2025, 7, 1), datetime.date(2025, 7, 31))
    """
    today = datetime.date.today()
    first_day_this_month = today.replace(day=1)
    last_day_last_month = first_day_this_month - datetime.timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)
    return first_day_last_month, last_day_last_month


def is_within_days(target_date: dt, reference_date: dt, days: int) -> bool:
    """
    判断 target_date 是否在 reference_date 前后 days 天之内

    :param target_date: 要判断的日期
    :param reference_date: 参考日期
    :param days: 天数范围
    :return: True 表示在范围内，False 表示不在
    """
    delta = abs((target_date - reference_date).days)
    return delta <= days


def is_today(datetime: dt) -> bool:
    if isinstance(datetime, dt):
        return datetime.date() == date.today()
    elif isinstance(dt, date):  # 如果传进来的是 date 对象
        return dt == date.today()
    else:
        raise TypeError(f"不支持的类型: {type(datetime)}")
