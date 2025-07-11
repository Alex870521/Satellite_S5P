import re
from datetime import datetime, timedelta
import pytz


def extract_datetime_from_filename(filename, to_local=True, local_tz='Asia/Taipei'):
    """
    從檔名提取日期時間並返回 datetime 對象

    Parameters:
    -----------
    filename : str
        檔名
    to_local : bool, optional
        是否轉換為本地時間，預設為 True
    local_tz : str, optional
        本地時區，預設為 'Asia/Taipei'
    """
    date_obj = None

    # Sentinel-5P 格式
    s5p_match = re.search(r'S5P_\w+_\w+__\w+____(\d{8}T\d{6})_', filename)
    if s5p_match:
        date_str = s5p_match.group(1)
        date_obj = datetime.strptime(date_str, '%Y%m%dT%H%M%S')


    # MODIS 格式解析
    elif not date_obj:
        # 匹配兩種主要的 MODIS 格式
        # 格式1: MOD04_L2.A2025001.0210.061.2025001180158.hdf (包含時間)
        # 格式2: MCD19A2.A2025001.h29v06.061.2025003055754.hdf (不包含時間)

        # 先嘗試格式1 - 有時間信息的格式
        modis_with_time = re.search(r'(?:MOD|MYD|MCD)\w*\.A(\d{7})\.(\d{4})\.', filename)
        if modis_with_time:
            date_str = modis_with_time.group(1)
            time_str = modis_with_time.group(2)

            year = int(date_str[:4])
            day_of_year = int(date_str[4:])
            hour = int(time_str[:2])
            minute = int(time_str[2:]) if len(time_str) >= 4 else 0

            date_obj = datetime(year, 1, 1) + timedelta(days=day_of_year - 1, hours=hour, minutes=minute)

        # 如果格式1沒匹配到，嘗試格式2 - 沒有時間信息的格式
        else:
            modis_without_time = re.search(r'(?:MOD|MYD|MCD)\w*\.A(\d{7})\.', filename)
            if modis_without_time:
                date_str = modis_without_time.group(1)

                year = int(date_str[:4])
                day_of_year = int(date_str[4:])

                # 沒有時間信息時，設為當天午夜
                date_obj = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)

    # YYYYMMDD 格式
    elif not date_obj:
        yyyymmdd_match = re.search(r'(\d{8})', filename)
        if yyyymmdd_match:
            date_str = yyyymmdd_match.group(1)
            date_obj = datetime.strptime(date_str, '%Y%m%d')

    # YYYYMMDD_HHMMSS 格式
    elif not date_obj:
        yyyymmdd_hhmmss_match = re.search(r'(\d{8}_\d{6})', filename)
        if yyyymmdd_hhmmss_match:
            date_str = yyyymmdd_hhmmss_match.group(1)
            date_obj = datetime.strptime(date_str, '%Y%m%d_%H%M%S')

    # ISO 日期格式 YYYY-MM-DD
    elif not date_obj:
        iso_date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        if iso_date_match:
            date_str = iso_date_match.group(1)
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')

    # ISO 日期時間格式 YYYY-MM-DDThh:mm:ss
    elif not date_obj:
        iso_datetime_match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})', filename)
        if iso_datetime_match:
            date_str = iso_datetime_match.group(1)
            date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')

    # 轉換時區（如果需要並且有找到日期）
    if to_local and date_obj:
        # 假設原始時間是 UTC
        utc_time = pytz.utc.localize(date_obj)
        # 轉換為本地時間
        local_time = utc_time.astimezone(pytz.timezone(local_tz))
        return local_time

    return date_obj