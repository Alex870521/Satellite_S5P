import re
import logging
from pathlib import Path
from datetime import datetime
import numpy as np
import imageio
import pytz
from PIL import Image
from src.utils.extract_datetime_from_filename import extract_datetime_from_filename


logger = logging.getLogger(__name__)


def animate_data(image_dir, output_path, date_type="auto", fps=2, resize=None, to_local=True, local_tz='Asia/Taipei',
                 **kwargs):
    """
    將圖片製作成 GIF 動畫，智能識別日期格式並排序

    Parameters:
    -----------
    image_dir : str or Path
        圖片資料夾路徑
    output_path : str or Path
        輸出檔案路徑
    date_type : str, optional
        日期類型，可選:
        - "auto": 自動嘗試所有支持的日期格式
        - "s5p": Sentinel-5P 衛星資料 (使用開始觀測時間排序)
        - "modis": MODIS 衛星資料 (使用年份和儒略日排序)
        - "yyyymmdd": 識別 YYYYMMDD 格式
        - "iso": 識別 YYYY-MM-DD 或 YYYY-MM-DDThh:mm:ss 格式
        - "custom": 使用自定義的 pattern 和 format
    fps : int
        每秒顯示幾張圖片
    resize : tuple
        調整圖片大小，例如 (800, 600)
    to_local : bool, optional
        是否將 UTC 時間轉換為本地時間，預設為 True
    local_tz : str, optional
        本地時區，預設為 'Asia/Taipei'
    custom_pattern : str, optional
        當 date_type="custom" 時使用的正則表達式模式
    custom_format : str, optional
        當 date_type="custom" 時使用的日期格式
    """
    # 確保路徑是 Path 對象
    image_dir = Path(image_dir)
    output_path = Path(output_path)
    # breakpoint()
    # 創建輸出目錄（如果不存在）
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 取得所有 PNG 圖片檔案
    image_files = [f for f in image_dir.glob('**/*.png') if not f.name.startswith('._')]

    if not image_files:
        logger.warning("沒有找到 PNG 圖片！")
        return

    # 處理自定義日期格式
    if date_type == "custom":
        custom_pattern = kwargs.get("custom_pattern", "")
        custom_format = kwargs.get("custom_format", "")
        if not custom_pattern or not custom_format:
            logger.warning("使用自定義模式時必須提供 custom_pattern 和 custom_format！")
            return

        # 創建自定義日期提取函數
        def get_custom_datetime(filename):
            match = re.search(custom_pattern, filename)
            if match:
                try:
                    date_str = match.group(1)
                    date_obj = datetime.strptime(date_str, custom_format)

                    # 轉換時區
                    if to_local:
                        utc_time = pytz.utc.localize(date_obj)
                        return utc_time.astimezone(pytz.timezone(local_tz))
                    return date_obj
                except (ValueError, IndexError):
                    pass
            return None

    # 根據日期類型選擇排序函數
    def get_sort_key(filepath):
        filename = filepath.name

        if date_type == "custom":
            date_obj = get_custom_datetime(filename)
        elif date_type == "s5p" and "S5P" in filename:
            date_obj = extract_datetime_from_filename(filename, to_local, local_tz)
        elif date_type == "modis" and any(prefix in filename for prefix in ["MOD", "MYD", "MCD"]):
            date_obj = extract_datetime_from_filename(filename, to_local, local_tz)
        elif date_type in ["yyyymmdd", "iso"]:
            date_obj = extract_datetime_from_filename(filename, to_local, local_tz)
        else:  # "auto"
            date_obj = extract_datetime_from_filename(filename, to_local, local_tz)

        return date_obj if date_obj else filename

    # 依照提取的信息排序
    image_files.sort(key=get_sort_key)

    # 如果找到了日期，顯示第一個檔案的日期格式
    if image_files and date_type == "auto":
        first_date = get_sort_key(image_files[0])
        if isinstance(first_date, datetime):
            tz_info = f" ({local_tz})" if to_local else " (UTC)"
            logger.info(f"自動檢測到日期: {first_date}{tz_info}")
        else:
            logger.warning("無法自動檢測日期格式，將使用檔名排序")

    # 準備圖片
    images = []
    for filepath in image_files:
        img = Image.open(filepath)
        if resize:
            img = img.resize(resize, Image.Resampling.LANCZOS)
        images.append(np.array(img))

    logger.info(f"找到 {len(images)} 張圖片")

    # 製作 GIF 動畫
    logger.info(f"正在製作 GIF 動畫... fps={fps}")
    imageio.mimsave(
        output_path,
        images,
        fps=fps,
        loop=0  # 0 表示無限循環
    )
    logger.info(f"動畫製作完成：{output_path}")

    return output_path