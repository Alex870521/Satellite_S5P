import imageio
from datetime import datetime
import re
from PIL import Image
import numpy as np
from src.config.settings import FIGURE_DIR
from src.config.catalog import TypeInput


def animate_data(file_type: TypeInput, start_date, end_date, fps=1, resize=None, **kwargs):
    """
    將 Sentinel 衛星圖片製作成 GIF 動畫

    Parameters:
    -----------
    image_dir : str or Path
        圖片資料夾路徑
    output_path : str or Path
        輸出檔案路徑
    fps : int
        每秒顯示幾張圖片
    resize : tuple
        調整圖片大小，例如 (800, 600)
    """
    image_dir = FIGURE_DIR / file_type
    output_path = FIGURE_DIR / file_type / 'sentinel_animation.gif'

    # 取得所有圖片檔案
    image_files = list(image_dir.glob('**/*OFFL*.png'))

    # 從檔名提取日期時間
    def get_datetime(filepath):
        match = re.search(r'(\d{8}T\d{6})', filepath.name)
        return datetime.strptime(match.group(1), '%Y%m%dT%H%M%S') if match else datetime.min

    # 依照日期時間排序
    image_files.sort(key=get_datetime)

    # 準備圖片
    images = []
    for filepath in image_files:
        img = Image.open(filepath)
        if resize:
            img = img.resize(resize, Image.Resampling.LANCZOS)
        images.append(np.array(img))

    print(f"找到 {len(images)} 張圖片")

    if not images:
        print("沒有找到符合條件的圖片！")
        return

    # 製作 GIF 動畫
    print(f"正在製作 GIF 動畫... fps={fps}")
    imageio.mimsave(
        output_path,
        images,
        fps=fps,
        loop=0  # 0 表示無限循環
    )
    print(f"動畫製作完成：{output_path}")
