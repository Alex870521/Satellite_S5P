import re
import shutil
import earthaccess

from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from src.config.richer import console, rich_print, DisplayManager
from src.config.settings import MODIS_RAW_DATA_DIR


# 加載環境變數
load_dotenv()

# 地理範圍設定
SEARCH_BOUNDARY = (119.0, 21.0, 123.0, 26.0)  # 搜索數據的邊界 (west_lon, south_lat, east_lon, north_lat)


class EARTHDATAFetcher:
    def __init__(self):
        # 使用 earthaccess 登入
        self.auth = earthaccess.login(strategy="environment")

        # 配置下載數據
        self.download_dir = MODIS_RAW_DATA_DIR

    async def fetch_data(self, file_type, start_date: datetime, end_date: datetime, boundary: tuple = SEARCH_BOUNDARY):
        self.file_type = file_type

        products = earthaccess.search_data(
            short_name=f"{file_type}_L2",
            temporal=(f'{start_date}T00:00:00.000Z', f'{end_date}T23:59:59.999Z'),
            bounding_box=boundary,
        )

        # 進一步過濾結果，排除 NRT 文件
        filtered_products = []
        for product in products:
            # 檢查是否有下載連結
            if not hasattr(product, 'data_links') or not callable(getattr(product, 'data_links')):
                continue

            links = product.data_links()
            if not links:
                continue

            # 檢查文件名是否包含 .NRT.hdf
            filename = Path(links[0]).name
            if '.NRT.hdf' not in filename:
                filtered_products.append(product)

        # Display product info
        if filtered_products:
            DisplayManager().display_products_nasa(filtered_products)
            return filtered_products
        else:
            print("No valid products found (NRT files excluded)")
            return []

    def download(self, products):
        if not products:
            print("沒有找到符合條件的數據")
            return []

        # 用於跟踪下載的文件
        downloaded_files = []

        # 檢查哪些文件需要下載
        for result in products:
            try:
                # 獲取文件名和下載鏈接
                if not result.data_links():
                    continue

                file_url = result.data_links()[0]
                filename = Path(file_url).name

                # 跳過 NRT 文件
                if '.NRT.hdf' in filename:
                    print(f"跳過 NRT 文件: {filename}")
                    continue

                # 從檔案名稱提取日期信息
                date_match = re.search(r'\.A(\d{7})\.', filename)
                if not date_match:
                    print(f"無法從文件名提取日期: {filename}")
                    continue

                date_str = date_match.group(1)
                year = date_str[:4]
                day_of_year = int(date_str[4:7])

                # 將日期轉換為年月
                file_date = datetime.strptime(f"{year}-{day_of_year}", "%Y-%j")
                year_month_dir = file_date.strftime("%Y/%m")

                # 創建目標目錄
                target_dir = self.download_dir / self.file_type / year_month_dir
                target_dir.mkdir(parents=True, exist_ok=True)

                # 檢查文件是否已存在
                target_file = target_dir / filename
                if target_file.exists():
                    # print(f"檔案已存在: {target_file}")
                    downloaded_files.append(str(target_file))
                    continue

                # 下載單個文件
                # print(f"下載文件: {filename} 到 {target_dir}")

                # 使用 earthaccess 下載到臨時位置
                temp_files = earthaccess.download([result], self.download_dir / "temp")

                # 如果下載成功，移動到目標位置
                if temp_files and len(temp_files) > 0:
                    temp_file = Path(temp_files[0])
                    if temp_file.exists():
                        # 確保目標目錄存在
                        target_dir.mkdir(parents=True, exist_ok=True)

                        # 移動文件到正確的目錄
                        temp_file.rename(target_file)
                        # print(f"成功下載: {target_file}")
                        downloaded_files.append(str(target_file))
                    else:
                        print(f"下載失敗: {filename}")
                else:
                    print(f"下載失敗: {filename}")

            except Exception as e:
                print(f"處理文件時發生錯誤: {str(e)}")

        # 刪除臨時目錄
        temp_dir = self.download_dir / "temp"
        if temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
            except Exception:
                pass

        if not downloaded_files:
            print("所有檔案已經存在，無需下載")
        else:
            print(f"成功下載 {len(downloaded_files)} 個檔案")

        return downloaded_files
