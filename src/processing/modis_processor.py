import re
from datetime import datetime
from pathlib import Path
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import geopandas as gpd
import numpy as np
from matplotlib import pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.ticker import FixedLocator
from pyhdf.SD import SD, SDC
from src.config.settings import FIGURE_BOUNDARY, FILTER_BOUNDARY


plt.rcParams['mathtext.fontset'] = 'custom'
plt.rcParams['mathtext.rm'] = 'Times New Roman'
plt.rcParams['mathtext.it'] = 'Times New Roman: italic'
plt.rcParams['mathtext.bf'] = 'Times New Roman: bold'
plt.rcParams['mathtext.default'] = 'regular'
plt.rcParams['font.family'] = 'Times New Roman'
plt.rcParams['font.weight'] = 'normal'
plt.rcParams['font.size'] = 16

plt.rcParams['axes.titlesize'] = 'large'
plt.rcParams['axes.titleweight'] = 'bold'
plt.rcParams['axes.labelweight'] = 'bold'


class MODISProcessor:
    """處理 MODIS AOD 數據並生成可視化圖像"""

    def __init__(self, file_type):
        """初始化處理器"""
        self.raw_dir = None
        self.processed_dir = None
        self.figure_dir = None
        self.logger = None

        self.file_type = file_type

        # 預設參數
        self.filter_boundary = FILTER_BOUNDARY  # 過濾數據的邊界
        self.figure_boundary = FIGURE_BOUNDARY  # 圖像顯示的邊界

    def extract_date_from_filename(self, filename):
        """從文件名提取日期"""
        file_basename = Path(filename).name
        date_match = re.search(r'\.A(\d{7})\.', file_basename)

        if date_match:
            date_str = date_match.group(1)
            year = date_str[:4]
            day_of_year = date_str[4:7]
            try:
                file_date = datetime.strptime(f"{year}-{day_of_year}", "%Y-%j")
                return file_date, file_date.strftime("%Y-%m-%d")
            except ValueError:
                return None, file_basename
        else:
            return None, file_basename

    def process_hdf_file(self, hdf_file, save_individual=True):
        """處理單個 HDF 文件"""
        try:
            # 從文件名提取日期
            file_date, _ = self.extract_date_from_filename(hdf_file)
            self.logger.info(f"處理文件: {Path(hdf_file).name} ({file_date.strftime('%Y-%m-%d')})")

            # 打開 HDF 文件
            hdf = SD(str(hdf_file), SDC.READ)

            # 檢查可用的數據集
            datasets = hdf.datasets()

            # 嘗試獲取 AOD 數據集
            aod_name = 'Image_Optical_Depth_Land_And_Ocean'
            # aod_name = 'AOD_550_Dark_Target_Deep_Blue_Combined'

            if aod_name not in datasets:
                self.logger.info(f"  數據集 {aod_name} 未找到，嘗試 'Optical_Depth_Land_And_Ocean'")
                aod_name = 'Optical_Depth_Land_And_Ocean'

                if aod_name not in datasets:
                    self.logger.info(f"  數據集 {aod_name} 也未找到，跳過此文件")
                    return False

            # 獲取 AOD 數據
            aod_sds = hdf.select(aod_name)
            aod_data = aod_sds.get()
            aod_attrs = aod_sds.attributes()

            # 獲取地理位置數據
            latitude = hdf.select('Latitude').get()
            longitude = hdf.select('Longitude').get()

            # 確保經緯度和 AOD 數據的形狀一致
            if aod_data.shape != latitude.shape or aod_data.shape != longitude.shape:
                self.logger.info(f"  數據形狀不一致: AOD {aod_data.shape}, Lat {latitude.shape}, Lon {longitude.shape}")

                # 獲取最小的形狀維度
                min_shape = [min(dim) for dim in zip(aod_data.shape, latitude.shape, longitude.shape)]

                # 裁剪數據以匹配最小形狀
                if len(min_shape) == 2:
                    aod_data = aod_data[:min_shape[0], :min_shape[1]]
                    latitude = latitude[:min_shape[0], :min_shape[1]]
                    longitude = longitude[:min_shape[0], :min_shape[1]]
                    self.logger.info(f"  調整後的形狀: AOD {aod_data.shape}, Lat {latitude.shape}, Lon {longitude.shape}")
                else:
                    self.logger.warning(f"  無法調整數據形狀，跳過此文件")
                    return False

            # 從屬性獲取比例因子
            scale_factor = aod_attrs.get('scale_factor', 0.001)
            _FillValue = aod_attrs.get('_FillValue', -9999)
            # self.logger.info(f"  比例因子: {scale_factor.__round__(5)}, 填充值: {_FillValue}")

            # 應用比例因子並處理缺失值
            aod_data = aod_data.astype(float)
            aod_data[aod_data == _FillValue] = np.nan  # 將填充值設置為 NaN
            aod_data[aod_data < 0] = np.nan  # 將負值設置為 NaN

            # 應用比例因子到有效數據
            valid_mask = ~np.isnan(aod_data)
            if np.any(valid_mask):
                aod_data[valid_mask] = aod_data[valid_mask] * scale_factor

            # 使用 FILTER_BOUNDARY 按區域過濾
            taiwan_mask = ((longitude >= self.filter_boundary[0]) & (longitude <= self.filter_boundary[1]) &
                           (latitude >= self.filter_boundary[2]) & (latitude <= self.filter_boundary[3]))

            # 檢查過濾區域中是否有數據
            if np.sum(taiwan_mask & valid_mask) == 0:
                self.logger.debug(f"  該文件在過濾區域中沒有有效的 AOD 數據。")
                return False

            self.logger.debug(f"  在過濾區域中找到有效數據。")

            # 生成單獨的圖像
            if save_individual and file_date:
                # 創建與原始數據相同的目錄結構
                year_month_dir = file_date.strftime("%Y/%m")

                # 使用 MODIS AOD 作為圖像的基本名稱
                filename = f"{self.file_type}_MODIS_AOD_Taiwan_{file_date.strftime("%Y%m%d")}.png"

                satellite_name = "Terra" if self.file_type == "MOD04" else "Aqua" if self.file_type == "MYD04" else "Terra + Aqua"

                self._create_aod_map(aod_data, latitude, longitude,
                                     title=f'{satellite_name} AOD {file_date.strftime("%Y-%m-%d")}',
                                     output_file=filename,
                                     year_month_dir=year_month_dir)

            # 關閉 HDF 文件
            hdf.end()
            return True

        except Exception as e:
            self.logger.info(f"  處理文件時發生錯誤: {e}")
            return False

    def _create_aod_map(self, aod_data, latitude, longitude, title, output_file,
                        year_month_dir=None, vmin=0, vmax=1.0, colorbar_orientation='vertical'):
        """創建 AOD 地圖"""
        try:
            fig = plt.figure(figsize=(8, 8))
            ax = plt.axes(projection=ccrs.PlateCarree())

            # ax.coastlines(resolution='10m', color='black', linewidth=0.8)
            # ax.add_feature(cfeature.BORDERS, linestyle='-', linewidth=0.6)

            # 如果是台灣範圍且需要顯示測站
            # ax.add_feature(cfeature.COASTLINE.with_scale('10m'))

            # 讀取縣市和測站資料並添加縣市邊界
            taiwan_counties = gpd.read_file(
                Path(__file__).parents[2] / "data/shapefiles/taiwan/COUNTY_MOI_1090820.shp")
            ax.add_geometries(taiwan_counties['geometry'], crs=ccrs.PlateCarree(), edgecolor='black',
                              facecolor='none')

            # # 為沒有數據的區域添加灰色背景
            # ax.add_feature(cfeature.LAND, facecolor='#CCCCCC', zorder=0)
            # ax.add_feature(cfeature.OCEAN, facecolor='#CCCCCC', zorder=0)

            # 使用參考圖像中的顏色範圍
            colors = [(0.0, 0.0, 0.7), (0.0, 0.3, 1.0), (0.0, 0.7, 1.0),
                      (0.0, 1.0, 0.7), (0.7, 1.0, 0.0),
                      (1.0, 0.7, 0.0), (1.0, 0.3, 0.0), (0.7, 0.0, 0.0)]
            aod_cmap = LinearSegmentedColormap.from_list('aod_cmap', colors, N=256)

            # 為 AOD 數據創建蒙版數組
            aod_masked = np.ma.array(aod_data, mask=np.isnan(aod_data))

            # 為 AOD 數據創建 pcolormesh
            mesh = ax.pcolormesh(longitude, latitude, aod_masked,
                                 cmap=aod_cmap, vmin=vmin, vmax=vmax,
                                 transform=ccrs.PlateCarree())

            # 添加顏色條
            cbar = plt.colorbar(mesh, orientation=colorbar_orientation, pad=0.04, aspect=30)
            cbar.set_label('Aerosol Optical Depth (AOD)')

            # 使用 FIGURE_BOUNDARY 設置範圍
            ax.set_extent(self.figure_boundary, crs=ccrs.PlateCarree())

            # 設定網格線
            gl = ax.gridlines(draw_labels=True, linestyle='--', alpha=0.7)
            gl.top_labels = False
            gl.right_labels = False
            gl.xlocator = FixedLocator([119, 120, 121, 122, 123])  # 設定經度刻度

            # 添加標題
            plt.title(title, pad=20, fontdict={'weight': 'bold', 'fontsize': 24})

            # 構建保存路徑
            if year_month_dir:
                # 按照 file_type/YYYY/MM 結構保存圖像
                figure_subdir = self.figure_dir / self.file_type / year_month_dir
                figure_subdir.mkdir(parents=True, exist_ok=True)
                output_path = figure_subdir / output_file
            else:
                # 直接保存在 figure_dir
                output_path = self.figure_dir / output_file

            plt.tight_layout()
            plt.savefig(output_path, dpi=600)
            plt.close()

            self.logger.info(f"  已保存 AOD 地圖: {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"  創建地圖時發生錯誤: {e}")
            return False

    def find_hdf_files(self, pattern="**/*.hdf", start_date=None, end_date=None):
        """
        尋找所有符合條件的 HDF 文件，並根據日期範圍進行過濾

        Parameters:
            pattern (str): 文件匹配模式，默認為 "**/*.hdf"
            start_date (str or datetime): 過濾的開始日期，格式為 'YYYY-MM-DD' 或 datetime 對象
            end_date (str or datetime): 過濾的結束日期，格式為 'YYYY-MM-DD' 或 datetime 對象

        Returns:
            list: 符合條件的 HDF 文件列表
        """
        # 先查找符合模式的所有文件
        all_files = [f for f in self.raw_dir.glob(pattern) if not f.name.startswith("._")]

        # 如果沒有指定日期範圍，直接返回所有文件
        if not start_date and not end_date:
            return all_files

        # 轉換日期字符串為 datetime 對象
        if start_date and isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if end_date and isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")

        # 根據日期範圍過濾文件
        filtered_files = []
        for hdf_file in all_files:
            file_date, date_str = self.extract_date_from_filename(hdf_file)

            # 如果無法從文件名提取日期，跳過此文件
            if not file_date:
                self.logger.debug(f"無法從文件名提取日期: {hdf_file}")
                continue

            # 檢查文件日期是否在指定範圍內
            if start_date and file_date < start_date:
                continue
            if end_date and file_date > end_date:
                continue

            filtered_files.append(hdf_file)

        date_range_str = ""
        if start_date or end_date:
            date_range_str = f"(從 {start_date.strftime('%Y-%m-%d') if start_date else '最早'} 到 {end_date.strftime('%Y-%m-%d') if end_date else '最新'})"

        self.logger.info(f"找到 {len(filtered_files)} 個有效的 HDF 文件 {date_range_str}")
        return filtered_files

    def process_all_files(self, pattern="**/*.hdf", start_date=None, end_date=None):
        """
        處理日期範圍內的所有 HDF 文件並創建組合地圖

        Parameters:
            pattern (str): 文件匹配模式，默認為 "**/*.hdf"
            start_date (str or datetime): 處理的開始日期，格式為 'YYYY-MM-DD' 或 datetime 對象
            end_date (str or datetime): 處理的結束日期，格式為 'YYYY-MM-DD' 或 datetime 對象

        Returns:
            int: 成功處理的文件數量
        """
        # 尋找符合條件的文件（已根據日期範圍過濾）
        hdf_files = self.find_hdf_files(pattern, start_date, end_date)

        if not hdf_files:
            self.logger.info("沒有找到符合條件的 HDF 文件")
            return 0

        # 處理每個文件
        processed_count = 0
        for hdf_file in hdf_files:
            if self.process_hdf_file(hdf_file):
                processed_count += 1

        self.logger.info(f"處理完成! 成功處理 {processed_count} 個檔案。")
        return processed_count