from datetime import datetime
from pathlib import Path
from typing import Literal
import cartopy.crs as ccrs
import numpy as np
from matplotlib import pyplot as plt
from pyhdf.SD import SD, SDC

from src.config.catalog import PRODUCT_CONFIGS
from src.config.settings import FIGURE_BOUNDARY, FILTER_BOUNDARY
from src.visualization.plot_nc import plot_global_var, basic_map
from src.visualization.gif import animate_data
from src.utils.extract_datetime_from_filename import extract_datetime_from_filename


class MODISProcessor:
    """處理 MODIS AOD 數據並生成可視化圖像"""
    def __init__(self):
        """初始化處理器"""
        self.raw_dir = None
        self.processed_dir = None
        self.figure_dir = None
        self.logger = None
        self.file_type = None

        # 預設參數
        self.filter_boundary = FILTER_BOUNDARY  # 過濾數據的邊界
        self.figure_boundary = FIGURE_BOUNDARY  # 圖像顯示的邊界

    def process_hdf_file(self, hdf_file):
        """處理單個 HDF 文件"""
        try:
            # 從文件名提取日期
            file_date = extract_datetime_from_filename(hdf_file.name, to_local=False)
            self.logger.info(f"處理文件: {Path(hdf_file).name} ({file_date.strftime('%Y-%m-%d')})")

            # 打開 HDF 文件
            hdf = SD(str(hdf_file), SDC.READ)

            # 檢查可用的數據集
            datasets = hdf.datasets()

            # 判斷文件類型並選擇對應的處理方法
            if self._is_mcd19a2_file(hdf_file.name):
                return self._process_mcd19a2(hdf, hdf_file, file_date, datasets)
            else:
                return self._process_mod04_myd04(hdf, hdf_file, file_date, datasets)

        except Exception as e:
            self.logger.info(f"  處理文件時發生錯誤: {e}")
            return False

    def _is_mcd19a2_file(self, filename):
        """判斷是否為 MCD19A2 文件"""
        return 'MCD19A2' in filename

    def _process_mcd19a2(self, hdf, hdf_file, file_date, datasets):
        """處理 MCD19A2 文件"""
        try:
            self.logger.info("  處理 MCD19A2 (Level 3) 產品")

            # MCD19A2 的 AOD 數據集名稱
            aod_name = 'Optical_Depth_047'  # 或 'Optical_Depth_055' 根據需要

            if aod_name not in datasets:
                self.logger.info(f"  數據集 {aod_name} 未找到，嘗試 'Optical_Depth_055'")
                aod_name = 'Optical_Depth_055'

                if aod_name not in datasets:
                    self.logger.info(f"  找不到合適的 AOD 數據集，跳過此文件")
                    return False

            # 獲取 AOD 數據
            aod_sds = hdf.select(aod_name)
            aod_data = aod_sds.get()
            aod_attrs = aod_sds.attributes()

            self.logger.info(f"  AOD 數據形狀: {aod_data.shape}")

            # MCD19A2 是3D數據 (時間, Y, X)，通常取第一個時間層
            if len(aod_data.shape) == 3:
                # 取第一個時間層或平均值
                aod_data = aod_data[0, :, :]  # 或使用 np.nanmean(aod_data, axis=0)
                self.logger.info(f"  取第一個時間層，調整後形狀: {aod_data.shape}")

            # 生成地理座標 (MCD19A2 使用 sinusoidal 投影)
            latitude, longitude = self._generate_mcd19a2_coordinates(aod_data.shape, hdf_file.name)

            # 從屬性獲取比例因子
            scale_factor = aod_attrs.get('scale_factor', 0.0001)  # MCD19A2 通常用 0.0001
            _FillValue = aod_attrs.get('_FillValue', -28672)

            self.logger.info(f"  比例因子: {scale_factor}, 填充值: {_FillValue}")

            # 應用比例因子並處理缺失值
            aod_data = aod_data.astype(float)
            aod_data[aod_data == _FillValue] = np.nan
            aod_data[aod_data < 0] = np.nan

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

            # 生成圖像
            if file_date:
                savefig_path = self.figure_dir / 'MCD19A2' / file_date.strftime("%Y/%m") / f"{hdf_file.stem}.png"

                self._create_figures(aod_data, latitude, longitude,
                                     title=f'MODIS Combined AOD {file_date.strftime("%Y-%m-%d")}',
                                     savefig_path=savefig_path,
                                     map_scale='Taiwan',
                                     mark_stations=None)

            hdf.end()
            return True

        except Exception as e:
            self.logger.error(f"  處理 MCD19A2 文件時發生錯誤: {e}")
            return False

    def _process_mod04_myd04(self, hdf, hdf_file, file_date, datasets):
        """處理 MOD04_L2/MYD04_L2 文件（原有的處理邏輯）"""
        try:
            # 嘗試獲取 AOD 數據集
            aod_name = 'Image_Optical_Depth_Land_And_Ocean'

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
                    self.logger.info(
                        f"  調整後的形狀: AOD {aod_data.shape}, Lat {latitude.shape}, Lon {longitude.shape}")
                else:
                    self.logger.warning(f"  無法調整數據形狀，跳過此文件")
                    return False

            # 從屬性獲取比例因子
            scale_factor = aod_attrs.get('scale_factor', 0.001)
            _FillValue = aod_attrs.get('_FillValue', -9999)

            # 應用比例因子並處理缺失值
            aod_data = aod_data.astype(float)
            aod_data[aod_data == _FillValue] = np.nan
            aod_data[aod_data < 0] = np.nan

            # 應用比例因子到有效數據
            valid_mask = ~np.isnan(aod_data)
            if np.any(valid_mask):
                aod_data[valid_mask] = aod_data[valid_mask] * scale_factor

            # 使用 FILTER_BOUNDARY 按區域過濾
            taiwan_mask = ((longitude >= self.filter_boundary[0]) & (longitude <= self.filter_boundary[1]) &
                           (latitude >= self.filter_boundary[2]) & (latitude <= self.filter_boundary[3]))

            # 檢查過濾區域中是否有數據
            if np.sum(taiwan_mask & valid_mask) == 0:
                self.logger.info(f"  該文件在過濾區域中沒有有效的 AOD 數據。")
                return False

            self.logger.debug(f"  在過濾區域中找到有效數據。")

            # 生成單獨的圖像
            if file_date:
                satellite_name = "Terra" if self.file_type == "MOD04_L2" else "Aqua"
                savefig_path = self.figure_dir / self.file_type / file_date.strftime("%Y/%m") / f"{hdf_file.stem}.png"

                self._create_figures(aod_data, latitude, longitude,
                                     title=f'{satellite_name} AOD {file_date.strftime("%Y-%m-%d")}',
                                     savefig_path=savefig_path,
                                     map_scale='Taiwan',
                                     mark_stations=None)

            hdf.end()
            return True

        except Exception as e:
            self.logger.error(f"  處理 MOD04/MYD04 文件時發生錯誤: {e}")
            return False

    def _generate_mcd19a2_coordinates(self, data_shape, filename):
        """為 MCD19A2 數據生成地理座標"""
        try:
            # 從檔名提取 tile 信息 (例如 h29v06)
            import re
            tile_match = re.search(r'\.h(\d{2})v(\d{2})\.', filename)
            if not tile_match:
                raise ValueError("無法從檔名提取 tile 信息")

            h_tile = int(tile_match.group(1))
            v_tile = int(tile_match.group(2))

            # MODIS sinusoidal 投影的正確參數
            # 地球半徑 (meters)
            R = 6371007.181
            # 每個像素的大小 (meters) - 1km產品
            pixel_size = 926.625433056
            # 每個 tile 的像素數
            tile_size = 1200

            rows, cols = data_shape

            # MODIS tile 系統的起始座標 (sinusoidal projection)
            # 水平方向：從 -20015109.354 到 20015109.354 (共36個 tiles)
            # 垂直方向：從 -10007554.677 到 10007554.677 (共18個 tiles)

            # 計算 tile 的 sinusoidal 座標範圍
            h_min = -20015109.354 + h_tile * tile_size * pixel_size
            h_max = h_min + tile_size * pixel_size
            v_max = 10007554.677 - v_tile * tile_size * pixel_size
            v_min = v_max - tile_size * pixel_size

            # 生成 sinusoidal 座標網格
            x_coords = np.linspace(h_min, h_max, cols)
            y_coords = np.linspace(v_max, v_min, rows)
            X, Y = np.meshgrid(x_coords, y_coords)

            # 轉換為經緯度
            # Sinusoidal 投影的逆轉換
            longitude = X / (R * np.cos(Y / R)) * 180 / np.pi
            latitude = Y / R * 180 / np.pi

            # 處理極地區域的特殊情況
            latitude = np.clip(latitude, -90, 90)
            longitude = np.clip(longitude, -180, 180)

            # 檢查台灣區域的座標範圍
            taiwan_mask = ((longitude >= 119.0) & (longitude <= 123.0) &
                           (latitude >= 21.0) & (latitude <= 26.0))

            if np.any(taiwan_mask):
                lon_range = longitude[taiwan_mask]
                lat_range = latitude[taiwan_mask]
                self.logger.info(f"  台灣區域座標範圍: 經度 {lon_range.min():.2f} 到 {lon_range.max():.2f}, "
                                 f"緯度 {lat_range.min():.2f} 到 {lat_range.max():.2f}")
            else:
                self.logger.warning("  此 tile 不包含台灣區域")

            self.logger.info(f"  Tile h{h_tile:02d}v{v_tile:02d} 整體座標範圍: "
                             f"經度 {longitude.min():.2f} 到 {longitude.max():.2f}, "
                             f"緯度 {latitude.min():.2f} 到 {latitude.max():.2f}")

            return latitude, longitude

        except Exception as e:
            self.logger.error(f"  生成 MCD19A2 座標時發生錯誤: {e}")
            # 如果無法生成座標，返回簡單的索引網格
            rows, cols = data_shape
            y_indices, x_indices = np.mgrid[0:rows, 0:cols]
            return y_indices.astype(float), x_indices.astype(float)

    def _create_figures(self, aod_data, latitude, longitude, title,
                        savefig_path=None,
                        map_scale: Literal['global', 'East_Asia', 'Taiwan'] = 'global',
                        mark_stations: list | None = ['古亭', '忠明', '楠梓', '鳳山'],
                        ):
        """創建 AOD 地圖"""
        try:
            fig = plt.figure(figsize=(12, 8) if map_scale == 'global' else (8, 8), dpi=300)
            ax = plt.axes(projection=ccrs.PlateCarree())

            ax = basic_map(ax, map_scale=map_scale, mark_stations=mark_stations)

            # 為 AOD 數據創建蒙版數組
            aod_masked = np.ma.array(aod_data, mask=np.isnan(aod_data))

            # 為 AOD 數據創建 pcolormesh
            mesh = ax.pcolormesh(longitude, latitude, aod_masked,
                                 cmap='jet', vmin=0, vmax=1,
                                 transform=ccrs.PlateCarree())

            # 添加顏色條
            cbar = plt.colorbar(mesh, orientation='vertical', pad=0.04, aspect=30)
            cbar.set_label('Aerosol Optical Depth (AOD)', labelpad=10)

            # 添加標題
            datetime_str = extract_datetime_from_filename(savefig_path.name)
            plt.title(datetime_str, pad=20, fontdict={'weight': 'bold', 'fontsize': 24})

            plt.tight_layout()
            plt.savefig(savefig_path, dpi=600)
            plt.close()

            self.logger.info(f"  已保存 AOD 地圖: {savefig_path}")
            return True

        except Exception as e:
            self.logger.error(f"  創建地圖時發生錯誤: {e}")
            return False

    def process_all_files(self, pattern=None, start_date=None, end_date=None):
        """
        處理日期範圍內的所有 HDF 文件

        Parameters:
            pattern (str): 文件匹配模式，默認為 "**/*.hdf"
            start_date (str or datetime): 處理的開始日期，格式為 'YYYY-MM-DD' 或 datetime 對象
            end_date (str or datetime): 處理的結束日期，格式為 'YYYY-MM-DD' 或 datetime 對象

        Returns:
            bool: 處理是否成功
        """
        # 設置默認值和進行類型轉換
        if pattern is None:
            if hasattr(self, 'file_class') and self.file_class:
                pattern = f"**/{self.file_type}/**/*{self.file_type}*.hdf"
            else:
                pattern = f"**/{self.file_type}/**/*.hdf"

        self.pattern = pattern

        # 處理日期格式：接受字符串或datetime對象
        if isinstance(start_date, str):
            self.start = datetime.strptime(start_date, '%Y-%m-%d')
        else:
            self.start = start_date

        if isinstance(end_date, str):
            self.end = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            self.end = end_date

        # 找到所有符合條件的文件
        self.logger.info(f"尋找所有符合條件的衛星數據文件，模式: {self.pattern}")
        all_files = [f for f in self.raw_dir.glob(pattern) if not f.name.startswith("._") and f.is_file()]

        # 根據日期範圍過濾文件
        filtered_files = []
        for file_path in all_files:
            file_date = extract_datetime_from_filename(file_path.name, to_local=False)

            # 如果無法從文件名提取日期，跳過此文件
            if not file_date:
                self.logger.debug(f"無法從文件名提取日期: {file_path}")
                continue

            # 檢查文件日期是否在指定範圍內
            if self.start and file_date < self.start:
                continue
            if self.end and file_date > self.end:
                continue

            filtered_files.append(file_path)

        date_range_str = ""
        if self.start or self.end:
            date_range_str = f"(從 {self.start.strftime('%Y-%m-%d') if self.start else '最早'} 到 {self.end.strftime('%Y-%m-%d') if self.end else '最新'})"

        self.logger.info(f"找到 {len(filtered_files)} 個有效的衛星數據文件 {date_range_str}")

        if not filtered_files:
            self.logger.info("沒有找到符合條件的 HDF 文件")
            return 0

        # 按年月組織文件
        files_by_month = {}
        for file_path in filtered_files:
            file_date = extract_datetime_from_filename(file_path.name, to_local=False)
            year_month = file_date.strftime('%Y-%m')

            if year_month not in files_by_month:
                files_by_month[year_month] = []

            files_by_month[year_month].append(file_path)

        # 處理每個月的文件
        processed_count = 0
        for year_month, month_files in files_by_month.items():
            year, month = year_month.split('-')

            # 設置目錄
            paths = {
                'input': self.raw_dir / self.file_type / year / month,
                # 'output': self.processed_dir / self.file_type / year / month,
                'figure': self.figure_dir / self.file_type / year / month,
            }

            # 創建目錄
            for dir_path in paths.values():
                if dir_path != paths['input']:  # 不創建輸入目錄
                    dir_path.mkdir(parents=True, exist_ok=True)

            # 處理該月的所有文件
            month_processed = 0
            self.logger.info(f"處理 {year}-{month} 的 {len(month_files)} 個文件")

            for hdf_file in month_files:
                try:
                    result = self.process_hdf_file(hdf_file)
                    if result:
                        # TODO: 整合 _create_figures and plot_global_var
                        # 文件處理成功後立即創建圖像
                        # try:
                        #     output_file = paths['output'] / hdf_file.name
                        #     figure_path = paths['figure'] / f"{hdf_file.stem}.png"
                        #
                        #     self.logger.info(f"正在創建圖像: {figure_path}")
                        #
                        #     plot_global_var(
                        #         dataset=output_file,
                        #         product_params=PRODUCT_CONFIGS[self.file_type],
                        #         savefig_path=figure_path,
                        #         map_scale='Taiwan',
                        #         mark_stations=None
                        #     )
                        # except Exception as e:
                        #     self.logger.error(f"繪製檔案 {hdf_file.name} 時發生錯誤: {e}")

                        month_processed += 1
                        processed_count += 1
                except Exception as e:
                    self.logger.error(f"處理檔案 {hdf_file.name} 時發生錯誤: {e}")

            if month_processed > 0:
                try:
                    # 創建動畫
                    animation_path = paths['figure'] / f"{self.file_type}_{year}{month}_animation.gif"

                    self.logger.info(f"創建動畫: {animation_path}")

                    animate_data(
                        image_dir=paths['figure'],
                        output_path=animation_path,
                        date_type="modis",
                        fps=2
                    )
                except Exception as e:
                    self.logger.error(f"創建 {year}-{month} 的動畫時發生錯誤: {e}")

        self.logger.info(f"處理完成! 成功處理 {processed_count} 個檔案，共 {len(files_by_month)} 個月。")
        return processed_count > 0