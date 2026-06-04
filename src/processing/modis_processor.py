from datetime import datetime
from pathlib import Path
from typing import Literal, List, Dict, Optional, Tuple
import cartopy.crs as ccrs
import numpy as np
import xarray as xr
from matplotlib import pyplot as plt
import pandas as pd

from src.config.catalog import PRODUCT_CONFIGS
from src.config.settings import FIGURE_BOUNDARY, FILTER_BOUNDARY
from src.visualization.plot_nc import plot_global_var, basic_map
from src.visualization.gif import animate_data
from src.utils.extract_datetime_from_filename import extract_datetime_from_filename


class MODISProcessor:
    """處理 MODIS AOD 數據並生成可視化圖像"""
    def __init__(self, aod_variable: str = 'AOD_550_Dark_Target_Deep_Blue_Combined'):
        """
        初始化處理器
        
        Parameters:
        -----------
        aod_variable : str
            MOD04/MYD04 使用的 AOD 變量名稱
            可選項:
            - 'AOD_550_Dark_Target_Deep_Blue_Combined' (推薦，最高質量，DT+DB合併)
            - 'Optical_Depth_Land_And_Ocean' (舊版，兼容性好)
            - 'Image_Optical_Depth_Land_And_Ocean' (繪圖用，低解析度)
        """
        self.raw_dir = None
        self.processed_dir = None
        self.figure_dir = None
        self.logger = None
        self.file_type = None
        self.aod_variable = aod_variable  # 新增：可配置的 AOD 變量名稱

        # 預設參數
        self.filter_boundary = FILTER_BOUNDARY  # 過濾數據的邊界
        self.figure_boundary = FIGURE_BOUNDARY  # 圖像顯示的邊界

    def process_hdf_file(self, hdf_file):
        """處理單個 HDF 文件 - 跨平台版本"""
        try:
            # 從文件名提取日期
            file_date = extract_datetime_from_filename(hdf_file.name, to_local=False)
            self.logger.info(f"處理文件: {Path(hdf_file).name} ({file_date.strftime('%Y-%m-%d')})")

            # 讀取原始 HDF4 一律走 pyhdf（唯一的 HDF4 入口；見 hdf4_to_netcdf）
            hdf_obj = self._open_with_pyhdf(hdf_file)
            datasets = hdf_obj.datasets() if hdf_obj else {}

            if not hdf_obj:
                self.logger.error(f"無法打開文件: {hdf_file}")
                return False

            # 判斷文件類型並選擇對應的處理方法
            if self._is_mcd19a2_file(hdf_file.name):
                return self._process_mcd19a2(hdf_obj, hdf_file, file_date, datasets)
            else:
                return self._process_mod04_myd04(hdf_obj, hdf_file, file_date, datasets)

        except Exception as e:
            self.logger.error(f"處理文件時發生錯誤: {e}")
            return False

    def process_nc_file(self, nc_file):
        """從轉檔後的 NetCDF 生成圖像（下游：僅用 xarray，不需 pyhdf）。

        nc_file 由 hdf4_to_netcdf() 產生，已是解碼後的 AOD + 2D lat/lon，
        故此處只負責讀取、區域過濾與出圖，不再套用 scale_factor / fill 值。
        """
        try:
            nc_file = Path(nc_file)
            file_date = extract_datetime_from_filename(nc_file.name, to_local=False)
            self.logger.info(f"處理文件: {nc_file.name} ({file_date.strftime('%Y-%m-%d')})")

            with xr.open_dataset(nc_file) as ds:
                aod_data = ds["aod"].values
                latitude = ds["latitude"].values
                longitude = ds["longitude"].values

            if not self._filter_and_validate_data(aod_data, latitude, longitude):
                return False

            if self._is_mcd19a2_file(nc_file.name):
                self._generate_mcd19a2_figure(aod_data, latitude, longitude, file_date, nc_file)
            else:
                self._generate_mod04_figure(aod_data, latitude, longitude, file_date, nc_file)
            return True

        except Exception as e:
            self.logger.error(f"處理 NetCDF 文件時發生錯誤: {e}")
            return False

    def _open_with_pyhdf(self, hdf_file):
        """以 pyhdf 開啟原始 HDF4 檔（讀 HDF4 的唯一途徑，需要 [ingest] extra）"""
        try:
            from pyhdf.SD import SD, SDC
            hdf = SD(str(hdf_file), SDC.READ)
            return hdf
        except ImportError:
            import sys
            msg = (
                "讀取原始 HDF4 需要 pyhdf，請安裝 ingest extra："
                "pip install 's5p-processor[ingest]'（或 conda install -c conda-forge pyhdf）"
            )
            if sys.version_info >= (3, 14):
                msg += (
                    "\n⚠️ Python 3.14 目前沒有 pyhdf wheel（含 Windows win_amd64），"
                    "pip 會嘗試從源碼編譯並因缺少 HDF4 標頭而失敗。"
                    "請改用 Python 3.12 / 3.13 執行轉檔（ingest）；"
                    "Python 3.14 僅用於下游 NetCDF 分析即可。"
                )
            self.logger.error(msg)
            return None
        except Exception as e:
            self.logger.error(f"pyhdf 打開文件失敗: {e}")
            return None

    def _is_mcd19a2_file(self, filename):
        """判斷是否為 MCD19A2 文件"""
        return 'MCD19A2' in filename

    def hdf4_to_netcdf(self, hdf_file, out_dir=None):
        """逐檔 1:1 無損轉換：單一 MODIS HDF4 → NetCDF。

        這是整個套件中唯一讀取原始 HDF4（依賴 pyhdf / [ingest] extra）的入口。
        轉出的 .nc 保留原始 swath 解析度（AOD + 2D lat/lon，不重投影、不合併），
        下游一律只讀 NetCDF（xarray/netCDF4，全平台一致、支援 Py3.14）。

        在有 pyhdf 的機器（macOS / 遠端 / conda）執行 ingest；分析端不需 pyhdf。

        Args:
            hdf_file: 原始 .hdf 路徑
            out_dir: 輸出資料夾，預設與來源同目錄
        Returns:
            產生的 .nc 路徑；無有效資料或無法開檔時回傳 None
        """
        hdf_file = Path(hdf_file)
        hdf_obj = self._open_with_pyhdf(hdf_file)
        if not hdf_obj:
            return None

        try:
            datasets = hdf_obj.datasets()
            if self._is_mcd19a2_file(hdf_file.name):
                product = "MCD19A2"
                aod, lat, lon = self._extract_mcd19a2_data(hdf_obj, datasets, hdf_file.name)
            else:
                product = self.file_type or "MOD04_L2"
                aod, lat, lon = self._extract_mod04_data(hdf_obj, datasets)
        finally:
            self._close_hdf_file(hdf_obj)

        if aod is None or lat is None or lon is None:
            self.logger.warning(f"無有效資料，略過轉檔: {hdf_file.name}")
            return None

        out_dir = Path(out_dir) if out_dir else hdf_file.parent
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{hdf_file.stem}.nc"

        ds = xr.Dataset(
            {"aod": (("y", "x"), np.asarray(aod, dtype="float32"))},
            coords={
                "latitude": (("y", "x"), np.asarray(lat, dtype="float32")),
                "longitude": (("y", "x"), np.asarray(lon, dtype="float32")),
            },
            attrs={
                "source_file": hdf_file.name,
                "product": product,
                "aod_variable": self.aod_variable,
                "conversion": "1:1 lossless HDF4->NetCDF (swath preserved, no regridding)",
            },
        )
        ds["aod"].attrs["long_name"] = "Aerosol Optical Depth"
        encoding = {"aod": {"zlib": True, "complevel": 4, "_FillValue": np.nan}}
        ds.to_netcdf(out_path, encoding=encoding)
        self.logger.info(f"轉檔完成: {hdf_file.name} -> {out_path.name}")
        return out_path

    def _process_mcd19a2(self, hdf_obj, hdf_file, file_date, datasets):
        """處理 MCD19A2 文件 - 跨平台版本"""
        try:
            self.logger.info("  處理 MCD19A2 (Level 3) 產品")

            # MCD19A2 的 AOD 數據集名稱
            aod_name = 'Optical_Depth_047'
            if aod_name not in datasets:
                self.logger.info(f"  數據集 {aod_name} 未找到，嘗試 'Optical_Depth_055'")
                aod_name = 'Optical_Depth_055'
                if aod_name not in datasets:
                    self.logger.info(f"  找不到合適的 AOD 數據集，跳過此文件")
                    return False

            aod_data, aod_attrs = self._get_data_pyhdf(hdf_obj, aod_name)

            if aod_data is None:
                return False

            self.logger.info(f"  AOD 數據形狀: {aod_data.shape}")

            # MCD19A2 是3D數據，取第一個時間層
            if len(aod_data.shape) == 3:
                aod_data = aod_data[0, :, :]
                self.logger.info(f"  取第一個時間層，調整後形狀: {aod_data.shape}")

            # 生成地理座標
            latitude, longitude = self._generate_mcd19a2_coordinates(aod_data.shape, hdf_file.name)

            # 處理數據
            scale_factor = aod_attrs.get('scale_factor', 0.0001)
            _FillValue = aod_attrs.get('_FillValue', -28672)

            aod_data = self._process_aod_data(aod_data, scale_factor, _FillValue)

            # 區域過濾和圖像生成
            if self._filter_and_validate_data(aod_data, latitude, longitude):
                self._generate_mcd19a2_figure(aod_data, latitude, longitude, file_date, hdf_file)

            # 關閉文件
            self._close_hdf_file(hdf_obj)
            return True

        except Exception as e:
            self.logger.error(f"  處理 MCD19A2 文件時發生錯誤: {e}")
            return False

    def _process_mod04_myd04(self, hdf_obj, hdf_file, file_date, datasets):
        """處理 MOD04_L2/MYD04_L2 文件 - 跨平台版本"""
        try:
            # 使用配置的 AOD 變量名稱，並提供備用選項
            aod_name = self.aod_variable
            fallback_options = [
                'AOD_550_Dark_Target_Deep_Blue_Combined',
                'Optical_Depth_Land_And_Ocean',
                'Image_Optical_Depth_Land_And_Ocean'
            ]
            
            if aod_name not in datasets:
                self.logger.info(f"  數據集 {aod_name} 未找到，嘗試備用選項...")
                for fallback in fallback_options:
                    if fallback != aod_name and fallback in datasets:
                        aod_name = fallback
                        self.logger.info(f"  使用備用數據集: {aod_name}")
                        break
                else:
                    self.logger.info(f"  找不到任何可用的 AOD 數據集，跳過此文件")
                    self.logger.info(f"  可用數據集: {list(datasets)[:10]}...")  # 顯示前10個
                    return False

            aod_data, aod_attrs = self._get_data_pyhdf(hdf_obj, aod_name)
            latitude = self._get_data_pyhdf(hdf_obj, 'Latitude')[0]
            longitude = self._get_data_pyhdf(hdf_obj, 'Longitude')[0]

            if aod_data is None:
                return False

            # 確保數據形狀一致
            aod_data, latitude, longitude = self._align_data_shapes(aod_data, latitude, longitude)

            # 處理數據
            scale_factor = aod_attrs.get('scale_factor', 0.001)
            _FillValue = aod_attrs.get('_FillValue', -9999)

            aod_data = self._process_aod_data(aod_data, scale_factor, _FillValue)

            # 區域過濾和圖像生成
            if self._filter_and_validate_data(aod_data, latitude, longitude):
                self._generate_mod04_figure(aod_data, latitude, longitude, file_date, hdf_file)

            # 關閉文件
            self._close_hdf_file(hdf_obj)
            return True

        except Exception as e:
            self.logger.error(f"  處理 MOD04/MYD04 文件時發生錯誤: {e}")
            return False

    def _get_data_pyhdf(self, hdf_obj, dataset_name):
        """pyhdf: 獲取數據和屬性"""
        try:
            sds = hdf_obj.select(dataset_name)
            data = sds.get()
            attrs = sds.attributes()
            sds.endaccess()
            return data, attrs
        except Exception as e:
            self.logger.error(f"pyhdf 讀取 {dataset_name} 失敗: {e}")
            return None, {}

    def _process_aod_data(self, aod_data, scale_factor, fill_value):
        """處理 AOD 數據：應用比例因子和處理缺失值"""
        aod_data = aod_data.astype(float)
        aod_data[aod_data == fill_value] = np.nan
        aod_data[aod_data < 0] = np.nan

        valid_mask = ~np.isnan(aod_data)
        if np.any(valid_mask):
            aod_data[valid_mask] = aod_data[valid_mask] * scale_factor

        return aod_data

    def _align_data_shapes(self, aod_data, latitude, longitude):
        """確保 AOD、經度、緯度數據形狀一致"""
        if aod_data.shape != latitude.shape or aod_data.shape != longitude.shape:
            self.logger.info(f"  數據形狀不一致: AOD {aod_data.shape}, Lat {latitude.shape}, Lon {longitude.shape}")

            min_shape = [min(dim) for dim in zip(aod_data.shape, latitude.shape, longitude.shape)]

            if len(min_shape) == 2:
                aod_data = aod_data[:min_shape[0], :min_shape[1]]
                latitude = latitude[:min_shape[0], :min_shape[1]]
                longitude = longitude[:min_shape[0], :min_shape[1]]
                self.logger.info(f"  調整後的形狀: AOD {aod_data.shape}, Lat {latitude.shape}, Lon {longitude.shape}")
            else:
                self.logger.warning(f"  無法調整數據形狀")
                return None, None, None

        return aod_data, latitude, longitude

    def _filter_and_validate_data(self, aod_data, latitude, longitude):
        """使用邊界過濾數據並驗證"""
        taiwan_mask = ((longitude >= self.filter_boundary[0]) & (longitude <= self.filter_boundary[1]) &
                       (latitude >= self.filter_boundary[2]) & (latitude <= self.filter_boundary[3]))

        valid_mask = ~np.isnan(aod_data)

        if np.sum(taiwan_mask & valid_mask) == 0:
            self.logger.info(f"  該文件在過濾區域中沒有有效的 AOD 數據。")
            return False

        self.logger.debug(f"  在過濾區域中找到有效數據。")
        return True

    def _generate_mcd19a2_figure(self, aod_data, latitude, longitude, file_date, hdf_file):
        """生成 MCD19A2 圖像"""
        if file_date:
            savefig_path = self.figure_dir / 'MCD19A2' / file_date.strftime("%Y/%m") / f"{hdf_file.stem}.png"
            self._create_figures(aod_data, latitude, longitude,
                                 title=f'MODIS Combined AOD {file_date.strftime("%Y-%m-%d")}',
                                 savefig_path=savefig_path,
                                 map_scale='Taiwan',
                                 mark_stations=None)

    def _generate_mod04_figure(self, aod_data, latitude, longitude, file_date, hdf_file):
        """生成 MOD04/MYD04 圖像"""
        if file_date:
            satellite_name = "Terra" if self.file_type == "MOD04_L2" else "Aqua"
            savefig_path = self.figure_dir / self.file_type / file_date.strftime("%Y/%m") / f"{hdf_file.stem}.png"
            self._create_figures(aod_data, latitude, longitude,
                                 title=f'{satellite_name} AOD {file_date.strftime("%Y-%m-%d")}',
                                 savefig_path=savefig_path,
                                 map_scale='Taiwan',
                                 mark_stations=None)

    def _close_hdf_file(self, hdf_obj):
        """關閉 pyhdf 開啟的 HDF4 檔案"""
        try:
            hdf_obj.end()
        except Exception as e:
            self.logger.debug(f"關閉文件時出錯: {e}")

    def _generate_mcd19a2_coordinates(self, data_shape, filename):
        """為 MCD19A2 數據生成地理座標"""
        try:
            # 從檔名提取 tile 信息 (例如 h29v06)
            import re
            
            # 嘗試多種可能的 tile 格式
            tile_patterns = [
                r'\.h(\d{2})v(\d{2})\.',  # 標準格式 .h29v06.
                r'h(\d{2})v(\d{2})',      # 無點格式 h29v06
                r'_h(\d{2})v(\d{2})_',    # 下劃線格式 _h29v06_
                r'h(\d{2})v(\d{2})\.',    # 無前點格式 h29v06.
            ]
            
            h_tile = None
            v_tile = None
            
            for pattern in tile_patterns:
                tile_match = re.search(pattern, filename)
                if tile_match:
                    h_tile = int(tile_match.group(1))
                    v_tile = int(tile_match.group(2))
                    self.logger.debug(f"從文件名 '{filename}' 提取到 tile: h{h_tile:02d}v{v_tile:02d}")
                    break
            
            if h_tile is None or v_tile is None:
                # 如果無法提取 tile 信息，使用默認值（台灣附近的 tile）
                self.logger.warning(f"無法從文件名 '{filename}' 提取 tile 信息，使用默認 tile h29v06 (台灣區域)")
                h_tile = 29
                v_tile = 6

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
            fig = plt.figure(figsize=(12, 8) if map_scale == 'global' else (8, 8))
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
            savefig_path.parent.mkdir(parents=True, exist_ok=True)
            plt.savefig(savefig_path)
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

        # 按時間順序排序文件
        filtered_files.sort(key=lambda f: extract_datetime_from_filename(f.name, to_local=False))
        self.logger.info(f"文件已按時間順序排序")

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
                'nc': self.processed_dir / self.file_type / year / month,
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
                    # Ingest：原始 HDF4 → NetCDF（唯一需要 pyhdf 的步驟）
                    nc_file = self.hdf4_to_netcdf(hdf_file, out_dir=paths['nc'])
                    if not nc_file:
                        continue

                    # 下游：一律從 NetCDF 出圖（xarray，不需 pyhdf）
                    if self.process_nc_file(nc_file):
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

    def merge_hdf_files_to_netcdf(self, 
                                 pattern: str = None,
                                 start_date: Optional[str] = None, 
                                 end_date: Optional[str] = None,
                                 output_filename: Optional[str] = None,
                                 merge_by_month: bool = True) -> bool:
        """
        將多個 HDF 文件合併成一個 NetCDF 文件
        
        Parameters:
            pattern (str): 文件匹配模式，默認為 "**/*.hdf"
            start_date (str): 處理的開始日期，格式為 'YYYY-MM-DD'
            end_date (str): 處理的結束日期，格式為 'YYYY-MM-DD'
            output_filename (str): 輸出的 NetCDF 文件名，如果為 None 則自動生成
            merge_by_month (bool): 是否按月份分別合併，True 為每月一個文件，False 為全部合併
            
        Returns:
            bool: 合併是否成功
        """
        try:
            # 設置默認值
            if pattern is None:
                if hasattr(self, 'file_class') and self.file_class:
                    pattern = f"**/{self.file_type}/**/*{self.file_type}*.hdf"
                else:
                    pattern = f"**/{self.file_type}/**/*.hdf"

            # 處理日期格式
            start_dt = datetime.strptime(start_date, '%Y-%m-%d') if start_date else None
            end_dt = datetime.strptime(end_date, '%Y-%m-%d') if end_date else None

            # 找到所有符合條件的文件
            self.logger.info(f"尋找所有符合條件的衛星數據文件，模式: {pattern}")
            all_files = [f for f in self.raw_dir.glob(pattern) if not f.name.startswith("._") and f.is_file()]

            # 根據日期範圍過濾文件
            filtered_files = []
            for file_path in all_files:
                file_date = extract_datetime_from_filename(file_path.name, to_local=False)
                if not file_date:
                    self.logger.debug(f"無法從文件名提取日期: {file_path}")
                    continue
                if start_dt and file_date < start_dt:
                    continue
                if end_dt and file_date > end_dt:
                    continue
                filtered_files.append(file_path)

            if not filtered_files:
                self.logger.info("沒有找到符合條件的 HDF 文件")
                return False

            # 按時間順序排序文件
            filtered_files.sort(key=lambda f: extract_datetime_from_filename(f.name, to_local=False))
            self.logger.info(f"找到 {len(filtered_files)} 個有效的衛星數據文件，已按時間順序排序")

            if merge_by_month:
                return self._merge_by_month(filtered_files, output_filename)
            else:
                # 按日期分組文件，同一天的數據合併為一個時間點
                daily_files = self._group_files_by_date(filtered_files)
                self.logger.info(f"按日期分組後有 {len(daily_files)} 個不同的日期")
                return self._merge_all_files_grouped(daily_files, output_filename)

        except Exception as e:
            self.logger.error(f"合併文件時發生錯誤: {e}")
            return False

    def _merge_by_month(self, filtered_files: List[Path], output_filename: Optional[str] = None) -> bool:
        """按月份分別合併文件"""
        # 按年月組織文件
        files_by_month = {}
        for file_path in filtered_files:
            file_date = extract_datetime_from_filename(file_path.name, to_local=False)
            year_month = file_date.strftime('%Y-%m')
            if year_month not in files_by_month:
                files_by_month[year_month] = []
            files_by_month[year_month].append(file_path)

        success_count = 0
        for year_month, month_files in files_by_month.items():
            year, month = year_month.split('-')
            
            # 生成輸出文件名
            if output_filename:
                base_name = output_filename.replace('.nc', '')
                nc_filename = f"{base_name}_{year}{month}.nc"
            else:
                nc_filename = f"{self.file_type}_merged_{year}{month}.nc"
            
            output_path = self.processed_dir / self.file_type / year / month / nc_filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.logger.info(f"合併 {year}-{month} 的 {len(month_files)} 個文件到 {nc_filename}")
            
            if self._merge_files_to_netcdf(month_files, output_path):
                success_count += 1
                self.logger.info(f"成功合併 {year}-{month} 的文件")
            else:
                self.logger.error(f"合併 {year}-{month} 的文件失敗")

        self.logger.info(f"合併完成! 成功合併 {success_count} 個月份的文件。")
        return success_count > 0

    def _group_files_by_date(self, filtered_files: List[Path]) -> Dict[str, List[Path]]:
        """按日期分組文件，同一天的數據合併為一個時間點"""
        daily_files = {}
        
        for file_path in filtered_files:
            file_date = extract_datetime_from_filename(file_path.name, to_local=False)
            if file_date:
                # 使用日期作為鍵（不包含時間）
                date_key = file_date.strftime('%Y-%m-%d')
                if date_key not in daily_files:
                    daily_files[date_key] = []
                daily_files[date_key].append(file_path)
        
        # 對每個日期的文件按檔名排序
        for date_key in daily_files:
            daily_files[date_key].sort(key=lambda f: f.name)
        
        return daily_files

    def _merge_all_files(self, filtered_files: List[Path], output_filename: Optional[str] = None) -> bool:
        """合併所有文件到一個 NetCDF 文件"""
        # 生成輸出文件名
        if output_filename:
            nc_filename = output_filename if output_filename.endswith('.nc') else f"{output_filename}.nc"
        else:
            start_date = extract_datetime_from_filename(filtered_files[0].name, to_local=False)
            end_date = extract_datetime_from_filename(filtered_files[-1].name, to_local=False)
            nc_filename = f"{self.file_type}_merged_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.nc"
        
        output_path = self.processed_dir / self.file_type / nc_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"合併所有 {len(filtered_files)} 個文件到 {nc_filename}")
        
        success = self._merge_files_to_netcdf(filtered_files, output_path)
        if success:
            self.logger.info(f"成功合併所有文件到 {nc_filename}")
        else:
            self.logger.error(f"合併所有文件失敗")
        
        return success

    def _merge_all_files_grouped(self, daily_files: Dict[str, List[Path]], output_filename: Optional[str] = None) -> bool:
        """合併按日期分組的文件到一個 NetCDF 文件"""
        try:
            # 生成輸出文件名
            if output_filename:
                # 確保檔名有 .nc 後綴
                if not output_filename.endswith('.nc'):
                    nc_filename = f"{output_filename}.nc"
                else:
                    nc_filename = output_filename
            else:
                nc_filename = f"{self.file_type}_all_merged.nc"
            
            output_path = self.processed_dir / self.file_type / nc_filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            self.logger.info(f"開始合併 {len(daily_files)} 個日期的數據到 {nc_filename}")
            
            # 收集所有日期的數據
            all_aod_data = []
            all_times = []
            all_latitudes = []
            all_longitudes = []
            
            # 按日期順序處理
            sorted_dates = sorted(daily_files.keys())
            
            for date_key in sorted_dates:
                day_files = daily_files[date_key]
                self.logger.info(f"處理 {date_key} 的 {len(day_files)} 個文件")
                
                # 合併同一天的多個文件
                day_aod_data, day_latitudes, day_longitudes = self._merge_single_day_files(day_files, date_key)
                
                if day_aod_data is not None:
                    all_aod_data.append(day_aod_data)
                    all_times.append(datetime.strptime(date_key, '%Y-%m-%d'))
                    all_latitudes.append(day_latitudes)
                    all_longitudes.append(day_longitudes)
            
            if not all_aod_data:
                self.logger.error("沒有成功處理任何數據")
                return False
            
            # 創建合併的數據集
            merged_dataset = self._create_merged_dataset(all_aod_data, all_times, all_latitudes, all_longitudes)
            
            if merged_dataset is None:
                self.logger.error("創建合併數據集失敗")
                return False
            
            # 保存到 NetCDF 文件
            merged_dataset.to_netcdf(output_path, engine='netcdf4')
            merged_dataset.close()
            
            self.logger.info(f"成功保存合併的 NetCDF 文件: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"合併按日期分組的文件時發生錯誤: {e}")
            return False

    def _merge_single_day_files(self, day_files: List[Path], date_key: str) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], Optional[np.ndarray]]:
        """合併同一天的多個文件"""
        try:
            all_day_aod = []
            all_day_lat = []
            all_day_lon = []
            
            for file_path in day_files:
                self.logger.debug(f"  處理文件: {file_path.name}")
                
                # 打開 HDF4 文件（pyhdf）
                hdf_obj = self._open_with_pyhdf(file_path)
                datasets = hdf_obj.datasets() if hdf_obj else {}

                if not hdf_obj:
                    self.logger.warning(f"無法打開文件: {file_path}")
                    continue
                
                # 提取數據
                if self.file_type == "MCD19A2":
                    aod_data, lat_data, lon_data = self._extract_mcd19a2_data(hdf_obj, datasets, file_path.name)
                else:
                    aod_data, lat_data, lon_data = self._extract_mod04_data(hdf_obj, datasets)
                
                # 關閉文件
                self._close_hdf_file(hdf_obj)
                
                if aod_data is not None and lat_data is not None and lon_data is not None:
                    all_day_aod.append(aod_data)
                    all_day_lat.append(lat_data)
                    all_day_lon.append(lon_data)
            
            if not all_day_aod:
                self.logger.warning(f"{date_key} 沒有有效的數據")
                return None, None, None
            
            # 定義台灣統一網格（根據數據類型設置適當的解析度）
            taiwan_lat_min, taiwan_lat_max = 21.0, 26.0
            taiwan_lon_min, taiwan_lon_max = 119.0, 123.0
            
            # 根據數據類型設置適當的網格解析度
            if self.file_type == "MCD19A2":
                grid_resolution = 0.01  # 1km 解析度
                self.logger.info(f"  使用 1km 解析度網格 (0.01°)")
            elif self.file_type in ["MOD04_L2", "MYD04_L2"]:
                grid_resolution = 0.1  # 約10km 解析度（更適合軌道數據）
                self.logger.info(f"  使用 10km 解析度網格 (0.1°)")
            else:
                grid_resolution = 0.01  # 預設使用 1km 解析度
                self.logger.info(f"  使用預設 1km 解析度網格 (0.01°)")
            
            taiwan_lat = np.arange(taiwan_lat_min, taiwan_lat_max + grid_resolution, grid_resolution)
            taiwan_lon = np.arange(taiwan_lon_min, taiwan_lon_max + grid_resolution, grid_resolution)
            
            # 合併同一天的多個 tile 數據
            if len(all_day_aod) > 1:
                # 有多個 tile，使用手動合併邏輯
                self.logger.info(f"  合併 {len(all_day_aod)} 個 tile")
                
                # 分別投影每個 tile 到台灣網格
                reprojected_tiles = []
                for i, (aod_data, lat_data, lon_data) in enumerate(zip(all_day_aod, all_day_lat, all_day_lon)):
                    self.logger.debug(f"    投影 tile {i+1}")
                    reprojected = self._reproject_to_taiwan_grid(aod_data, lat_data, lon_data, taiwan_lat, taiwan_lon)
                    reprojected_tiles.append(reprojected)
                
                # 合併投影後的數據
                merged_aod = self._merge_reprojected_tiles(reprojected_tiles)
                
                self.logger.info(f"  手動合併完成，有效數據點: {np.sum(~np.isnan(merged_aod))}/{merged_aod.size}")
            else:
                # 只有一個 tile，也要投影到台灣統一網格
                self.logger.info(f"  投影單個 tile 到台灣網格")
                merged_aod = self._reproject_to_taiwan_grid(all_day_aod[0], all_day_lat[0], all_day_lon[0], taiwan_lat, taiwan_lon)
                self.logger.info(f"  單個 tile 投影完成，有效數據點: {np.sum(~np.isnan(merged_aod))}/{merged_aod.size}")
            
            # 使用統一的台灣網格座標
            merged_lat = taiwan_lat
            merged_lon = taiwan_lon
            
            return merged_aod, merged_lat, merged_lon
            
        except Exception as e:
            self.logger.error(f"合併 {date_key} 的文件時發生錯誤: {e}")
            return None, None, None

    def _merge_tile_data(self, aod_list: List[np.ndarray], lat_list: List[np.ndarray], lon_list: List[np.ndarray]) -> np.ndarray:
        """合併多個 tile 的數據"""
        try:
            # 檢查所有 tile 的形狀是否一致
            shapes = [aod.shape for aod in aod_list]
            if len(set(shapes)) == 1:
                # 如果所有 tile 形狀一致，直接合併
                merged_aod = np.full_like(aod_list[0], np.nan)
                for aod_data in aod_list:
                    valid_mask = ~np.isnan(aod_data)
                    merged_aod[valid_mask] = aod_data[valid_mask]
                return merged_aod
            
            # 如果形狀不一致，使用第一個 tile 的形狀作為基準
            self.logger.warning(f"不同 tile 的形狀不一致: {shapes}，使用第一個 tile 的形狀")
            base_shape = aod_list[0].shape
            merged_aod = np.full(base_shape, np.nan)
            
            for aod_data in aod_list:
                if aod_data.shape == base_shape:
                    # 形狀匹配，直接合併
                    valid_mask = ~np.isnan(aod_data)
                    merged_aod[valid_mask] = aod_data[valid_mask]
                else:
                    # 形狀不匹配，跳過這個 tile
                    self.logger.warning(f"跳過形狀不匹配的 tile: {aod_data.shape} != {base_shape}")
                    continue
            
            return merged_aod
            
        except Exception as e:
            self.logger.error(f"合併 tile 數據時發生錯誤: {e}")
            # 如果合併失敗，返回第一個 tile 的數據
            return aod_list[0]

    def _merge_files_to_netcdf(self, hdf_files: List[Path], output_path: Path) -> bool:
        """將多個 HDF 文件合併成一個 NetCDF 文件"""
        try:
            # 存儲所有數據的列表
            aod_data_list = []
            time_list = []
            latitude_list = []
            longitude_list = []
            
            # 處理每個 HDF 文件
            for hdf_file in hdf_files:
                try:
                    file_date = extract_datetime_from_filename(hdf_file.name, to_local=False)
                    self.logger.debug(f"處理文件: {hdf_file.name} ({file_date.strftime('%Y-%m-%d')})")
                    
                    # 打開 HDF4 文件（pyhdf）
                    hdf_obj = self._open_with_pyhdf(hdf_file)
                    datasets = hdf_obj.datasets() if hdf_obj else {}

                    if not hdf_obj:
                        self.logger.warning(f"無法打開文件: {hdf_file}")
                        continue
                    
                    # 根據文件類型處理數據
                    if self._is_mcd19a2_file(hdf_file.name):
                        aod_data, lat, lon = self._extract_mcd19a2_data(hdf_obj, datasets, hdf_file.name)
                    else:
                        aod_data, lat, lon = self._extract_mod04_data(hdf_obj, datasets)
                    
                    if aod_data is not None and lat is not None and lon is not None:
                        aod_data_list.append(aod_data)
                        time_list.append(file_date)
                        latitude_list.append(lat)
                        longitude_list.append(lon)
                    
                    # 關閉文件
                    self._close_hdf_file(hdf_obj)
                    
                except Exception as e:
                    self.logger.error(f"處理文件 {hdf_file.name} 時發生錯誤: {e}")
                    continue
            
            if not aod_data_list:
                self.logger.error("沒有成功提取任何數據")
                return False
            
            # 檢查所有數據的形狀是否一致
            shapes = [data.shape for data in aod_data_list]
            if len(set(shapes)) > 1:
                self.logger.warning(f"數據形狀不一致: {shapes}")
                # 統一形狀到最小值
                min_shape = [min(dim) for dim in zip(*shapes)]
                aod_data_list = [data[:min_shape[0], :min_shape[1]] for data in aod_data_list]
                latitude_list = [lat[:min_shape[0], :min_shape[1]] for lat in latitude_list]
                longitude_list = [lon[:min_shape[0], :min_shape[1]] for lon in longitude_list]
            
            # 創建 xarray Dataset
            ds = self._create_merged_dataset(aod_data_list, time_list, latitude_list, longitude_list)
            
            # 保存為 NetCDF 文件
            ds.to_netcdf(output_path, engine='netcdf4')
            self.logger.info(f"成功保存合併的 NetCDF 文件: {output_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"合併文件時發生錯誤: {e}")
            return False

    def _extract_mcd19a2_data(self, hdf_obj, datasets, filename: str = None) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], Optional[np.ndarray]]:
        """從 MCD19A2 文件中提取 AOD 數據和座標"""
        try:
            aod_name = 'Optical_Depth_047'
            if aod_name not in datasets:
                aod_name = 'Optical_Depth_055'
                if aod_name not in datasets:
                    return None, None, None
            
            # 獲取 AOD 數據（pyhdf）
            aod_data, aod_attrs = self._get_data_pyhdf(hdf_obj, aod_name)

            if aod_data is None:
                return None, None, None

            # MCD19A2 是3D數據，取第一個時間層
            if len(aod_data.shape) == 3:
                aod_data = aod_data[0, :, :]
            
            # 生成地理座標
            latitude, longitude = self._generate_mcd19a2_coordinates(aod_data.shape, filename or "MCD19A2.hdf")
            
            # 處理數據
            scale_factor = aod_attrs.get('scale_factor', 0.0001)
            _FillValue = aod_attrs.get('_FillValue', -28672)
            aod_data = self._process_aod_data(aod_data, scale_factor, _FillValue)
            
            return aod_data, latitude, longitude
            
        except Exception as e:
            self.logger.error(f"提取 MCD19A2 數據時發生錯誤: {e}")
            return None, None, None

    def _extract_mod04_data(self, hdf_obj, datasets) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], Optional[np.ndarray]]:
        """從 MOD04/MYD04 文件中提取 AOD 數據和座標"""
        try:
            # 使用配置的 AOD 變量名稱，並提供備用選項
            aod_name = self.aod_variable
            fallback_options = [
                'AOD_550_Dark_Target_Deep_Blue_Combined',
                'Optical_Depth_Land_And_Ocean',
                'Image_Optical_Depth_Land_And_Ocean'
            ]
            
            if aod_name not in datasets:
                for fallback in fallback_options:
                    if fallback != aod_name and fallback in datasets:
                        aod_name = fallback
                        break
                else:
                    return None, None, None
            
            # 獲取 AOD 數據和座標（pyhdf）
            aod_data, aod_attrs = self._get_data_pyhdf(hdf_obj, aod_name)
            latitude = self._get_data_pyhdf(hdf_obj, 'Latitude')[0]
            longitude = self._get_data_pyhdf(hdf_obj, 'Longitude')[0]

            if aod_data is None:
                return None, None, None

            # 確保數據形狀一致
            aod_data, latitude, longitude = self._align_data_shapes(aod_data, latitude, longitude)
            
            # 處理數據
            scale_factor = aod_attrs.get('scale_factor', 0.001)
            _FillValue = aod_attrs.get('_FillValue', -9999)
            aod_data = self._process_aod_data(aod_data, scale_factor, _FillValue)
            
            return aod_data, latitude, longitude
            
        except Exception as e:
            self.logger.error(f"提取 MOD04 數據時發生錯誤: {e}")
            return None, None, None

    def _create_merged_dataset(self, aod_data_list: List[np.ndarray], 
                              time_list: List[datetime], 
                              latitude_list: List[np.ndarray], 
                              longitude_list: List[np.ndarray]) -> xr.Dataset:
        """創建合併的 xarray Dataset，並重新投影到台灣統一網格"""
        try:
            # 按時間順序排序所有數據
            time_coords = pd.to_datetime(time_list)
            sorted_indices = np.argsort(time_coords)
            
            # 重新排序所有列表
            aod_data_sorted = [aod_data_list[i] for i in sorted_indices]
            time_sorted = [time_list[i] for i in sorted_indices]
            latitude_sorted = [latitude_list[i] for i in sorted_indices]
            longitude_sorted = [longitude_list[i] for i in sorted_indices]
            
            self.logger.info(f"按時間順序重新排序了 {len(sorted_indices)} 個時間點")
            
            # 定義台灣統一網格範圍和解析度
            taiwan_lat_min, taiwan_lat_max = 21.0, 26.0  # 台灣緯度範圍
            taiwan_lon_min, taiwan_lon_max = 119.0, 123.0  # 台灣經度範圍
            
            # 根據數據類型設置適當的網格解析度
            if self.file_type == "MCD19A2":
                grid_resolution = 0.01  # 1km 解析度
                resolution_description = "1km"
            elif self.file_type in ["MOD04_L2", "MYD04_L2"]:
                grid_resolution = 0.1  # 約10km 解析度（更適合軌道數據）
                resolution_description = "10km"
            else:
                grid_resolution = 0.01  # 預設使用 1km 解析度
                resolution_description = "1km"
            
            # 創建台灣統一網格
            taiwan_lat = np.arange(taiwan_lat_min, taiwan_lat_max + grid_resolution, grid_resolution)
            taiwan_lon = np.arange(taiwan_lon_min, taiwan_lon_max + grid_resolution, grid_resolution)
            
            self.logger.info(f"創建台灣統一網格: 緯度 {len(taiwan_lat)} 點 ({taiwan_lat_min:.2f} 到 {taiwan_lat_max:.2f}), "
                           f"經度 {len(taiwan_lon)} 點 ({taiwan_lon_min:.2f} 到 {taiwan_lon_max:.2f})")
            
            # 按日期分組數據，同一天的數據合併為一個時間點
            daily_data = {}
            for i, (aod_data, time, lat_coords, lon_coords) in enumerate(zip(aod_data_sorted, time_sorted, latitude_sorted, longitude_sorted)):
                date_key = time.strftime('%Y-%m-%d')
                if date_key not in daily_data:
                    daily_data[date_key] = {
                        'aod_list': [],
                        'lat_list': [],
                        'lon_list': [],
                        'time': time
                    }
                daily_data[date_key]['aod_list'].append(aod_data)
                daily_data[date_key]['lat_list'].append(lat_coords)
                daily_data[date_key]['lon_list'].append(lon_coords)
            
            self.logger.info(f"按日期分組後有 {len(daily_data)} 個不同的日期")
            
            # 處理每個日期的數據：使用已經合併好的數據（已經投影到正確網格）
            reprojected_aod_list = []
            daily_times = []
            
            for date_key, daily_info in sorted(daily_data.items()):
                self.logger.info(f"處理日期 {date_key}: 使用已合併的數據")
                
                # 由於數據已經在 _merge_single_day_files 中合併並投影到正確網格，這裡直接使用
                # daily_info['aod_list'] 應該只有一個元素（合併後的數據）
                if len(daily_info['aod_list']) == 1:
                    merged_daily_aod = daily_info['aod_list'][0]
                    self.logger.info(f"  使用已合併的數據，形狀: {merged_daily_aod.shape}")
                    
                    # 驗證數據形狀是否與目標網格匹配
                    expected_shape = (len(taiwan_lat), len(taiwan_lon))
                    if merged_daily_aod.shape != expected_shape:
                        self.logger.warning(f"  數據形狀 {merged_daily_aod.shape} 與目標網格 {expected_shape} 不匹配")
                        # 如果形狀不匹配，重新投影
                        if len(daily_info['lat_list']) > 0 and len(daily_info['lon_list']) > 0:
                            source_lat = daily_info['lat_list'][0]
                            source_lon = daily_info['lon_list'][0]
                            merged_daily_aod = self._reproject_to_taiwan_grid(merged_daily_aod, source_lat, source_lon, taiwan_lat, taiwan_lon)
                            self.logger.info(f"  重新投影後形狀: {merged_daily_aod.shape}")
                else:
                    self.logger.warning(f"  預期只有一個合併後的數據，但發現 {len(daily_info['aod_list'])} 個")
                    # 如果有多個，取第一個
                    merged_daily_aod = daily_info['aod_list'][0]
                
                reprojected_aod_list.append(merged_daily_aod)
                daily_times.append(daily_info['time'])
            
            self.logger.info(f"創建了重新投影的 3D 數據集，形狀: ({len(reprojected_aod_list)}, {len(taiwan_lat)}, {len(taiwan_lon)})")
            
            # 將數據堆疊成 3D 數組 (time, lat, lon)
            aod_3d = np.stack(reprojected_aod_list, axis=0)
            
            # 創建時間座標
            time_coords_sorted = pd.to_datetime(daily_times)
            
            # 創建 xarray Dataset
            ds = xr.Dataset(
                data_vars={
                    'aod': (['time', 'lat', 'lon'], aod_3d, {
                        'long_name': 'Aerosol Optical Depth',
                        'units': 'dimensionless',
                        'description': 'MODIS AOD data merged from multiple HDF files and reprojected to Taiwan unified grid'
                    })
                },
                coords={
                    'time': time_coords_sorted,
                    'lat': (['lat'], taiwan_lat, {
                        'long_name': 'latitude',
                        'units': 'degrees_north'
                    }),
                    'lon': (['lon'], taiwan_lon, {
                        'long_name': 'longitude', 
                        'units': 'degrees_east'
                    })
                },
                attrs={
                    'title': f'MODIS {self.file_type} Merged AOD Dataset (Taiwan Grid)',
                    'description': f'Aerosol Optical Depth data merged from {len(daily_times)} HDF files and reprojected to Taiwan unified grid',
                    'creation_date': datetime.now().isoformat(),
                    'file_type': self.file_type,
                    'grid_resolution': f'{grid_resolution} degrees (~{resolution_description})',
                    'taiwan_bounds': f'lat: {taiwan_lat_min:.2f} to {taiwan_lat_max:.2f}, lon: {taiwan_lon_min:.2f} to {taiwan_lon_max:.2f}'
                }
            )
            
            self.logger.info(f"創建了 3D 數據集，形狀: {ds.aod.shape}")
            return ds
            
        except Exception as e:
            self.logger.error(f"創建合併數據集時發生錯誤: {e}")
            raise
    
    def _merge_reprojected_tiles(self, reprojected_tiles: List[np.ndarray]) -> np.ndarray:
        """合併已經投影到統一網格的 tile 數據"""
        try:
            if len(reprojected_tiles) == 1:
                return reprojected_tiles[0]
            
            # 創建合併的網格
            merged_grid = np.full(reprojected_tiles[0].shape, np.nan)
            count_grid = np.zeros(reprojected_tiles[0].shape)
            
            # 累加所有有效數據
            for tile_data in reprojected_tiles:
                valid_mask = ~np.isnan(tile_data)
                # 如果 merged_grid 在該位置還是 NaN，直接賦值
                nan_mask = np.isnan(merged_grid) & valid_mask
                merged_grid[nan_mask] = tile_data[nan_mask]
                count_grid[nan_mask] += 1
                
                # 如果 merged_grid 在該位置已有值，則累加
                both_valid_mask = ~np.isnan(merged_grid) & valid_mask
                merged_grid[both_valid_mask] += tile_data[both_valid_mask]
                count_grid[both_valid_mask] += 1
            
            # 計算平均值
            avg_mask = count_grid > 0
            merged_grid[avg_mask] = merged_grid[avg_mask] / count_grid[avg_mask]
            
            # 統計合併結果
            valid_count = np.sum(~np.isnan(merged_grid))
            total_count = merged_grid.size
            self.logger.info(f"    合併後有效數據點: {valid_count}/{total_count} ({valid_count/total_count*100:.1f}%)")
            
            return merged_grid
            
        except Exception as e:
            self.logger.error(f"合併投影後的 tile 數據時發生錯誤: {e}")
            return reprojected_tiles[0] if reprojected_tiles else None
    
    def _merge_daily_tiles(self, aod_list: List[np.ndarray], 
                          lat_list: List[np.ndarray], 
                          lon_list: List[np.ndarray]) -> np.ndarray:
        """合併同一天的多個 tile 數據（使用聯集方式）"""
        try:
            if len(aod_list) == 1:
                # 如果只有一個 tile，直接返回
                return aod_list[0]
            
            self.logger.info(f"合併 {len(aod_list)} 個 tile 數據")
            
            # 找到所有 tile 的經緯度範圍（聯集）
            all_lat_min = min([np.min(lat) for lat in lat_list])
            all_lat_max = max([np.max(lat) for lat in lat_list])
            all_lon_min = min([np.min(lon) for lon in lon_list])
            all_lon_max = max([np.max(lon) for lon in lon_list])
            
            self.logger.info(f"合併後的經緯度範圍: 經度 {all_lon_min:.2f} 到 {all_lon_max:.2f}, 緯度 {all_lat_min:.2f} 到 {all_lat_max:.2f}")
            
            # 創建統一的網格（根據數據類型設置適當的解析度）
            if self.file_type == "MCD19A2":
                lat_res = 0.01  # 1km 解析度
                lon_res = 0.01
            elif self.file_type in ["MOD04_L2", "MYD04_L2"]:
                lat_res = 0.1  # 約10km 解析度
                lon_res = 0.1
            else:
                lat_res = 0.01  # 預設使用 1km 解析度
                lon_res = 0.01
            
            lat_coords = np.arange(all_lat_min, all_lat_max + lat_res, lat_res)
            lon_coords = np.arange(all_lon_min, all_lon_max + lon_res, lon_res)
            
            # 創建合併的網格
            merged_grid = np.full((len(lat_coords), len(lon_coords)), np.nan)
            
            # 將每個 tile 的數據填入合併網格
            for i, (aod_data, lat_data, lon_data) in enumerate(zip(aod_list, lat_list, lon_list)):
                self.logger.info(f"處理 tile {i+1}: 形狀 {aod_data.shape}, 經度範圍 {np.min(lon_data):.2f} 到 {np.max(lon_data):.2f}")
                
                # 找到當前 tile 在合併網格中的位置
                lat_mask = (lat_coords >= np.min(lat_data)) & (lat_coords <= np.max(lat_data))
                lon_mask = (lon_coords >= np.min(lon_data)) & (lon_coords <= np.max(lon_data))
                
                lat_indices = np.where(lat_mask)[0]
                lon_indices = np.where(lon_mask)[0]
                
                if len(lat_indices) > 0 and len(lon_indices) > 0:
                    # 確保索引範圍正確
                    min_lat_len = min(len(lat_indices), aod_data.shape[0])
                    min_lon_len = min(len(lon_indices), aod_data.shape[1])
                    
                    # 將有效數據填入合併網格（覆蓋 NaN 值）
                    valid_data = aod_data[:min_lat_len, :min_lon_len]
                    valid_mask = ~np.isnan(valid_data)
                    
                    if np.any(valid_mask):
                        # 只在有效數據的位置填入值
                        target_lat_slice = lat_indices[:min_lat_len]
                        target_lon_slice = lon_indices[:min_lon_len]
                        
                        # 使用布林索引只更新有效數據的位置
                        for lat_idx, data_lat_idx in enumerate(target_lat_slice):
                            for lon_idx, data_lon_idx in enumerate(target_lon_slice):
                                if valid_mask[lat_idx, lon_idx]:
                                    merged_grid[data_lat_idx, data_lon_idx] = valid_data[lat_idx, lon_idx]
            
            # 檢查合併結果
            valid_count = np.sum(~np.isnan(merged_grid))
            total_count = merged_grid.size
            self.logger.info(f"合併完成: {valid_count}/{total_count} 個有效數據點 ({valid_count/total_count*100:.1f}%)")
            
            return merged_grid
            
        except Exception as e:
            self.logger.error(f"合併每日 tile 數據時發生錯誤: {e}")
            return None

    def _reproject_to_taiwan_grid(self, aod_data: np.ndarray, 
                                 source_lat: np.ndarray, 
                                 source_lon: np.ndarray,
                                 target_lat: np.ndarray, 
                                 target_lon: np.ndarray) -> np.ndarray:
        """將單個時間點的 MODIS 數據填入台灣統一網格（基於 NASA 最佳實踐）"""
        try:
            # 創建目標網格（初始化為 NaN）
            target_grid = np.full((len(target_lat), len(target_lon)), np.nan)
            
            # 準備源數據點
            if source_lat.shape == (1200, 1200):
                # 如果是 2D 網格，展平為點
                source_lat_flat = source_lat.flatten()
                source_lon_flat = source_lon.flatten()
                aod_flat = aod_data.flatten()
            else:
                # 如果已經是 1D 或點數據
                source_lat_flat = source_lat.flatten()
                source_lon_flat = source_lon.flatten()
                aod_flat = aod_data.flatten()
            
            # 移除無效數據點
            valid_mask = ~np.isnan(aod_flat) & (aod_flat != -28672) & (aod_flat != 0)
            if np.sum(valid_mask) == 0:
                # 如果沒有有效數據，返回全 NaN
                return target_grid
            
            source_lat_valid = source_lat_flat[valid_mask]
            source_lon_valid = source_lon_flat[valid_mask]
            aod_valid = aod_flat[valid_mask]
            
            # 計算數據密度和空間分佈
            data_density = len(aod_valid) / (len(target_lat) * len(target_lon))
            lat_range = np.max(source_lat_valid) - np.min(source_lat_valid)
            lon_range = np.max(source_lon_valid) - np.min(source_lon_valid)
            spatial_coverage = (lat_range * lon_range) / ((target_lat[-1] - target_lat[0]) * 
                                                         (target_lon[-1] - target_lon[0]))
            
            self.logger.info(f"數據分析: 密度={data_density:.3f}, 空間覆蓋率={spatial_coverage:.3f}, 有效點={len(aod_valid)}")
            
            # 根據 NASA 最佳實踐選擇插值方法
            if data_density > 0.1 and spatial_coverage > 0.3:
                # 高密度、高覆蓋率：使用距離限制的線性插值
                self.logger.info("使用距離限制的線性插值")
                target_grid = self._linear_interpolation_with_distance_limit(
                    source_lat_valid, source_lon_valid, aod_valid, target_lat, target_lon, max_distance=0.15
                )
            elif data_density > 0.01:
                # 中等密度：使用距離加權插值
                self.logger.info("使用距離加權插值")
                target_grid = self._distance_weighted_interpolation(
                    source_lat_valid, source_lon_valid, aod_valid, target_lat, target_lon, max_distance=0.2
                )
            else:
                # 低密度：使用限制距離的最近鄰插值
                self.logger.info("使用限制距離的最近鄰插值")
                target_grid = self._nearest_neighbor_with_distance_limit(
                    source_lat_valid, source_lon_valid, aod_valid, target_lat, target_lon, max_distance=0.25
                )
            
            # 檢查結果的合理性
            valid_count = np.sum(~np.isnan(target_grid))
            total_count = target_grid.size
            coverage = valid_count / total_count * 100
            
            self.logger.info(f"插值結果: {valid_count}/{total_count} 有效點 ({coverage:.1f}% 覆蓋率)")
            
            # 如果覆蓋率過高，發出警告
            if coverage > 50:
                self.logger.warning(f"覆蓋率過高 ({coverage:.1f}%)，可能存在過度插值")
            
            return target_grid
            
        except Exception as e:
            self.logger.error(f"投影到台灣網格時發生錯誤: {e}")
            # 返回全 NaN 作為後備
            return np.full((len(target_lat), len(target_lon)), np.nan)
    
    def _distance_weighted_interpolation(self, source_lat, source_lon, aod_valid, target_lat, target_lon, max_distance=0.2):
        """距離加權插值 - 基於 NASA MODIS 處理方法"""
        target_grid = np.full((len(target_lat), len(target_lon)), np.nan)
        
        # 對每個目標網格點進行插值
        for i in range(len(target_lat)):
            for j in range(len(target_lon)):
                target_lat_val = target_lat[i]
                target_lon_val = target_lon[j]
                
                # 計算到所有源數據點的距離
                distances = np.sqrt((source_lat - target_lat_val)**2 + 
                                  (source_lon - target_lon_val)**2)
                
                # 找到在最大距離內的數據點
                valid_mask = distances <= max_distance
                
                if np.any(valid_mask):
                    valid_distances = distances[valid_mask]
                    valid_aod = aod_valid[valid_mask]
                    
                    # 計算距離權重（距離越近權重越大）
                    weights = 1.0 / (valid_distances + 1e-6)  # 避免除零
                    
                    # 加權平均
                    weighted_aod = np.average(valid_aod, weights=weights)
                    target_grid[i, j] = weighted_aod
        
        return target_grid
    
    def _linear_interpolation_with_distance_limit(self, source_lat, source_lon, aod_valid, target_lat, target_lon, max_distance=0.15):
        """帶距離限制的線性插值"""
        try:
            from scipy.interpolate import griddata
            
            # 創建目標網格點
            target_lon_grid, target_lat_grid = np.meshgrid(target_lon, target_lat)
            target_points = np.column_stack([target_lat_grid.flatten(), target_lon_grid.flatten()])
            
            # 源數據點
            source_points = np.column_stack([source_lat, source_lon])
            
            # 使用線性插值
            target_grid_flat = griddata(source_points, aod_valid, target_points, method='linear')
            
            # 重塑為目標網格形狀
            target_grid = target_grid_flat.reshape(target_lat_grid.shape)
            
            # 過濾掉不合理的值
            target_grid[target_grid < 0] = np.nan
            
            # 應用距離限制（只保留在有效數據點附近的插值結果）
            distance_limited_grid = np.full_like(target_grid, np.nan)
            for i in range(len(target_lat)):
                for j in range(len(target_lon)):
                    target_lat_val = target_lat[i]
                    target_lon_val = target_lon[j]
                    
                    # 計算到最近源數據點的距離
                    distances = np.sqrt((source_lat - target_lat_val)**2 + 
                                      (source_lon - target_lon_val)**2)
                    min_distance = np.min(distances)
                    
                    # 只有在最大距離內才保留插值結果
                    if min_distance <= max_distance and not np.isnan(target_grid[i, j]):
                        distance_limited_grid[i, j] = target_grid[i, j]
            
            return distance_limited_grid
            
        except ImportError:
            # 如果 scipy 不可用，回退到距離加權插值
            return self._distance_weighted_interpolation(source_lat, source_lon, aod_valid, target_lat, target_lon, max_distance)
    
    def _nearest_neighbor_with_distance_limit(self, source_lat, source_lon, aod_valid, target_lat, target_lon, max_distance=0.25):
        """帶距離限制的最近鄰插值"""
        target_grid = np.full((len(target_lat), len(target_lon)), np.nan)
        
        for i in range(len(target_lat)):
            for j in range(len(target_lon)):
                target_lat_val = target_lat[i]
                target_lon_val = target_lon[j]
                
                # 計算到所有源數據點的距離
                distances = np.sqrt((source_lat - target_lat_val)**2 + 
                                  (source_lon - target_lon_val)**2)
                
                # 找到最近的數據點
                min_distance_idx = np.argmin(distances)
                min_distance = distances[min_distance_idx]
                
                # 只有在最大距離內才進行插值
                if min_distance <= max_distance:
                    target_grid[i, j] = aod_valid[min_distance_idx]
        
        return target_grid
    
    def _direct_fill_interpolation(self, source_lat, source_lon, aod_valid, target_lat, target_lon):
        """直接填充方法（原始方法）"""
        target_grid = np.full((len(target_lat), len(target_lon)), np.nan)
        
        # 計算網格解析度
        lat_res = target_lat[1] - target_lat[0]
        lon_res = target_lon[1] - target_lon[0]
        
        # 將每個有效數據點填入對應的網格
        for lat, lon, aod_value in zip(source_lat, source_lon, aod_valid):
            # 檢查是否在台灣網格範圍內
            if (target_lat[0] <= lat <= target_lat[-1] and 
                target_lon[0] <= lon <= target_lon[-1]):
                
                # 找到最近的網格點
                lat_idx = int((lat - target_lat[0]) / lat_res)
                lon_idx = int((lon - target_lon[0]) / lon_res)
                
                # 確保索引在範圍內
                lat_idx = max(0, min(lat_idx, len(target_lat) - 1))
                lon_idx = max(0, min(lon_idx, len(target_lon) - 1))
                
                # 如果該網格點還沒有數據，填入數據
                if np.isnan(target_grid[lat_idx, lon_idx]):
                    target_grid[lat_idx, lon_idx] = aod_value
        
        return target_grid