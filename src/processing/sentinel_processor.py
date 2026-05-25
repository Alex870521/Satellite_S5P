import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime
from pathlib import Path

from src.processing.interpolators import DataInterpolator
from src.processing.grid_frame import GridFrame
from src.config.settings import FIGURE_BOUNDARY
from src.config.catalog import PRODUCT_CONFIGS, get_resolution_for_product
from src.visualization.plot_nc import plot_global_var
from src.visualization.gif import animate_data
from src.utils.extract_datetime_from_filename import extract_datetime_from_filename


class SentinelProcessor:
    """處理 S5P 數據並生成可視化圖像"""
    
    # 氣體類型對應的網格解析度設定（基於 Sentinel-5P 2019/8/6 後官方規格）
    GAS_RESOLUTION_CONFIG = {
        'NO2': (5.5, 3.5),    # NO₂: 5.5km x 3.5km
        'O3': (5.5, 3.5),     # O₃ (total): 5.5km x 3.5km
        'SO2': (5.5, 3.5),    # SO₂: 5.5km x 3.5km
        'CO': (5.5, 7.0),     # CO: 5.5km x 7km
        'CH4': (5.5, 7.0),    # CH₄: 5.5km x 7km
        'HCHO': (5.5, 3.5),   # HCHO: 5.5km x 3.5km (推測)
        'AER_AI': (5.5, 3.5), # AER (AI): 5.5km x 3.5km
        'AER_LH': (5.5, 3.5), # AER (LH): 5.5km x 3.5km
        'O3_PROFILE': (30.0, 30.0), # O₃ (profile): 30km x 30km
    }
    
    def __init__(self, interpolation_method='rbf', resolution=None, mask_qc_value=0.5, file_type=None):
        """初始化處理器

        Parameters:
        -----------
        interpolation_method : str
            插值方法，可選 'griddata' 或 'kdtree' 或 'rbf'
        resolution : tuple, optional
            網格解析度，格式為 (x_km, y_km)，例如 (5.5, 3.5) 代表 5.5km x 3.5km
            如果為 None，則根據 file_type 自動選擇
        mask_qc_value : float
            QA 值的閾值
        file_type : str, optional
            氣體類型，用於自動選擇解析度
        """
        self.raw_dir = None
        self.processed_dir = None
        self.figure_dir = None
        self.geotiff_dir = None
        self.logger = None
        self.file_type = file_type
        self.file_class = None

        self.interpolation_method = interpolation_method
        self.mask_qc_value = mask_qc_value
        
        # 自動選擇解析度（優先使用 catalog 中的配置）
        if resolution is None and file_type is not None:
            # 首先嘗試從 catalog 獲取解析度
            catalog_resolution = get_resolution_for_product(file_type)
            if catalog_resolution != (5.5, 3.5):  # 如果 catalog 中有特定配置
                self.resolution = catalog_resolution
                if self.logger:
                    self.logger.info(f"從 catalog 獲取 {file_type} 解析度: {self.resolution} km")
            else:
                # 回退到舊的配置方式
                self.resolution = self.GAS_RESOLUTION_CONFIG.get(file_type, (5.5, 3.5))
                if self.logger:
                    self.logger.info(f"根據氣體類型 {file_type} 自動選擇解析度: {self.resolution} km")
        else:
            self.resolution = resolution or (5.5, 3.5)
            
        self.grid_frame = GridFrame(self.resolution)

    def extract_data(self, dataset: xr.Dataset, extract_range: tuple[float, float, float, float] = None):
        """提取數據，可選擇是否限定範圍

        Args:
            dataset: xarray Dataset
            extract_range: 可選的tuple (min_lon, max_lon, min_lat, max_lat)，如果提供則提取指定範圍
        """
        # 初始處理
        time = np.datetime64(dataset.time.values[0], 'D')
        attributes = PRODUCT_CONFIGS[self.file_type].dataset_name

        # 如果提供了範圍，進行過濾
        if extract_range is not None:
            min_lon, max_lon, min_lat, max_lat = extract_range
            mask_lon = (dataset.longitude >= min_lon) & (dataset.longitude <= max_lon)
            mask_lat = (dataset.latitude >= min_lat) & (dataset.latitude <= max_lat)
            dataset = dataset.where((mask_lon & mask_lat), drop=True)

            # 檢查是否有數據
            if dataset.sizes['scanline'] == 0 or dataset.sizes['ground_pixel'] == 0:
                raise ValueError(f"No data points within region: {extract_range}")

        # QA 過濾
        mask_qa = (dataset.qa_value >= self.mask_qc_value)
        mask_dataset = dataset.where(mask_qa)

        # 檢查數據有效性
        if np.all(np.isnan(mask_dataset[attributes])):
            raise ValueError("No valid data points after QA filtering")

        lon = mask_dataset.longitude[0].values
        lat = mask_dataset.latitude[0].values
        shape = mask_dataset.latitude[0].values.shape
        var = mask_dataset[attributes][0].values

        # 檢查原始數據形狀是否異常大（僅警告，不進行額外過濾）
        max_expected_points = 1000  # 設定最大預期數據點數
        if shape[0] * shape[1] > max_expected_points:
            self.logger.warning(f"文件 {self.nc_info.get('file_name', 'unknown')}: 原始數據 shape={shape} (數據點: {shape[0] * shape[1]:,})")
            self.logger.warning(f"這表示衛星軌道覆蓋範圍較大，插值後將統一為固定網格")

        info_dict = {
            'time': f"{time}",
            'shape': f"{shape}",
            'latitude': f'{np.nanmin(lat):.2f} to {np.nanmax(lat):.2f}',
            'longitude': f'{np.nanmin(lon):.2f} to {np.nanmax(lon):.2f}',
        }

        if hasattr(self, 'nc_info'):
            self.nc_info.update(info_dict)

        return mask_dataset, lon, lat, var

    def process_nc_file(self, nc_file: Path, output_dir: Path, geotiff_dir: Path, skip_existing: bool = False):
        """處理單個 nc 檔"""
        # 從文件名提取日期
        file_date = extract_datetime_from_filename(nc_file.name, to_local=False)
        
        # 檢查輸出文件是否已存在
        output_file = output_dir / nc_file.name
        if skip_existing and output_file.exists():
            self.logger.info(f"跳過已存在的文件: {nc_file.name} (輸出: {output_file})")
            return True
            
        self.logger.info(f"處理文件: {nc_file.name} ({file_date.strftime('%Y-%m-%d')})")

        # 打開 nc 文件
        ds = xr.open_dataset(nc_file, engine='netcdf4', group='PRODUCT', chunks='auto')

        try:
            interpolated_ds = self._process_data(ds, nc_file)
            if interpolated_ds is not None:
                self._save_outputs(interpolated_ds, nc_file, output_dir, geotiff_dir)
                return True
            else:
                return False

        except Exception as e:
            self.logger.error(f"Error processing file {nc_file.name}: {str(e)}")
            return False
        finally:
            ds.close()

    def _calculate_nearest_grid_points(self, stations, grid_lats, grid_lons, extract_surrounding=False):
        """
        計算每個站點的最近網格點

        Parameters:
            stations (list): 站點列表，每個包含 name, lat, lon
            grid_lats (array): 網格緯度數組 - 可能是1D或2D
            grid_lons (array): 網格經度數組 - 可能是1D或2D
            extract_surrounding (bool): 是否提取周圍8格的數據

        Returns:
            dict: 站點名稱到網格索引的映射
                  如果 extract_surrounding=False: {station_name: (lat_index, lon_index)}
                  如果 extract_surrounding=True: {station_name: [(lat_idx, lon_idx), ...]}  # 9個點的列表
        """
        nearest_points = {}

        # 檢查數組維度並處理
        if grid_lats.ndim == 1 and grid_lons.ndim == 1:
            # 一維數組情況 - S5P 衛星數據的典型情況
            self.logger.debug(f"Grid dimensions: lats={grid_lats.shape}, lons={grid_lons.shape} (1D arrays)")
            lon_grid, lat_grid = np.meshgrid(grid_lons, grid_lats)

        elif grid_lats.ndim == 2 and grid_lons.ndim == 2:
            # 二維數組情況 - 已經是網格格式
            self.logger.debug(f"Grid dimensions: lats={grid_lats.shape}, lons={grid_lons.shape} (2D arrays)")
            lat_grid = grid_lats
            lon_grid = grid_lons

        else:
            raise ValueError(f"Unsupported grid dimensions: lats.ndim={grid_lats.ndim}, lons.ndim={grid_lons.ndim}")

        # 獲取網格邊界
        grid_shape = lat_grid.shape

        for station in stations:
            # 計算到所有網格點的距離
            lat_diff = lat_grid - station['lat']
            lon_diff = lon_grid - station['lon']
            distances = np.sqrt(lat_diff ** 2 + lon_diff ** 2)

            # 找到最小距離的索引
            min_idx = np.unravel_index(np.argmin(distances), distances.shape)
            center_lat_idx, center_lon_idx = min_idx

            if not extract_surrounding:
                # 只返回最近點
                nearest_points[station['name']] = (center_lat_idx, center_lon_idx)

                # 記錄最近網格點的信息
                nearest_lat = lat_grid[center_lat_idx, center_lon_idx]
                nearest_lon = lon_grid[center_lat_idx, center_lon_idx]
                distance = distances[center_lat_idx, center_lon_idx]

                self.logger.debug(f"Station {station['name']}: "
                                  f"Target({station['lat']:.3f}, {station['lon']:.3f}) -> "
                                  f"Grid({nearest_lat:.3f}, {nearest_lon:.3f}), "
                                  f"Distance: {distance:.3f}°")
            else:
                # 返回3x3網格的所有點
                surrounding_points = []

                # 定義3x3網格的相對偏移 (按行優先順序)
                offsets = [(-1, -1), (-1, 0), (-1, 1),
                           (0, -1), (0, 0), (0, 1),
                           (1, -1), (1, 0), (1, 1)]

                valid_points = 0
                for d_lat, d_lon in offsets:
                    new_lat_idx = center_lat_idx + d_lat
                    new_lon_idx = center_lon_idx + d_lon

                    # 檢查邊界
                    if (0 <= new_lat_idx < grid_shape[0] and
                            0 <= new_lon_idx < grid_shape[1]):
                        surrounding_points.append((new_lat_idx, new_lon_idx))
                        valid_points += 1
                    else:
                        # 超出邊界的點用 None 標記
                        surrounding_points.append(None)

                nearest_points[station['name']] = surrounding_points

                # 記錄信息
                center_lat = lat_grid[center_lat_idx, center_lon_idx]
                center_lon = lon_grid[center_lat_idx, center_lon_idx]
                center_distance = distances[center_lat_idx, center_lon_idx]

                self.logger.debug(f"Station {station['name']}: "
                                  f"Target({station['lat']:.3f}, {station['lon']:.3f}) -> "
                                  f"Center({center_lat:.3f}, {center_lon:.3f}), "
                                  f"Distance: {center_distance:.3f}°, "
                                  f"Valid surrounding points: {valid_points}/9")

            # 如果距離太遠，給出警告
            center_distance = distances[center_lat_idx, center_lon_idx]
            if center_distance > 0.1:  # 約11公里
                self.logger.warning(f"Station {station['name']} is {center_distance:.3f}° "
                                    f"({center_distance * 111:.1f}km) from nearest grid point")

        return nearest_points

    def _extract_single_file_data(self, nc_file, stations, nearest_grid_points, extract_surrounding=False):
        """
        從單個文件提取站點數據

        Parameters:
            nc_file (Path): NetCDF 文件路徑
            stations (list): 站點列表
            nearest_grid_points (dict): 預計算的最近網格點
            extract_surrounding (bool): 是否提取周圍8格的數據

        Returns:
            dict: 包含時間和各站點數據的字典，失敗返回 None
        """
        try:
            import xarray as xr

            # 打開數據集
            ds = xr.open_dataset(nc_file, chunks='auto')

            # 獲取數據變量名
            var_name = PRODUCT_CONFIGS[self.file_type].dataset_name

            if var_name not in ds:
                self.logger.warning(f"Variable {var_name} not found in {nc_file}")
                ds.close()
                return None

            # 獲取時間信息
            time_value = pd.to_datetime(ds.time.values[0])

            # 準備結果數據
            result = {
                'time': time_value,
            }

            # 提取各站點數據
            data_values = ds[var_name][0].values  # 取第一個時間點的數據

            for station in stations:
                station_name = station['name']
                if station_name not in nearest_grid_points:
                    if extract_surrounding:
                        # 為每個周圍格點創建列
                        for i in range(9):
                            result[f"{station_name}_grid_{i}"] = None
                        result[f"{station_name}_mean"] = None
                        result[f"{station_name}_std"] = None
                    else:
                        result[station_name] = None
                    continue

                if not extract_surrounding:
                    # 原有的單點提取邏輯
                    lat_idx, lon_idx = nearest_grid_points[station_name]
                    station_value = self._extract_value_at_point(ds, data_values, lat_idx, lon_idx)

                    # 檢查是否為有效值
                    if np.isnan(station_value) or np.isinf(station_value):
                        result[station_name] = None
                    else:
                        result[station_name] = float(station_value)

                else:
                    # 提取3x3網格的數據
                    grid_points = nearest_grid_points[station_name]
                    surrounding_values = []

                    # 提取每個網格點的數據
                    for i, point in enumerate(grid_points):
                        column_name = f"{station_name}_grid_{i}"

                        if point is None:
                            # 超出邊界的點
                            result[column_name] = None
                            surrounding_values.append(np.nan)
                        else:
                            lat_idx, lon_idx = point
                            station_value = self._extract_value_at_point(ds, data_values, lat_idx, lon_idx)

                            # 檢查是否為有效值
                            if np.isnan(station_value) or np.isinf(station_value):
                                result[column_name] = None
                                surrounding_values.append(np.nan)
                            else:
                                result[column_name] = float(station_value)
                                surrounding_values.append(station_value)

                    # 計算統計量
                    valid_values = [v for v in surrounding_values if not np.isnan(v)]
                    if valid_values:
                        result[f"{station_name}_mean"] = float(np.mean(valid_values))
                        result[f"{station_name}_std"] = float(np.std(valid_values)) if len(valid_values) > 1 else 0.0
                    else:
                        result[f"{station_name}_mean"] = None
                        result[f"{station_name}_std"] = None

            ds.close()
            return result

        except Exception as e:
            self.logger.error(f"Error extracting data from {nc_file}: {str(e)}")
            return None

    def _extract_value_at_point(self, ds, data_values, lat_idx, lon_idx):
        """
        在指定網格點提取數值的輔助函數

        Parameters:
            ds: xarray Dataset
            data_values: 數據數組
            lat_idx, lon_idx: 網格索引

        Returns:
            float: 提取的數值，失敗返回 np.nan
        """
        try:
            if data_values.ndim == 2:
                return data_values[lat_idx, lon_idx]
            elif data_values.ndim == 1:
                # 如果是一維數組，需要計算線性索引
                lats = ds.latitude.values if 'latitude' in ds.coords else ds.lat.values
                lons = ds.longitude.values if 'longitude' in ds.coords else ds.lon.values

                if lats.ndim == 2:
                    linear_idx = lat_idx * data_values.shape[-1] + lon_idx
                else:
                    linear_idx = lat_idx * len(lons) + lon_idx

                if linear_idx < len(data_values):
                    return data_values[linear_idx]
                else:
                    return np.nan
            else:
                self.logger.error(f"Unsupported data array dimensions: {data_values.ndim}")
                return np.nan

        except (IndexError, ValueError) as e:
            self.logger.warning(f"Error extracting value at ({lat_idx}, {lon_idx}): {e}")
            return np.nan

    def process_files_to_csv(self, stations, file_pattern=None, start_date=None, end_date=None,
                             output_file=None, fill_missing_dates=True, extract_surrounding=False):
        """
        批量處理文件並提取站點數據到單個 CSV 文件

        Parameters:
            stations (list): 站點列表，每個站點包含 name, lat, lon
            file_pattern (str): 文件匹配模式
            start_date (str or datetime): 開始日期
            end_date (str or datetime): 結束日期
            output_file (str or Path): 輸出 CSV 文件路徑，如果為 None 則自動生成
            fill_missing_dates (bool): 是否填補缺失日期
            extract_surrounding (bool): 是否提取站點周圍8格的數據（3x3網格）

        Returns:
            str: 生成的 CSV 文件路徑，失敗返回 None
        """
        if not stations:
            self.logger.error("No stations provided")
            return None

        try:
            # 設置默認文件模式
            if file_pattern is None:
                if hasattr(self, 'file_class') and self.file_class:
                    file_pattern = f"**/{self.file_type}/**/*{self.file_class}*.nc"
                else:
                    file_pattern = f"**/{self.file_type}/**/*.nc"

            # 處理日期格式
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d')

            # 找到所有符合條件的已處理文件
            processed_files = []
            if hasattr(self, 'processed_dir') and self.processed_dir.exists():
                all_files = [f for f in self.processed_dir.glob(file_pattern)
                             if not f.name.startswith("._") and f.is_file()]

                # 根據日期範圍過濾文件
                for file_path in all_files:
                    try:
                        file_date = extract_datetime_from_filename(file_path.name, to_local=False)

                        if start_date and file_date < start_date:
                            continue
                        if end_date and file_date > end_date:
                            continue

                        processed_files.append(file_path)
                    except:
                        continue

            if not processed_files:
                self.logger.warning("No processed files found matching criteria")
                return None

            # 按時間排序文件
            processed_files.sort(key=lambda f: extract_datetime_from_filename(f.name, to_local=False))

            self.logger.info(f"Found {len(processed_files)} processed files to extract station data")
            if extract_surrounding:
                self.logger.info("Will extract 3x3 grid (9 points) around each station")

            # 使用第一個文件計算網格點位置（假設所有文件的網格相同）
            self.logger.info("Calculating nearest grid points for stations...")

            first_file = processed_files[0]
            ds_sample = xr.open_dataset(first_file, chunks='auto')

            # 檢查坐標
            if 'latitude' in ds_sample.coords and 'longitude' in ds_sample.coords:
                lats = ds_sample.latitude.values
                lons = ds_sample.longitude.values
            elif 'lat' in ds_sample.coords and 'lon' in ds_sample.coords:
                lats = ds_sample.lat.values
                lons = ds_sample.lon.values
            else:
                self.logger.error("Cannot find latitude/longitude coordinates")
                ds_sample.close()
                return None

            nearest_grid_points = self._calculate_nearest_grid_points(stations, lats, lons, extract_surrounding)
            ds_sample.close()

            # 提取所有文件的數據
            all_data = []
            successful_extractions = 0

            self.logger.info("Extracting station data from all files...")
            for i, nc_file in enumerate(processed_files):
                result = self._extract_single_file_data(nc_file, stations, nearest_grid_points, extract_surrounding)

                if result:
                    all_data.append(result)
                    successful_extractions += 1

                # 進度報告
                if (i + 1) % 50 == 0:
                    self.logger.info(f"Processed {i + 1}/{len(processed_files)} files")

            if not all_data:
                self.logger.error("No valid data extracted from any files")
                return None

            self.logger.info(f"Successfully extracted data from {successful_extractions}/{len(processed_files)} files")

            # 創建 DataFrame
            df = pd.DataFrame(all_data)
            df = df.sort_values('time').reset_index(drop=True)

            # 填補缺失日期（可選）
            if fill_missing_dates and start_date and end_date:
                self.logger.info("Filling missing dates...")

                # 創建完整的日期範圍
                full_date_range = pd.date_range(start=start_date, end=end_date, freq='D')

                # 找出缺失的日期
                existing_dates = set(df['time'].dt.date)
                missing_dates = [d for d in full_date_range if d.date() not in existing_dates]

                if missing_dates:
                    self.logger.info(f"Found {len(missing_dates)} missing dates, filling with NaN")

                    # 創建缺失日期的數據
                    if extract_surrounding:
                        # 為3x3網格創建列
                        station_columns = []
                        for station in stations:
                            station_name = station['name']
                            for i in range(9):
                                station_columns.append(f"{station_name}_grid_{i}")
                            station_columns.extend([f"{station_name}_mean", f"{station_name}_std"])
                    else:
                        station_columns = [station['name'] for station in stations]

                    missing_data = []
                    for missing_date in missing_dates:
                        missing_row = {'time': missing_date}
                        for col_name in station_columns:
                            missing_row[col_name] = np.nan
                        missing_data.append(missing_row)

                    # 合併數據 - 只有在有缺失數據時才合併
                    if missing_data:
                        missing_df = pd.DataFrame(missing_data)
                        # 確保兩個 DataFrame 有相同的列
                        missing_df = missing_df.reindex(columns=df.columns, fill_value=np.nan)
                        df = pd.concat([df, missing_df], ignore_index=True)
                        df = df.sort_values('time').reset_index(drop=True)

            # 重新排列列的順序（time 在前，然後是按字母順序的站點相關列）
            time_col = ['time']
            if extract_surrounding:
                # 按站點名稱排序，每個站點包含grid_0到grid_8, mean, std
                station_cols = []
                for station_name in sorted([station['name'] for station in stations]):
                    for i in range(9):
                        station_cols.append(f"{station_name}_grid_{i}")
                    station_cols.extend([f"{station_name}_mean", f"{station_name}_std"])
            else:
                station_cols = sorted([station['name'] for station in stations])

            df = df[time_col + station_cols]

            # 生成輸出文件路徑
            if output_file is None:
                # 創建 CSV 輸出目錄
                csv_dir = self.processed_dir / "csv"
                csv_dir.mkdir(parents=True, exist_ok=True)

                # 生成文件名
                start_str = df['time'].min().strftime('%Y%m%d')
                end_str = df['time'].max().strftime('%Y%m%d')
                var_name = PRODUCT_CONFIGS[self.file_type].dataset_name

                if extract_surrounding:
                    output_filename = f"{self.file_type}_{var_name}_stations_3x3_{start_str}_{end_str}.csv"
                else:
                    output_filename = f"{self.file_type}_{var_name}_stations_{start_str}_{end_str}.csv"

                output_file = csv_dir / output_filename
            else:
                output_file = Path(output_file)

            # 確保輸出目錄存在
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # 保存 CSV
            df.to_csv(output_file, index=False)

            # 報告統計信息
            self.logger.info("=" * 60)
            self.logger.info("Station Data Extraction Summary:")
            self.logger.info(f"  Output file: {output_file}")
            self.logger.info(f"  Time range: {df['time'].min()} to {df['time'].max()}")
            self.logger.info(f"  Total time points: {len(df)}")
            self.logger.info(f"  Stations: {len(stations)}")

            if extract_surrounding:
                self.logger.info(f"  Data type: 3x3 grid extraction (9 points + statistics per station)")
                self.logger.info(f"  Total columns: {len(df.columns)} (time + {len(df.columns) - 1} data columns)")

                # 計算數據完整性（只統計mean列）
                mean_cols = [col for col in df.columns if col.endswith('_mean')]
                total_possible_values = len(df) * len(mean_cols)
                valid_values = df[mean_cols].count().sum()
                completeness = (valid_values / total_possible_values) * 100
                self.logger.info(
                    f"  Data completeness (mean values): {completeness:.1f}% ({valid_values}/{total_possible_values})")

                # 各站點數據統計
                self.logger.info("  Station-wise data availability (mean values):")
                for station_name in sorted([station['name'] for station in stations]):
                    mean_col = f"{station_name}_mean"
                    if mean_col in df.columns:
                        station_valid = df[mean_col].count()
                        station_rate = station_valid / len(df) * 100
                        self.logger.info(
                            f"    {station_name:>15}: {station_valid:>3}/{len(df)} ({station_rate:>5.1f}%)")

            else:
                self.logger.info(f"  Data type: Single point extraction")
                station_cols_simple = [col for col in df.columns if col != 'time']
                total_possible_values = len(df) * len(station_cols_simple)
                valid_values = df[station_cols_simple].count().sum()
                completeness = (valid_values / total_possible_values) * 100
                self.logger.info(f"  Data completeness: {completeness:.1f}% ({valid_values}/{total_possible_values})")

                # 各站點數據統計
                self.logger.info("  Station-wise data availability:")
                for station_name in sorted([station['name'] for station in stations]):
                    if station_name in df.columns:
                        station_valid = df[station_name].count()
                        station_rate = station_valid / len(df) * 100
                        self.logger.info(
                            f"    {station_name:>15}: {station_valid:>3}/{len(df)} ({station_rate:>5.1f}%)")

            self.logger.info("=" * 60)

            return str(output_file)

        except Exception as e:
            self.logger.error(f"Error in station data extraction: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None

    def debug_dataset_structure(self, dataset_or_file):
        """
        調試函數：檢查數據集結構

        Parameters:
            dataset_or_file: xarray Dataset 或文件路徑
        """
        try:
            # 處理輸入數據
            if isinstance(dataset_or_file, (str, Path)):
                ds = xr.open_dataset(dataset_or_file, chunks='auto')
                should_close = True
                print(f"Debugging file: {dataset_or_file}")
            else:
                ds = dataset_or_file
                should_close = False
                print("Debugging dataset object")

            print("\n=== Dataset Information ===")
            print(f"Dimensions: {dict(ds.dims)}")
            print(f"Coordinates: {list(ds.coords.keys())}")
            print(f"Data variables: {list(ds.data_vars.keys())}")

            # 檢查主要變量
            var_name = PRODUCT_CONFIGS[self.file_type].dataset_name
            if var_name in ds:
                var_data = ds[var_name]
                print(f"\n=== Variable '{var_name}' ===")
                print(f"Dimensions: {var_data.dims}")
                print(f"Shape: {var_data.shape}")
                print(f"Data type: {var_data.dtype}")

            # 檢查經緯度坐標
            lat_coords = [coord for coord in ds.coords if 'lat' in coord.lower()]
            lon_coords = [coord for coord in ds.coords if 'lon' in coord.lower()]

            print(f"\n=== Coordinate Information ===")
            print(f"Latitude coordinates found: {lat_coords}")
            print(f"Longitude coordinates found: {lon_coords}")

            for coord_name in lat_coords + lon_coords:
                coord_data = ds[coord_name]
                print(f"{coord_name}: shape={coord_data.shape}, dtype={coord_data.dtype}")
                if coord_data.size < 10:
                    print(f"  Values: {coord_data.values}")
                else:
                    print(f"  Range: {coord_data.min().values:.3f} to {coord_data.max().values:.3f}")

            # 檢查時間坐標
            if 'time' in ds.coords:
                print(f"\n=== Time Information ===")
                time_coord = ds.time
                print(f"Time shape: {time_coord.shape}")
                print(f"Time values: {time_coord.values}")

            if should_close:
                ds.close()

        except Exception as e:
            print(f"Error debugging dataset: {str(e)}")
            import traceback
            traceback.print_exc()

    def _process_data(self, ds: xr.Dataset, file_path: Path) -> xr.Dataset | None:
        """處理數據並返回插值後的數據集"""
        try:
            # 1. 記錄基本信息
            self.nc_info = {'file_name': file_path.name}

            # 2. 提取數據
            mask_dataset, lon, lat, var = self.extract_data(ds, extract_range=FIGURE_BOUNDARY)
            if any(x is None for x in [lon, lat, var]):
                self.logger.error("Failed to extract data")
                return None

            # 觀察不同q_value下的原始圖
            # for q in [0.5, 0.75, 0.85, 0.9]:
            #     if q == 0.5:
            #         _dataset = mask_dataset
            #     else:
            #         _dataset = mask_dataset.where((mask_dataset.qa_value >= q))
            #
            #     plot_global_var(dataset=_dataset,
            #                     product_params=PRODUCT_CONFIGS[self.file_type],
            #                     map_scale='Taiwan')

            # 2.5 檢查竹苗中部空品區是否有數據，如果指定區域內沒有數據點，返回 None
            self.lat_range = (24.0, 25.0)  # 北緯24.0度至25度
            self.lon_range = (120.5, 121.5)  # 東經120.5度至121.5度
            mask_lon = (lon >= self.lon_range[0]) & (lon <= self.lon_range[1])
            mask_lat = (lat >= self.lat_range[0]) & (lat <= self.lat_range[1])
            region_mask = mask_lon & mask_lat

            if not np.any(region_mask) or np.all(np.isnan(var[region_mask])):
                self.logger.info(f"No valid data in specified region {self.lat_range}, {self.lon_range}")

                try:
                    # 直接從原始檔案路徑獲取年月信息
                    year = file_path.parent.parent.name
                    month = file_path.parent.name
                    file_name = file_path.name

                    # 構建要清理的路徑
                    clean_paths = {
                        'raw_data': self.raw_dir / self.file_type / year / month / f"{file_name}.nc",
                        'output': self.processed_dir / self.file_type / year / month / f"{file_name}.nc",
                        'figure': self.figure_dir / self.file_type / year / month / f"{file_path.stem}.png",
                        # 'geotiff': self.geotiff_dir / self.file_type / year / month / f"{file_path.stem}.tiff"
                    }

                    # 刪除對應的檔案
                    for path_type, file_path_to_delete in clean_paths.items():
                        if file_path_to_delete.exists():
                            file_path_to_delete.unlink()
                            self.logger.info(f"刪除 {path_type} 檔案: {file_path_to_delete}")

                    self.logger.info(f"清理完成: 沒有區域內的有效數據")
                except Exception as e:
                    self.logger.error(f"清理檔案時出錯: {e}")

                return None

            # 3. 創建網格並插值
            lon_grid, lat_grid = self.grid_frame.get_grid(custom_bounds=FIGURE_BOUNDARY)
            
            # 動態調整 max_distance 根據數據密度（以格數為單位）
            valid_data_count = np.sum(~np.isnan(var))
            
            # 計算網格解析度（用於格數換算）
            grid_resolution_lon = abs(lon_grid[0, 1] - lon_grid[0, 0]) if lon_grid.shape[1] > 1 else 0.01
            grid_resolution_lat = abs(lat_grid[1, 0] - lat_grid[0, 0]) if lat_grid.shape[0] > 1 else 0.01
            cell_deg = (grid_resolution_lon + grid_resolution_lat) / 2.0
            
            if valid_data_count < 100:
                max_distance = 2 * cell_deg  # 數據稀少時，覆蓋2格
            elif valid_data_count < 500:
                max_distance = 3 * cell_deg  # 中等密度時，覆蓋3格
            else:
                max_distance = 4 * cell_deg  # 數據充足時，覆蓋4格
            
            self.logger.debug(f"使用 max_distance={max_distance:.3f}° (約 {max_distance*111:.1f}km, 覆蓋{max_distance/cell_deg:.1f}格), 有效數據點: {valid_data_count}")
            
            var_grid = DataInterpolator.interpolate(
                lon, lat, var,
                lon_grid, lat_grid,
                method=self.interpolation_method,
                max_distance=max_distance,
                rbf_function='thin_plate'
            )

            # 4. 創建數據集
            return xr.Dataset(
                {
                    PRODUCT_CONFIGS[self.file_type].dataset_name: (
                        ['time', 'latitude', 'longitude'],
                        var_grid[np.newaxis, :, :]
                    )
                },
                coords={
                    'time': ds.time.values[0:1],
                    'latitude': np.squeeze(lat_grid[:, 0]),
                    'longitude': np.squeeze(lon_grid[0, :])
                },
                attrs={
                    'units': PRODUCT_CONFIGS[self.file_type].units,
                    'time': str(ds.time.values[0]),
                    'description': PRODUCT_CONFIGS[self.file_type].title,
                    'processing_method': self.interpolation_method,
                    'resolution': self.resolution,
                }
            )
        except Exception as e:
            self.logger.error(f"Error in data processing: {str(e)}")
            return None

    def _save_outputs(self, ds: xr.Dataset, file_path: Path, output_dir: Path, geotiff_dir: Path):
        """保存處理結果"""
        try:
            # 保存 NetCDF
            output_nc_path = output_dir / file_path.name
            ds.to_netcdf(output_nc_path)
            # logger.info(f"Saved NetCDF to {output_nc_path}")

            # 保存 GeoTIFF
            # output_tiff_path = geotiff_dir / file_path.stem
            # self.save_as_tiff(ds, output_tiff_path)
            # logger.info(f"Saved GeoTIFF to {output_tiff_path}")

        except Exception as e:
            self.logger.error(f"Error saving outputs: {str(e)}")

    def process_all_files(self, pattern=None, start_date=None, end_date=None, skip_existing=False):
        """
        處理日期範圍內的所有衛星數據文件

        Parameters:
            pattern (str): 文件匹配模式，如果為 None 則根據文件類型自動選擇
            start_date (str or datetime): 處理的開始日期，格式為 'YYYY-MM-DD' 或 datetime 對象
            end_date (str or datetime): 處理的結束日期，格式為 'YYYY-MM-DD' 或 datetime 對象

        Returns:
            bool: 處理是否成功
        """
        # 設置默認值和進行類型轉換
        if pattern is None:
            if hasattr(self, 'file_class') and self.file_class:
                pattern = f"**/{self.file_type}/**/*{self.file_class}*.nc"
            else:
                pattern = f"**/{self.file_type}/**/*.nc"

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
            self.logger.info("沒有找到符合條件的文件")
            return False

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
                'output': self.processed_dir / self.file_type / year / month,
                'figure': self.figure_dir / self.file_type / year / month,
                'geotiff': self.geotiff_dir / self.file_type / year / month
            }

            # 創建目錄
            for dir_path in paths.values():
                if dir_path != paths['input']:  # 不創建輸入目錄
                    dir_path.mkdir(parents=True, exist_ok=True)

            # 處理該月的所有文件
            month_processed = 0
            self.logger.info(f"處理 {year}-{month} 的 {len(month_files)} 個文件")

            for nc_file in month_files:
                try:
                    result = self.process_nc_file(nc_file, paths['output'], paths['geotiff'], skip_existing)
                    if result:
                        # 文件處理成功後立即創建圖像
                        try:
                            output_file = paths['output'] / nc_file.name
                            figure_path = paths['figure'] / f"{nc_file.stem}.png"

                            self.logger.info(f"正在創建圖像: {figure_path}")

                            plot_global_var(
                                dataset=output_file,
                                product_params=PRODUCT_CONFIGS[self.file_type],
                                savefig_path=figure_path,
                                map_scale='Taiwan',
                                mark_stations=None
                            )
                        except Exception as e:
                            self.logger.error(f"繪製檔案 {nc_file.name} 時發生錯誤: {e}")

                        month_processed += 1
                        processed_count += 1
                except Exception as e:
                    self.logger.error(f"處理檔案 {nc_file.name} 時發生錯誤: {e}")

            # 創建動畫
            if month_processed > 0:
                try:
                    # 創建動畫
                    animation_path = paths['figure'] / f"{self.file_type}_{self.file_class}_{year}{month}_animation.gif"
                    animate_data(
                        image_dir=paths['figure'],
                        output_path=animation_path,
                        date_type="s5p" if self.file_type == "S5P" else "auto",
                        fps=2
                    )
                    self.logger.info(f"創建動畫: {animation_path}")
                except Exception as e:
                    self.logger.error(f"創建 {year}-{month} 的動畫時發生錯誤: {e}")

        self.logger.info(f"處理完成! 成功處理 {processed_count} 個檔案，共 {len(files_by_month)} 個月。")
        return processed_count > 0

    def save_as_tiff(self, ds: xr.Dataset, output_path: Path) -> None:
        """將 NetCDF 數據集中的 NO2 數值儲存為 GeoTIFF"""
        try:
            # 獲取變數名稱和數據
            var_name = PRODUCT_CONFIGS[self.file_type].dataset_name
            da = ds[var_name].isel(time=0)

            # 設定地理資訊
            da.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude", inplace=True)
            da.rio.write_crs("EPSG:4326", inplace=True)

            # 儲存為 GeoTIFF
            tiff_path = output_path.with_suffix('.tiff')
            da.rio.to_raster(
                tiff_path,
                driver='GTiff'
            )

        except Exception as e:
            self.logger.error(f"Error saving TIFF file {output_path}: {str(e)}")