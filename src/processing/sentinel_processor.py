import numpy as np
import xarray as xr
from datetime import datetime
from pathlib import Path

from src.processing.interpolators import DataInterpolator
from src.processing.grid_frame import GridFrame
from src.config.settings import FIGURE_BOUNDARY
from src.config.catalog import PRODUCT_CONFIGS
from src.visualization.plot_nc import plot_global_var
from src.visualization.gif import animate_data
from src.utils.extract_datetime_from_filename import extract_datetime_from_filename


class SentinelProcessor:
    """處理 S5P 數據並生成可視化圖像"""
    def __init__(self, interpolation_method='kdtree', resolution=(5.5, 3.5), mask_qc_value=0.75):
        """初始化處理器

        Parameters:
        -----------
        interpolation_method : str
            插值方法，可選 'griddata' 或 'kdtree' 或 'rbf'
        resolution : tuple
            網格解析度，格式為 (x_km, y_km)，例如 (5.5, 3.5) 代表 5.5km x 3.5km
        mask_qc_value : float
            QA 值的閾值
        """
        self.raw_dir = None
        self.processed_dir = None
        self.figure_dir = None
        self.geotiff_dir = None
        self.logger = None
        self.file_type = None
        self.file_class = None

        self.interpolation_method = interpolation_method
        self.resolution = resolution
        self.mask_qc_value = mask_qc_value
        self.grid_frame = GridFrame(resolution)

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

        info_dict = {
            'time': f"{time}",
            'shape': f"{shape}",
            'latitude': f'{np.nanmin(lat):.2f} to {np.nanmax(lat):.2f}',
            'longitude': f'{np.nanmin(lon):.2f} to {np.nanmax(lon):.2f}',
        }

        if hasattr(self, 'nc_info'):
            self.nc_info.update(info_dict)

        return mask_dataset, lon, lat, var

    def process_nc_file(self, nc_file: Path, output_dir: Path, geotiff_dir: Path):
        """處理單個 nc 檔"""
        # 從文件名提取日期
        file_date = extract_datetime_from_filename(nc_file.name, to_local=False)
        self.logger.info(f"處理文件: {nc_file.name} ({file_date.strftime('%Y-%m-%d')})")

        # 打開 nc 文件
        ds = xr.open_dataset(nc_file, engine='netcdf4', group='PRODUCT')

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
                        'geotiff': self.geotiff_dir / self.file_type / year / month / f"{file_path.stem}.tiff"
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
            var_grid = DataInterpolator.interpolate(
                lon, lat, var,
                lon_grid, lat_grid,
                method=self.interpolation_method,
                max_distance=0.2,
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
                    'units': 'mol/m2',
                    'time': str(ds.time.values[0]),
                    'description': 'Sentinel-5P NO2 Tropospheric Column',
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
            output_tiff_path = geotiff_dir / file_path.stem
            self.save_as_tiff(ds, output_tiff_path)
            # logger.info(f"Saved GeoTIFF to {output_tiff_path}")

        except Exception as e:
            self.logger.error(f"Error saving outputs: {str(e)}")

    def process_all_files(self, pattern=None, start_date=None, end_date=None):
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
                    result = self.process_nc_file(nc_file, paths['output'], paths['geotiff'])
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
                        fps=1
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