"""src/processing/no2_processor.py"""
import logging
import numpy as np
import xarray as xr
import rioxarray
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

from src.processing.interpolators import DataInterpolator
from src.processing.grid_frame import GridFrame
from src.config.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR, FIGURE_DIR, GEOTIFF_DIR, FIGURE_BOUNDARY
from src.config.catalog import ClassInput, TypeInput, PRODUCT_CONFIGS
from src.config.richer import DisplayManager
from src.visualization.plot_nc import plot_global_var

logger = logging.getLogger(__name__)


class S5Processor:
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
        attributes = PRODUCT_CONFIGS[self.product_type].dataset_name

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

    def process_single_file(self, file_path: Path, output_dir: Path, geotiff_dir: Path):
        """處理單一檔案"""
        ds = xr.open_dataset(file_path, engine='netcdf4', group='PRODUCT')

        try:
            interpolated_ds = self._process_data(ds, file_path)
            if interpolated_ds is not None:
                self._save_outputs(interpolated_ds, file_path, output_dir, geotiff_dir)
        except Exception as e:
            logger.error(f"Error processing file {file_path.name}: {str(e)}")
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
                logger.error("Failed to extract data")
                return None

            # 觀察不同q_value下的原始圖
            # for q in [0.5, 0.75, 0.85, 0.9]:
            #     if q == 0.5:
            #         _dataset = mask_dataset
            #     else:
            #         _dataset = mask_dataset.where((mask_dataset.qa_value >= q))
            #
            #     plot_global_var(dataset=_dataset,
            #                     product_params=PRODUCT_CONFIGS[self.product_type],
            #                     map_scale='Taiwan')

            # 2.5 檢查竹苗中部空品區是否有數據，如果指定區域內沒有數據點，返回 None
            self.lat_range = (24.0, 25.0)  # 北緯24.0度至25度
            self.lon_range = (120.5, 121.5)  # 東經120.5度至121.5度
            mask_lon = (lon >= self.lon_range[0]) & (lon <= self.lon_range[1])
            mask_lat = (lat >= self.lat_range[0]) & (lat <= self.lat_range[1])
            region_mask = mask_lon & mask_lat

            if not np.any(region_mask) or np.all(np.isnan(var[region_mask])):
                logger.info(f"No valid data in specified region {self.lat_range}, {self.lon_range}")

                try:
                    # 直接從原始檔案路徑獲取年月信息
                    year = file_path.parent.parent.name
                    month = file_path.parent.name
                    file_name = file_path.name

                    # 構建要清理的路徑
                    clean_paths = {
                        'raw_data': RAW_DATA_DIR / self.product_type / year / month / f"{file_name}.nc",
                        'output': PROCESSED_DATA_DIR / self.product_type / year / month / f"{file_name}.nc",
                        'figure': FIGURE_DIR / self.product_type / year / month / f"{file_path.stem}.png",
                        'geotiff': GEOTIFF_DIR / self.product_type / year / month / f"{file_path.stem}.tiff"
                    }

                    # 刪除對應的檔案
                    for path_type, file_path_to_delete in clean_paths.items():
                        if file_path_to_delete.exists():
                            file_path_to_delete.unlink()
                            logger.info(f"刪除 {path_type} 檔案: {file_path_to_delete}")

                    logger.info(f"清理完成: 沒有區域內的有效數據")
                except Exception as e:
                    logger.error(f"清理檔案時出錯: {e}")

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
                    PRODUCT_CONFIGS[self.product_type].dataset_name: (
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
            logger.error(f"Error in data processing: {str(e)}")
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
            logger.error(f"Error saving outputs: {str(e)}")

    def process_each_data(self, file_class: ClassInput, file_type: TypeInput,
                          start_date: str, end_date: str):
        """處理指定日期範圍內的衛星數據，限定於23.5N-25N, 120E-122E區域"""
        self.product_type = file_type
        self.file_pattern = f"*{file_class}_L2__{file_type}*.nc"
        self.start = datetime.strptime(start_date, '%Y-%m-%d')
        self.end = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = self.start

        while current_date <= self.end:
            year, month = current_date.strftime('%Y'), current_date.strftime('%m')

            # 設置目錄
            paths = {
                'input': RAW_DATA_DIR / file_type / year / month,
                'output': PROCESSED_DATA_DIR / file_type / year / month,
                'figure': FIGURE_DIR / file_type / year / month,
                'geotiff': GEOTIFF_DIR / file_type / year / month
            }

            # 創建目錄
            for dir_path in paths.values():
                if dir_path != paths['input']:  # 不創建輸入目錄
                    dir_path.mkdir(parents=True, exist_ok=True)

            # 處理檔案
            if paths['input'].exists():
                self._process_files(paths)
                self._create_figures(paths)

            current_date = (current_date + relativedelta(months=1)).replace(day=1)

    def _process_files(self, paths: dict):
        """處理原始檔案"""
        for file_path in paths['input'].glob(self.file_pattern):
            if not file_path.is_file() or file_path.name.startswith('._'):
                continue

            if not (self.start <= datetime.strptime(file_path.name[20:28], '%Y%m%d') <= self.end):
                continue

            try:
                self.process_single_file(file_path, paths['output'], paths['geotiff'])
            except Exception as e:
                logger.error(f"處理檔案 {file_path.name} 時發生錯誤: {e}")

    def _create_figures(self, paths: dict):
        """創建圖表"""
        for file_path in paths['output'].glob(self.file_pattern):
            if not file_path.is_file() or file_path.name.startswith('._'):
                continue

            if not (self.start <= datetime.strptime(file_path.name[20:28], '%Y%m%d') <= self.end):
                continue

            try:
                figure_path = paths['figure'] / f"{file_path.stem}.png"
                plot_global_var(
                    dataset=file_path,
                    product_params=PRODUCT_CONFIGS[self.product_type],
                    savefig_path=figure_path,
                    map_scale='Taiwan',
                )
            except Exception as e:
                logger.error(f"繪製檔案 {file_path.name} 時發生錯誤: {e}")

    def save_as_tiff(self, ds: xr.Dataset, output_path: Path) -> None:
        """將 NetCDF 數據集中的 NO2 數值儲存為 GeoTIFF"""
        try:
            # 獲取變數名稱和數據
            var_name = PRODUCT_CONFIGS[self.product_type].dataset_name
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
            logger.error(f"Error saving TIFF file {output_path}: {str(e)}")