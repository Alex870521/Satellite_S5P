"""src/processing/no2_processor.py"""
import numpy as np
import xarray as xr
import logging
import zipfile
import tempfile
from pathlib import Path
from datetime import datetime

from src.processing.interpolators import DataInterpolator
from src.processing.taiwan_frame import TaiwanFrame
from src.config.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR, FIGURE_DIR
from src.visualization.sample_plot_nc import plot_global_no2

logger = logging.getLogger(__name__)


class NO2Processor:
    def __init__(self, interpolation_method='kdtree', resolution=0.02, mask_value=0.50):
        """初始化 NO2 處理器

        Parameters:
        -----------
        interpolation_method : str
            插值方法，可選 'griddata' 或 'kdtree'
        resolution : float
            網格解析度（度）
        mask_value : float
            QA 值的閾值
        """
        self.interpolation_method = interpolation_method
        self.resolution = resolution
        self.mask_value = mask_value
        self.taiwan_frame = TaiwanFrame()

    @staticmethod
    def process_zipped_nc(zip_path: Path):
        """處理壓縮的 NC 檔案"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            nc_files = list(temp_dir_path.rglob("*.nc"))
            if nc_files:
                return xr.open_dataset(nc_files[0], group='PRODUCT')
        return None

    def create_grid(self, lon: np.ndarray, lat: np.ndarray):
        """根據數據的經緯度範圍創建網格"""
        # 取得經緯度的範圍
        lon_min, lon_max = np.nanmin(lon), np.nanmax(lon)
        lat_min, lat_max = np.nanmin(lat), np.nanmax(lat)

        # 創建網格點
        grid_lon = np.arange(lon_min, lon_max + self.resolution, self.resolution)
        grid_lat = np.arange(lat_min, lat_max + self.resolution, self.resolution)

        # 創建網格矩陣
        return np.meshgrid(grid_lon, grid_lat)

    def extract_data(self, dataset: xr.Dataset, use_taiwan_mask: bool = False):
        """從數據集中提取數據

        Parameters:
        -----------
        dataset : xr.Dataset
            輸入的數據集
        use_taiwan_mask : bool
            是否只提取台灣區域的數據
        """
        if use_taiwan_mask:
            return self._extract_data_taiwan(dataset)
        return self._extract_data_global(dataset)

    def _extract_data_global(self, dataset: xr.Dataset):
        """提取全球範圍的數據"""
        time = dataset.time.values[0]
        lat = dataset.latitude.values[0]
        lon = dataset.longitude.values[0]
        no2 = dataset.nitrogendioxide_tropospheric_column.values[0]
        qa = dataset.qa_value.values[0]

        # 根據 QA 值過濾數據
        mask = qa < self.mask_value
        no2[mask] = np.nan

        logger.info(f"\t{'data time':15}: {np.datetime64(time, 'D').astype(str)}")
        logger.info(f"\t{'lon range':15}: {lon.min():.2f} to {lon.max():.2f}")
        logger.info(f"\t{'lat range':15}: {lat.min():.2f} to {lat.max():.2f}")
        logger.info(f"\t{'data shape':15}: {no2.shape}")

        return lon, lat, no2

    def _extract_data_taiwan(self, dataset: xr.Dataset):
        """提取台灣區域的數據"""
        # 設定條件
        mask_lon = ((dataset.longitude >= 118) & (dataset.longitude <= 124))
        mask_lat = ((dataset.latitude >= 20) & (dataset.latitude <= 27))
        masked_lon_lat_ds = dataset.where((mask_lon & mask_lat), drop=True)

        if masked_lon_lat_ds.sizes['scanline'] == 0 or masked_lon_lat_ds.sizes['ground_pixel'] == 0:
            raise ValueError("No data points within Taiwan region")

        mask_qa = (masked_lon_lat_ds.qa_value >= self.mask_value)
        masked_ds = masked_lon_lat_ds.where(mask_qa)

        if np.all(np.isnan(masked_ds.nitrogendioxide_tropospheric_column)):
            raise ValueError("No valid data points after QA filtering")

        return (
            masked_ds.longitude[0].data,
            masked_ds.latitude[0].data,
            masked_ds.nitrogendioxide_tropospheric_column[0].data
        )

    def process_each_data(self, start_date: str, end_date: str, use_taiwan_mask: bool = False):
        """處理單一數據"""
        # 將字串日期轉換為 datetime 物件
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')

        # 逐月處理每個月份的路徑
        current_date = start
        while current_date <= end:
            year = current_date.strftime('%Y')
            month = current_date.strftime('%m')

            # 構建每個月份的 input 和 output 路徑
            input_dir = RAW_DATA_DIR / year / month
            output_dir = PROCESSED_DATA_DIR / year / month
            figure_output_file = FIGURE_DIR / year / month

            # 創建路徑
            input_dir.mkdir(parents=True, exist_ok=True)
            output_dir.mkdir(parents=True, exist_ok=True)
            figure_output_file.mkdir(parents=True, exist_ok=True)

            # for monthly data
            # output_file = output_dir / f"NO2_{year}{month}.nc"
            #
            # if output_file.exists():
            #     logger.info(f"File {output_file} exists, skipping")

            # container = []

            # 目前只拿"NTRI"畫圖
            for file_path in input_dir.glob("*NRTI_L2__NO2*.nc"):
                logger.info(f'Processing: {file_path.name}')

                try:
                    dataset = self.process_zipped_nc(file_path)

                    if dataset is not None:
                        try:
                            # 提取數據
                            lon, lat, no2 = self.extract_data(dataset, use_taiwan_mask)

                            # 為每個檔案創建新的網格
                            lon_grid, lat_grid = self.create_grid(lon, lat)

                            # 插值
                            no2_grid = DataInterpolator.interpolate(
                                lon, lat, no2,
                                lon_grid, lat_grid,
                                method=self.interpolation_method
                            )

                            # 創建臨時的 Dataset 來顯示插值後的結果
                            interpolated_ds = xr.Dataset(
                                {
                                    'nitrogendioxide_tropospheric_column': (
                                        ['time', 'latitude', 'longitude'],
                                        no2_grid[np.newaxis, :, :]
                                    )
                                },
                                coords={
                                    'time': dataset.time.values[0:1],  # 使用原始時間
                                    'latitude': np.squeeze(lat_grid[:, 0]),
                                    'longitude': np.squeeze(lon_grid[0, :])
                                }
                            )

                            # 繪製插值後的數據圖
                            logger.info("繪製插值後的數據圖...")
                            plot_global_no2(interpolated_ds, figure_output_file / file_path.stem, close_after=True, map_scale='Taiwan')

                            # 移動到下個月
                            if month == "12":
                                current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
                            else:
                                current_date = current_date.replace(month=current_date.month + 1, day=1)

                            # container.append(no2_grid)
                        finally:
                            dataset.close()

                except Exception as e:
                    logger.error(f"Error processing {file_path.name}: {e}")
                    continue

    def _save_monthly_average(self, container, grid, year, month, output_file):
        """保存月平均數據"""
        # 計算平均值
        no2_stack = np.stack(container)
        no2_average = np.nanmean(no2_stack, axis=0)

        # 創建數據集
        # 確保年月格式正確
        year = str(year).zfill(4)
        month = str(month).zfill(2)
        # 創建完整的時間戳格式（包含日期和時間）
        time_str = f"{year}-{month}-01T00:00:00.000000000"

        # 創建數據集
        ds_result = xr.Dataset(
            {
                'nitrogendioxide_tropospheric_column': (
                    ['time', 'latitude', 'longitude'],
                    no2_average[np.newaxis, :, :]
                )
            },
            coords={
                'time': [np.datetime64(time_str, 'ns')],  # 使用奈秒精度
                'latitude': np.squeeze(grid[1][:, 0]),
                'longitude': np.squeeze(grid[0][0, :])
            }
        )

        # 添加時間屬性
        ds_result.time.attrs['long_name'] = 'time'
        ds_result.time.attrs['standard_name'] = 'time'

        # 確保輸出目錄存在
        output_file.parent.mkdir(parents=True, exist_ok=True)
        ds_result.to_netcdf(output_file)

        logger.info(f"Saved monthly average to {output_file}")
        logger.info(f"Final grid shape: {no2_average.shape}")
        logger.info(f"Time: {ds_result.time.values}")
        logger.info(f"Longitude range: {grid[0][0].min():.2f} to {grid[0][0].max():.2f}")
        logger.info(f"Latitude range: {grid[1][:, 0].min():.2f} to {grid[1][:, 0].max():.2f}")