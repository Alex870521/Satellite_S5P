"""src/processing/no2_processor.py"""
import logging
import numpy as np
import xarray as xr
from datetime import datetime
from dateutil.relativedelta import relativedelta

from src.processing.interpolators import DataInterpolator
from src.processing.taiwan_frame import TaiwanFrame
from src.config.settings import RAW_DATA_DIR, PROCESSED_DATA_DIR, FIGURE_DIR, FIGURE_BOUNDARY
from src.visualization.plot_nc import plot_global_var
from src.config.catalog import ClassInput, TypeInput, PRODUCT_CONFIGS
from src.config.richer import DisplayManager


logger = logging.getLogger(__name__)


class S5Processor:
    def __init__(self, interpolation_method='kdtree', resolution=0.01, mask_qc_value=0.75):
        """初始化處理器

        Parameters:
        -----------
        interpolation_method : str
            插值方法，可選 'griddata' 或 'kdtree'
        resolution : float
            網格解析度（度）
        mask_qc_value : float
            QA 值的閾值
        """
        self.interpolation_method = interpolation_method
        self.resolution = resolution
        self.mask_qc_value = mask_qc_value
        self.taiwan_frame = TaiwanFrame()

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
        dataset = dataset.where(mask_qa)

        # 檢查數據有效性
        if np.all(np.isnan(dataset[attributes])):
            raise ValueError("No valid data points after QA filtering")

        lon = dataset.longitude[0].values
        lat = dataset.latitude[0].values
        shape = dataset.latitude[0].values.shape
        var = dataset[attributes][0].values

        info_dict = {
            'time': f"{time}",
            'shape': f"{shape}",
            'latitude': f'{np.nanmin(lat):.2f} to {np.nanmax(lat):.2f}',
            'longitude': f'{np.nanmin(lon):.2f} to {np.nanmax(lon):.2f}',
        }

        if hasattr(self, 'nc_info'):
            self.nc_info.update(info_dict)

        return lon, lat, var

    def process_each_data(self,
                          file_class: ClassInput,
                          file_type: TypeInput,
                          start_date: str,
                          end_date: str,
                          ):
        """
        處理指定日期範圍內的衛星數據

        Args:
            file_class : ProductClassInput
            file_type: ProductTypeInput
            start_date (str): 開始日期 (YYYY-MM-DD)
            end_date (str): 結束日期 (YYYY-MM-DD)
        """
        def process_single_file(file_path, output_dir):
            """處理單一數據檔案"""
            ds = xr.open_dataset(file_path, engine='netcdf4', group='PRODUCT')

            # 確保輸出目錄存在
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / file_path.name

            if not output_path.exists():
                ds.to_netcdf(output_path)

            if ds is None:
                return

            try:
                # 1. 紀錄 nc 檔訊息
                self.nc_info = {'file_name': file_path.name}

                # # 2. 提取數據和信息
                # lon, lat, var = self.extract_data(ds, extract_range=FIGURE_BOUNDARY)
                #
                # # 3. 顯示檔案信息
                # DisplayManager().display_product_info(self.nc_info)
                #
                # # 4. 創建網格並進行插值
                # lon_grid, lat_grid = self.create_grid(lon, lat)
                # var_grid = DataInterpolator.interpolate(
                #     lon, lat, var,
                #     lon_grid, lat_grid,
                #     method=self.interpolation_method
                # )
                #
                # # 5. 創建插值後的數據集
                # interpolated_ds = xr.Dataset(
                #     {
                #         self.product_type.dataset_name: (
                #             ['time', 'latitude', 'longitude'],
                #             var_grid[np.newaxis, :, :]
                #         )
                #     },
                #     coords={
                #         'time': ds.time.values[0:1],
                #         'latitude': np.squeeze(lat_grid[:, 0]),
                #         'longitude': np.squeeze(lon_grid[0, :])
                #     }
                # )

            finally:
                ds.close()

        # 主處理流程
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        current_date = start

        # 按月份逐月處理
        while current_date <= end:
            # 1. 準備當月的目錄路徑
            product_type = file_type
            year = current_date.strftime('%Y')
            month = current_date.strftime('%m')

            input_dir = RAW_DATA_DIR / product_type / year / month
            output_dir = PROCESSED_DATA_DIR / product_type / year / month
            figure_dir = FIGURE_DIR / product_type / year / month

            # 2. 創建必要的目錄
            for directory in [output_dir, figure_dir]:
                directory.mkdir(parents=True, exist_ok=True)

            file_pattern = f"*{file_class}_L2__{file_type}*.nc"

            # 3. 處理原始數據（如果存在）
            if input_dir.exists():
                for file_path in input_dir.glob(file_pattern):
                    # 檢查檔案日期是否在指定範圍內
                    date_to_check = datetime.strptime(file_path.name[20:28], '%Y%m%d')
                    if not (start <= date_to_check <= end):
                        continue
                    try:
                        process_single_file(file_path, output_dir)
                    except Exception as e:
                        logger.error(f"處理檔案 {file_path.name} 時發生錯誤: {e}")
                        continue

            # 4. 繪製圖片（使用處理後的數據）
            processed_files = list(output_dir.glob(file_pattern))
            if not processed_files:
                logger.warning(f"在 {output_dir} 中找不到符合條件的處理後檔案")
                continue

            for file_path in processed_files:
                date_to_check = datetime.strptime(file_path.name[20:28], '%Y%m%d')
                if not (start <= date_to_check <= end):
                    continue
                figure_path = figure_dir / f"{file_path.stem}.png"
                try:
                    plot_global_var(
                        dataset=file_path,
                        product_params=PRODUCT_CONFIGS[file_type],
                        savefig_path=figure_path,
                        map_scale='Taiwan',
                        show_stations=True
                    )
                except Exception as e:
                    logger.error(f"繪製檔案 {file_path.name} 時發生錯誤: {e}")
                    continue

            # 4. 移至下個月
            current_date = (current_date + relativedelta(months=1)).replace(day=1)

    @staticmethod
    def _save_monthly_average(container, grid, year, month, output_file):
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
