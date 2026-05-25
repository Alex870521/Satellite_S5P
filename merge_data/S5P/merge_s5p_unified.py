#!/usr/bin/env python3
"""
Sentinel-5P 統一數據合併工具

支援 NO2, O3, SO2 等多種氣體的合併處理
"""

import logging
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Tuple

from src.utils.extract_datetime_from_filename import extract_datetime_from_filename
from src.config.catalog import get_resolution_for_product
from src.config.settings import BASE_DIR


class S5PProcessor:
    """Sentinel-5P 統一數據處理器"""
    
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
    
    # 支援的氣體類型配置（基於 Sentinel-5P 產品類型）
    GAS_CONFIGS = {
        'NO2': {
            'pattern': '**/NO2___/',
            'variable_name': 'nitrogendioxide_tropospheric_column',
            'output_var': 'no2',
            'long_name': 'Nitrogen Dioxide Tropospheric Column',
            'units': 'mol m-2'
        },
        'O3': {
            'pattern': '**/O3____/',
            'variable_name': 'ozone_total_vertical_column',
            'output_var': 'o3',
            'long_name': 'Ozone Total Vertical Column',
            'units': 'mol m-2'
        },
        'O3_TCL': {
            'pattern': '**/O3_TCL/',
            'variable_name': 'ozone_tropospheric_column',
            'output_var': 'o3_tcl',
            'long_name': 'Ozone Tropospheric Column',
            'units': 'mol m-2'
        },
        'O3_PROFILE': {
            'pattern': '**/O3__PR/',
            'variable_name': 'ozone_tropospheric_column',
            'output_var': 'o3_profile',
            'long_name': 'Ozone Profile',
            'units': 'mol m-2'
        },
        'SO2': {
            'pattern': '**/SO2___/',
            'variable_name': 'sulfurdioxide_total_vertical_column',
            'output_var': 'so2',
            'long_name': 'Sulfur Dioxide Total Vertical Column',
            'units': 'mol m-2'
        },
        'CO': {
            'pattern': '**/CO____/',
            'variable_name': 'carbonmonoxide_total_column',
            'output_var': 'co',
            'long_name': 'Carbon Monoxide Total Column',
            'units': 'mol m-2'
        },
        'CH4': {
            'pattern': '**/CH4___/',
            'variable_name': 'methane_mixing_ratio',
            'output_var': 'ch4',
            'long_name': 'Methane Mixing Ratio',
            'units': 'mol mol-1'
        },
        'HCHO': {
            'pattern': '**/HCHO__/',
            'variable_name': 'formaldehyde_tropospheric_vertical_column',
            'output_var': 'hcho',
            'long_name': 'Formaldehyde Tropospheric Vertical Column',
            'units': 'mol m-2'
        },
        'CLOUD': {
            'pattern': '**/CLOUD_/',
            'variable_name': 'cloud_fraction',
            'output_var': 'cloud',
            'long_name': 'Cloud Fraction',
            'units': '1'
        },
        'FRESCO': {
            'pattern': '**/FRESCO/',
            'variable_name': 'cloud_fraction',
            'output_var': 'fresco',
            'long_name': 'FRESCO Cloud Fraction',
            'units': '1'
        },
        'AER_AI': {
            'pattern': '**/AER_AI/',
            'variable_name': 'aerosol_index_340_380',
            'output_var': 'aer_ai',
            'long_name': 'Aerosol Index',
            'units': '1'
        },
        'AER_LH': {
            'pattern': '**/AER_LH/',
            'variable_name': 'aerosol_layer_height',
            'output_var': 'aer_lh',
            'long_name': 'Aerosol Layer Height',
            'units': 'm'
        }
    }
    
    # 處理過的檔案配置（與 GAS_CONFIGS 保持一致）
    PROCESSED_CONFIGS = GAS_CONFIGS.copy()
    
    def __init__(self, gas_type: str = 'NO2', use_processed_files: bool = False):
        """
        初始化處理器
        
        Parameters:
        -----------
        gas_type : str
            氣體類型，支援 'NO2', 'O3', 'SO2'
        use_processed_files : bool
            是否使用已處理的檔案 (True) 或原始檔案 (False)
        """
        self.gas_type = gas_type.upper()
        self.use_processed_files = use_processed_files
        
        if self.use_processed_files:
            if self.gas_type not in self.PROCESSED_CONFIGS:
                raise ValueError(f"不支援的氣體類型: {gas_type}. 支援的類型: {list(self.PROCESSED_CONFIGS.keys())}")
            self.config = self.PROCESSED_CONFIGS[self.gas_type]
        else:
            if self.gas_type not in self.GAS_CONFIGS:
                raise ValueError(f"不支援的氣體類型: {gas_type}. 支援的類型: {list(self.GAS_CONFIGS.keys())}")
            self.config = self.GAS_CONFIGS[self.gas_type]
        
        self.raw_dir = None
        self.output_dir = None
        self.logger = None
        
        # 根據氣體類型自動選擇解析度（優先使用 catalog 中的配置）
        # 首先嘗試從 catalog 獲取解析度
        catalog_resolution = get_resolution_for_product(self.gas_type)
        if catalog_resolution != (5.5, 3.5):  # 如果 catalog 中有特定配置
            self.resolution = catalog_resolution
        else:
            # 回退到舊的配置方式
            self.resolution = self.GAS_RESOLUTION_CONFIG.get(self.gas_type, (5.5, 3.5))
        
        # 台灣統一網格（與 sentinel_processor.py 保持一致）
        # 使用自動選擇的解析度
        # 計算解析度（以台灣中心緯度 23.5° 為準）
        center_lat = 23.5
        lat_km_per_degree = 111.32
        lon_km_per_degree = 111.32 * np.cos(np.radians(center_lat))
        
        # 根據氣體類型選擇的解析度轉換為度數
        lat_res = self.resolution[1] / lat_km_per_degree  # y_km 轉換為緯度度數
        lon_res = self.resolution[0] / lon_km_per_degree  # x_km 轉換為經度度數
        
        # 創建完整網格（與 GridFrame 保持一致）
        full_lat = np.arange(20.0, 27.0 + lat_res, lat_res)
        full_lon = np.arange(118.0, 124.0 + lon_res, lon_res)
        
        # 應用 FIGURE_BOUNDARY 過濾（與 sentinel_processor.py 一致）
        # FIGURE_BOUNDARY = (119, 123, 21, 26) 格式：(min_lon, max_lon, min_lat, max_lat)
        min_lon, max_lon, min_lat, max_lat = 119, 123, 21, 26
        mask_lon = (full_lon >= min_lon) & (full_lon <= max_lon)
        mask_lat = (full_lat >= min_lat) & (full_lat <= max_lat)
        
        self.taiwan_lon = full_lon[mask_lon]
        self.taiwan_lat = full_lat[mask_lat]
        self.qa_threshold = 0.75
        
    def get_resolution_info(self):
        """獲取當前解析度信息"""
        return {
            'gas_type': self.gas_type,
            'resolution_km': self.resolution,
            'grid_size': (len(self.taiwan_lat), len(self.taiwan_lon)),
            'lat_range': (self.taiwan_lat.min(), self.taiwan_lat.max()),
            'lon_range': (self.taiwan_lon.min(), self.taiwan_lon.max())
        }
        
    def setup_logging(self):
        """設置日誌"""
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
    def find_s5p_files(self, start_date: str = None, end_date: str = None, year: str = None) -> List[Path]:
        """尋找 S5P 文件"""
        if year is None:
            year = "2024"  # 預設年份
            
        # 使用配置的模式
        pattern = f"{self.config['pattern']}{year}/**/*.nc"
        all_files = [f for f in self.raw_dir.glob(pattern) if not f.name.startswith("._") and f.is_file()]
        
        filtered_files = []
        for file_path in all_files:
            try:
                file_date = extract_datetime_from_filename(file_path.name, to_local=False)
                if start_date and file_date < datetime.strptime(start_date, '%Y-%m-%d'):
                    continue
                if end_date and file_date > datetime.strptime(end_date, '%Y-%m-%d'):
                    continue
                filtered_files.append(file_path)
            except:
                continue
                
        filtered_files.sort(key=lambda f: extract_datetime_from_filename(f.name, to_local=False))
        self.logger.info(f"找到 {len(filtered_files)} 個有效的 S5P {self.gas_type} 文件")
        return filtered_files
        
    def read_s5p_file(self, file_path: Path) -> Optional[Tuple[np.ndarray, np.ndarray, np.ndarray, datetime]]:
        """讀取單個 S5P 文件"""
        try:
            if self.use_processed_files:
                return self._read_processed_file(file_path)
            else:
                return self._read_raw_file(file_path)
                
        except Exception as e:
            self.logger.error(f"讀取文件 {file_path.name} 時發生錯誤: {e}")
            return None
    
    def _read_raw_file(self, file_path: Path) -> Optional[Tuple[np.ndarray, np.ndarray, np.ndarray, datetime]]:
        """讀取原始 S5P 文件"""
        ds = xr.open_dataset(file_path, engine='netcdf4', group='PRODUCT')
        
        time_value = pd.to_datetime(ds.time.values[0])
        gas_data = ds[self.config['variable_name']].values[0]
        lat_data = ds.latitude.values[0]
        lon_data = ds.longitude.values[0]
        qa_data = ds['qa_value'].values[0]
        
        # QA 過濾
        valid_mask = qa_data >= self.qa_threshold
        gas_filtered = np.where(valid_mask, gas_data, np.nan)
        
        ds.close()
        return gas_filtered, lat_data, lon_data, time_value
    
    def _read_processed_file(self, file_path: Path) -> Optional[Tuple[np.ndarray, np.ndarray, np.ndarray, datetime]]:
        """讀取已處理的 S5P 文件"""
        ds = xr.open_dataset(file_path)
        
        time_value = pd.to_datetime(ds.time.values[0])
        gas_data = ds[self.config['variable_name']].values[0]
        
        # 已處理檔案應該已經在統一網格上，直接使用
        # 不需要重投影，直接返回統一網格坐標
        lat_data = self.taiwan_lat
        lon_data = self.taiwan_lon
        
        ds.close()
        return gas_data, lat_data, lon_data, time_value
            
    def reproject_to_taiwan_grid(self, gas_data: np.ndarray, lat_data: np.ndarray, lon_data: np.ndarray) -> np.ndarray:
        """重投影到台灣網格（使用格數控制的重採樣）"""
        if self.use_processed_files:
            # 處理過的檔案已經在統一網格上，直接返回
            return gas_data
        
        taiwan_grid = np.full((len(self.taiwan_lat), len(self.taiwan_lon)), np.nan)
        
        valid_mask = ~np.isnan(gas_data)
        if not np.any(valid_mask):
            return taiwan_grid
            
        valid_lats = lat_data[valid_mask]
        valid_lons = lon_data[valid_mask]
        valid_gas = gas_data[valid_mask]
        
        # 計算目標網格解析度
        if len(self.taiwan_lat) > 1:
            target_lat_resolution = abs(self.taiwan_lat[1] - self.taiwan_lat[0])
        else:
            target_lat_resolution = 0.01
            
        if len(self.taiwan_lon) > 1:
            target_lon_resolution = abs(self.taiwan_lon[1] - self.taiwan_lon[0])
        else:
            target_lon_resolution = 0.01
        
        # 估算原始數據的像元大小
        if len(valid_lats) > 1:
            from scipy.spatial import cKDTree
            temp_tree = cKDTree(np.column_stack((valid_lats, valid_lons)))
            distances, _ = temp_tree.query(np.column_stack((valid_lats, valid_lons)), k=2)
            pixel_size = np.median(distances[:, 1])  # 第二近鄰距離
        else:
            pixel_size = max(target_lat_resolution, target_lon_resolution)
        
        # 判斷是否為低解析度到高解析度的重採樣
        is_upsampling = (pixel_size > target_lat_resolution * 1.5) or (pixel_size > target_lon_resolution * 1.5)
        
        if is_upsampling:
            # 低解析度到高解析度：使用格數控制的重採樣（索引切片，避免距離矩陣）
            cell_deg = (target_lat_resolution + target_lon_resolution) / 2.0
            cells_to_fill = max(2, int(np.ceil(pixel_size / cell_deg)))  # 至少覆蓋2格
            half_cells = max(1, cells_to_fill // 2)

            n_lat = len(self.taiwan_lat)
            n_lon = len(self.taiwan_lon)

            for obs_lat, obs_lon, obs_value in zip(valid_lats, valid_lons, valid_gas):
                # 觀測點中心對應的索引
                center_lat_idx = np.digitize(obs_lat, self.taiwan_lat) - 1
                center_lon_idx = np.digitize(obs_lon, self.taiwan_lon) - 1

                if center_lat_idx < 0 or center_lat_idx >= n_lat or center_lon_idx < 0 or center_lon_idx >= n_lon:
                    continue

                # 計算填充的索引範圍（方形覆蓋）
                lat_start = max(0, center_lat_idx - half_cells)
                lat_end = min(n_lat, center_lat_idx + half_cells + 1)
                lon_start = max(0, center_lon_idx - half_cells)
                lon_end = min(n_lon, center_lon_idx + half_cells + 1)

                # 實際填充（像元複寫）；如有重疊，直接覆寫即可
                taiwan_grid[lat_start:lat_end, lon_start:lon_end] = obs_value
        else:
            # 一般情況：使用傳統的網格填充
            # S5P 解析度參數（約 7km × 3.5km）
            s5p_lat_resolution = 4.0 / 111.0
            s5p_lon_resolution = 8.0 / (111.0 * np.cos(np.radians(23.5)))
            
            # 使用 np.digitize 進行網格填充
            lat_indices = np.digitize(valid_lats, self.taiwan_lat) - 1
            lon_indices = np.digitize(valid_lons, self.taiwan_lon) - 1
            
            valid_lat_idx = (lat_indices >= 0) & (lat_indices < len(self.taiwan_lat))
            valid_lon_idx = (lon_indices >= 0) & (lon_indices < len(self.taiwan_lon))
            valid_indices = valid_lat_idx & valid_lon_idx
            
            if np.any(valid_indices):
                # 填充網格
                for i in np.where(valid_indices)[0]:
                    lat_idx = lat_indices[i]
                    lon_idx = lon_indices[i]
                    gas_val = valid_gas[i]
                    s5p_lat = valid_lats[i]
                    s5p_lon = valid_lons[i]
                    
                    # 計算 S5P 像素覆蓋的範圍
                    lat_half_range = s5p_lat_resolution / 2
                    lon_half_range = s5p_lon_resolution / 2
                    
                    lat_min = s5p_lat - lat_half_range
                    lat_max = s5p_lat + lat_half_range
                    lon_min = s5p_lon - lon_half_range
                    lon_max = s5p_lon + lon_half_range
                    
                    # 找到對應的網格索引範圍
                    lat_start = max(0, np.digitize(lat_min, self.taiwan_lat) - 1)
                    lat_end = min(len(self.taiwan_lat), np.digitize(lat_max, self.taiwan_lat))
                    lon_start = max(0, np.digitize(lon_min, self.taiwan_lon) - 1)
                    lon_end = min(len(self.taiwan_lon), np.digitize(lon_max, self.taiwan_lon))
                    
                    # 填充網格
                    for grid_lat_idx in range(lat_start, lat_end):
                        for grid_lon_idx in range(lon_start, lon_end):
                            if self._is_within_taiwan_bounds(grid_lat_idx, grid_lon_idx):
                                if np.isnan(taiwan_grid[grid_lat_idx, grid_lon_idx]):
                                    taiwan_grid[grid_lat_idx, grid_lon_idx] = gas_val
                                else:
                                    # 如果有多個像素覆蓋同一個網格，取平均
                                    taiwan_grid[grid_lat_idx, grid_lon_idx] = (taiwan_grid[grid_lat_idx, grid_lon_idx] + gas_val) / 2
        
        return taiwan_grid
    
    def _is_within_taiwan_bounds(self, lat_idx: int, lon_idx: int) -> bool:
        """檢查網格點是否在台灣實際範圍內"""
        if lat_idx < 0 or lat_idx >= len(self.taiwan_lat) or lon_idx < 0 or lon_idx >= len(self.taiwan_lon):
            return False
        
        lat = self.taiwan_lat[lat_idx]
        lon = self.taiwan_lon[lon_idx]
        
        return (21.0 <= lat <= 26.0 and 119.0 <= lon <= 123.0)
    
    def _reproject_to_uniform_grid(self, gas_data: np.ndarray, lat_data: np.ndarray, lon_data: np.ndarray) -> np.ndarray:
        """重投影到統一網格"""
        from scipy.interpolate import griddata
        
        # 創建目標網格
        target_lat, target_lon = np.meshgrid(self.taiwan_lat, self.taiwan_lon, indexing='ij')
        
        # 創建源網格
        if lat_data.ndim == 1 and lon_data.ndim == 1:
            source_lat, source_lon = np.meshgrid(lat_data, lon_data, indexing='ij')
        else:
            source_lat, source_lon = lat_data, lon_data
        
        # 展平網格點
        source_points = np.column_stack([source_lat.ravel(), source_lon.ravel()])
        target_points = np.column_stack([target_lat.ravel(), target_lon.ravel()])
        
        # 只處理有效數據點
        valid_mask = ~np.isnan(gas_data)
        if not np.any(valid_mask):
            return np.full((len(self.taiwan_lat), len(self.taiwan_lon)), np.nan)
        
        valid_source_points = source_points[valid_mask.ravel()]
        valid_gas_values = gas_data[valid_mask]
        
        # 使用最近鄰插值
        try:
            interpolated_values = griddata(
                valid_source_points, 
                valid_gas_values, 
                target_points, 
                method='nearest',
                fill_value=np.nan
            )
            return interpolated_values.reshape((len(self.taiwan_lat), len(self.taiwan_lon)))
        except Exception as e:
            self.logger.error(f"重投影失敗: {e}")
            return np.full((len(self.taiwan_lat), len(self.taiwan_lon)), np.nan)
        
    def group_files_by_date(self, files: List[Path]) -> Dict[str, List[Path]]:
        """按日期分組文件"""
        daily_files = {}
        for file_path in files:
            file_date = extract_datetime_from_filename(file_path.name, to_local=False)
            if file_date:
                date_key = file_date.strftime('%Y-%m-%d')
                if date_key not in daily_files:
                    daily_files[date_key] = []
                daily_files[date_key].append(file_path)
                
        self.logger.info(f"按日期分組後有 {len(daily_files)} 個不同的日期")
        return daily_files
        
    def merge_daily_files(self, daily_files: List[Path]) -> Optional[Tuple[np.ndarray, datetime]]:
        """合併同一天的多個文件"""
        all_daily_data = []
        daily_time = None
        
        for file_path in daily_files:
            result = self.read_s5p_file(file_path)
            if result is not None:
                gas_data, lat_data, lon_data, time_value = result
                reprojected_data = self.reproject_to_taiwan_grid(gas_data, lat_data, lon_data)
                
                # 確保所有數據都有相同的形狀
                expected_shape = (len(self.taiwan_lat), len(self.taiwan_lon))
                if reprojected_data.shape != expected_shape:
                    self.logger.warning(f"文件 {file_path.name} 的數據形狀不一致: {reprojected_data.shape} vs {expected_shape}")
                    self.logger.warning(f"這表示該文件可能使用了不同的網格設定，將跳過此文件")
                    continue
                    
                all_daily_data.append(reprojected_data)
                
                if daily_time is None:
                    daily_time = time_value
                    
        if not all_daily_data:
            return None
            
        # 合併同一天的數據
        if len(all_daily_data) == 1:
            merged_data = all_daily_data[0]
        else:
            # 確保所有數組都有相同的形狀再進行合併
            try:
                # 先檢查所有數組的形狀是否一致
                shapes = [data.shape for data in all_daily_data]
                if len(set(shapes)) > 1:
                    self.logger.error(f"數據形狀不一致: {shapes}")
                    return None
                
                merged_data = np.nanmean(all_daily_data, axis=0)
            except Exception as e:
                self.logger.error(f"合併數據時發生錯誤: {e}")
                return None
            
        return merged_data, daily_time
        
    def merge_s5p_files_to_netcdf(self, start_date: str = None, end_date: str = None, 
                                 output_filename: str = None, year: str = None) -> bool:
        """合併 S5P 文件到 NetCDF"""
        try:
            files = self.find_s5p_files(start_date, end_date, year)
            if not files:
                self.logger.error("沒有找到符合條件的文件")
                return False
                
            daily_files = self.group_files_by_date(files)
            
            all_daily_data = []
            all_daily_times = []
            
            for date_key, day_files in sorted(daily_files.items()):
                self.logger.info(f"處理 {date_key} 的 {len(day_files)} 個文件")
                
                result = self.merge_daily_files(day_files)
                if result is not None:
                    daily_data, daily_time = result
                    all_daily_data.append(daily_data)
                    all_daily_times.append(daily_time)
                    
                    valid_count = np.sum(~np.isnan(daily_data))
                    total_count = daily_data.size
                    self.logger.info(f"  有效數據點: {valid_count}/{total_count} ({valid_count/total_count*100:.1f}%)")
                    
            if not all_daily_data:
                self.logger.error("沒有成功處理任何數據")
                return False
                
            # 創建 3D 數據集
            self.logger.info(f"創建 3D 數據集，形狀: ({len(all_daily_data)}, {len(self.taiwan_lat)}, {len(self.taiwan_lon)})")
            
            gas_3d = np.stack(all_daily_data, axis=0)
            time_coords = pd.to_datetime(all_daily_times)
            
            # 創建 xarray Dataset
            ds = xr.Dataset(
                data_vars={
                    self.config['output_var']: (['time', 'lat', 'lon'], gas_3d, {
                        'long_name': self.config['long_name'],
                        'units': self.config['units']
                    })
                },
                coords={
                    'time': time_coords,
                    'lat': (['lat'], self.taiwan_lat),
                    'lon': (['lon'], self.taiwan_lon)
                },
                attrs={
                    'title': f'Sentinel-5P {self.gas_type} Merged Dataset (Taiwan Grid)',
                    'creation_date': datetime.now().isoformat()
                }
            )
            
            # 保存文件
            if output_filename is None:
                start_str = time_coords.min().strftime('%Y%m%d')
                end_str = time_coords.max().strftime('%Y%m%d')
                output_filename = f"S5P_{self.gas_type}_{start_str}_{end_str}.nc"
                
            output_path = self.output_dir / output_filename
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            ds.to_netcdf(output_path)
            self.logger.info(f"成功保存合併的 NetCDF 文件: {output_path}")
            
            ds.close()
            return True
            
        except Exception as e:
            self.logger.error(f"合併文件時發生錯誤: {e}")
            return False


def main():
    """主函數 - 互動式使用"""
    print("🛰️ Sentinel-5P 統一合併工具")
    print("=" * 50)
    
    # 選擇檔案類型
    print("選擇要處理的檔案類型:")
    print("  1. 原始檔案 (需要重投影和插值)")
    print("  2. 已處理檔案 (已經插值到統一網格)")
    
    try:
        file_choice = int(input("請選擇 (1-2): "))
        use_processed_files = (file_choice == 2)
    except (ValueError, IndexError):
        use_processed_files = False
        print("使用預設選項: 原始檔案")
    
    file_type_str = "已處理檔案" if use_processed_files else "原始檔案"
    print(f"已選擇: {file_type_str}")
    
    # 選擇氣體類型
    print("\n選擇要處理的氣體類型:")
    gas_types = list(S5PProcessor.GAS_CONFIGS.keys())
    
    # 按類別分組顯示
    ozone_products = [g for g in gas_types if g.startswith('O3')]
    gas_products = [g for g in gas_types if g in ['NO2', 'SO2', 'CO', 'CH4', 'HCHO']]
    cloud_aerosol_products = [g for g in gas_types if g in ['CLOUD', 'FRESCO', 'AER_AI', 'AER_LH']]
    
    # 創建統一的選項列表
    all_options = ozone_products + gas_products + cloud_aerosol_products
    
    print("  臭氧產品:")
    for i, gas_type in enumerate(ozone_products, 1):
        print(f"    {i}. {gas_type}")
    
    print("  氣體產品:")
    for i, gas_type in enumerate(gas_products, len(ozone_products) + 1):
        print(f"    {i}. {gas_type}")
    
    print("  雲和氣溶膠產品:")
    for i, gas_type in enumerate(cloud_aerosol_products, len(ozone_products) + len(gas_products) + 1):
        print(f"    {i}. {gas_type}")
    
    try:
        choice = int(input(f"請選擇 (1-{len(all_options)}): ")) - 1
        selected_gas = all_options[choice]
    except (ValueError, IndexError):
        selected_gas = 'NO2'
        print("使用預設選項: NO2")
    
    print(f"已選擇: {selected_gas}")
    
    # 設置參數
    year = input("請輸入年份 (預設: 2024): ").strip() or "2024"
    start_date = input("請輸入開始日期 (YYYY-MM-DD, 預設: 不限制): ").strip() or None
    end_date = input("請輸入結束日期 (YYYY-MM-DD, 預設: 不限制): ").strip() or None
    
    # 初始化處理器
    processor = S5PProcessor(gas_type=selected_gas, use_processed_files=use_processed_files)
    
    # 顯示解析度信息
    resolution_info = processor.get_resolution_info()
    print(f"\n📊 解析度設定:")
    print(f"  氣體類型: {resolution_info['gas_type']}")
    print(f"  網格解析度: {resolution_info['resolution_km'][0]}km × {resolution_info['resolution_km'][1]}km")
    print(f"  網格大小: {resolution_info['grid_size'][0]} × {resolution_info['grid_size'][1]}")
    print(f"  緯度範圍: {resolution_info['lat_range'][0]:.3f}° 到 {resolution_info['lat_range'][1]:.3f}°")
    print(f"  經度範圍: {resolution_info['lon_range'][0]:.3f}° 到 {resolution_info['lon_range'][1]:.3f}°")
    
    if use_processed_files:
        # 已處理檔案：從 processed 目錄讀取
        processor.raw_dir = BASE_DIR / "Sentinel-5P" / "processed"
    else:
        # 原始檔案：從 raw 目錄讀取
        processor.raw_dir = BASE_DIR / "Sentinel-5P" / "raw"

    # 輸出目錄：桌面
    processor.output_dir = Path.home() / "Desktop"

    processor.setup_logging()
    
    print(f"\n開始處理 {selected_gas} 數據 ({file_type_str})...")
    
    # 合併數據
    success = processor.merge_s5p_files_to_netcdf(
        start_date=start_date,
        end_date=end_date,
        year=year
    )
    
    if success:
        print(f"\n✅ {selected_gas} 合併成功！")
    else:
        print(f"\n❌ {selected_gas} 合併失敗！")


def batch_process():
    """批次處理多種氣體"""
    print("🛰️ Sentinel-5P 批次合併工具")
    print("=" * 50)
    
    # 選擇檔案類型
    print("選擇要處理的檔案類型:")
    print("  1. 原始檔案 (需要重投影和插值)")
    print("  2. 已處理檔案 (已經插值到統一網格)")
    
    try:
        file_choice = int(input("請選擇 (1-2): "))
        use_processed_files = (file_choice == 2)
    except (ValueError, IndexError):
        use_processed_files = False
        print("使用預設選項: 原始檔案")
    
    file_type_str = "已處理檔案" if use_processed_files else "原始檔案"
    print(f"已選擇: {file_type_str}")
    
    # 選擇要批次處理的氣體類型
    print("\n選擇要批次處理的氣體類型:")
    print("  1. 臭氧產品 (O3, O3_TCL, O3_PROFILE)")
    print("  2. 氣體產品 (NO2, SO2, CO, CH4, HCHO)")
    print("  3. 雲和氣溶膠產品 (CLOUD, FRESCO, AER_AI, AER_LH)")
    print("  4. 所有產品")
    print("  5. 自定義選擇")
    
    try:
        batch_choice = int(input("請選擇 (1-5): "))
    except (ValueError, IndexError):
        batch_choice = 2
        print("使用預設選項: 氣體產品")
    
    if batch_choice == 1:
        gas_types = ['O3', 'O3_TCL', 'O3_PROFILE']
    elif batch_choice == 2:
        gas_types = ['NO2', 'SO2', 'CO', 'CH4', 'HCHO']
    elif batch_choice == 3:
        gas_types = ['CLOUD', 'FRESCO', 'AER_AI', 'AER_LH']
    elif batch_choice == 4:
        gas_types = list(S5PProcessor.GAS_CONFIGS.keys())
    else:  # 自定義選擇
        print("可用的氣體類型:")
        all_gas_types = list(S5PProcessor.GAS_CONFIGS.keys())
        for i, gas_type in enumerate(all_gas_types, 1):
            print(f"  {i}. {gas_type}")
        
        try:
            selected_indices = input("請輸入要處理的編號 (用逗號分隔，例如: 1,2,3): ").strip()
            indices = [int(x.strip()) - 1 for x in selected_indices.split(',')]
            gas_types = [all_gas_types[i] for i in indices if 0 <= i < len(all_gas_types)]
        except (ValueError, IndexError):
            gas_types = ['NO2', 'O3']
            print("使用預設選項: NO2, O3")
    
    year = input("請輸入年份 (預設: 2024): ").strip() or "2024"
    
    for gas_type in gas_types:
        print(f"\n處理 {gas_type} ({file_type_str})...")
        
        processor = S5PProcessor(gas_type=gas_type, use_processed_files=use_processed_files)
        
        if use_processed_files:
            # 已處理檔案：從 processed 目錄讀取
            processor.raw_dir = BASE_DIR / "Sentinel-5P" / "processed"
        else:
            # 原始檔案：從 raw 目錄讀取
            processor.raw_dir = BASE_DIR / "Sentinel-5P" / "raw"

        # 輸出目錄：桌面
        processor.output_dir = Path.home() / "Desktop"

        processor.setup_logging()
        
        success = processor.merge_s5p_files_to_netcdf(year=year)
        
        if success:
            print(f"✅ {gas_type} 合併成功！")
        else:
            print(f"❌ {gas_type} 合併失敗！")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--batch":
        batch_process()
    else:
        main()
