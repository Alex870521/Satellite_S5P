import os
import re
import shutil
from typing import Literal, Optional

import cdsapi
import xarray as xr
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from src.api.core import SatelliteHub


class ERA5Hub(SatelliteHub):
    # API name
    name = "ERA5"

    # Predefined variable abbreviation mapping table
    VAR_MAPPING = {
        # Dataset: reanalysis-era5-pressure-levels
        'relative_humidity': 'rh',
        'temperature': 't',
        'u_component_of_wind': 'u',
        'v_component_of_wind': 'v',

        # Dataset: reanalysis-era5-single-levels
        '10m_u_component_of_wind': 'u10',
        '10m_v_component_of_wind': 'v10',
        '2m_temperature': 't2m',
        '2m_dewpoint_temperature': 'd2m',
        'boundary_layer_height': 'blh',
        'surface_pressure': 'sp',
        'total_column_water_vapour': 'tcwv',

        # TODO: aerosol properties, including aerosol_optical_depth, aerosol extinction coefficient
        # Dataset: satellite-aerosol-properties
    }

    def __init__(self, timezone=None):
        """
        Initialize ERA5Hub, set the timezone parameters

        Parameters:
            timezone (str or None): Timezone string (e.g.: 'Asia/Taipei').
                                   If None, system timezone will be used.
        """
        super().__init__()

        # Set timezone
        self._setup_timezone(timezone)

        # 時間格式設定
        self.date_format = "%Y%m%d"
        self.datetime_format = "%Y%m%d_%H%M"

    def authentication(self):
        """Implements authentication method, returns a configured CDSAPI client"""
        if not os.getenv('CDSAPI_URL') or not os.getenv('CDSAPI_KEY'):
            raise EnvironmentError(
                "Missing CDS API credentials. Please set CDSAPI_URL and CDSAPI_KEY environment variables"
            )
        return cdsapi.Client(quiet=True)

    def _create_time_info(self, start_date, end_date) -> dict:
        """創建統一的時間信息對象"""
        user_start, user_end = self._normalize_time_inputs(start_date, end_date)

        # 計算查詢用的 UTC 時間範圍（考慮時區偏移）
        query_start = user_start
        query_end = user_end

        if self.tz_offset > 0:  # UTC+ 時區
            query_start = query_start - timedelta(days=1)
        elif self.tz_offset < 0:  # UTC- 時區
            query_end = query_end + timedelta(days=1)

        return {
            'user_start': user_start,
            'user_end': user_end,
            'query_start': query_start,
            'query_end': query_end,
            'user_date_range': f"{user_start.strftime(self.date_format)}_{user_end.strftime(self.date_format)}",
            'query_date_range': f"{query_start.strftime(self.date_format)}_{query_end.strftime(self.date_format)}"
        }

    def _create_file_paths(self, time_info: dict, variables: list,
                           pressure_levels: Optional[list] = None,
                           mode: str = 'monthly') -> dict:
        """統一的文件路徑創建邏輯"""
        var_str = '_'.join([self.VAR_MAPPING.get(v, v[:3].lower()) for v in variables])

        # 基礎目錄
        if pressure_levels:
            base_dir = self.raw_dir / "pressure_level"
            level_str = '-'.join(map(str, pressure_levels))
            prefix = f"era5_pl_{var_str}_{level_str}"
            self.is_pressure_level_data = True
        else:
            base_dir = self.raw_dir / "single_level"
            prefix = f"era5_sfc_{var_str}"
            self.is_pressure_level_data = False

        # 文件命名策略
        if mode == 'monthly':
            # 月份模式：使用用戶時間範圍命名
            filename = f"{prefix}_{time_info['user_date_range']}.nc"
            # 按年份分目錄
            year_dir = base_dir / str(time_info['user_start'].year)
            file_path = year_dir / filename
        else:
            # 一次性模式：直接存在基礎目錄
            filename = f"{prefix}_{time_info['user_date_range']}.nc"
            year_dir = base_dir / str(time_info['user_start'].year)
            file_path = year_dir / filename

        return {
            'file_path': file_path,
            'filename': filename,
            'base_dir': base_dir,
            'csv_base_name': f"{var_str}_{time_info['user_date_range']}"
        }

    def _create_csv_paths(self, variable: str, level: Optional[str] = None,
                          suffix: str = "", custom_csv_dir: Optional[str | Path] = None) -> Path:
        """統一的 CSV 路徑創建"""
        if custom_csv_dir:
            # 使用自定義目錄
            csv_dir = Path(custom_csv_dir)
            csv_dir.mkdir(parents=True, exist_ok=True)
        else:
            # 使用默認目錄：processed/csv/年份/
            year = self.time_info['user_start'].year
            csv_dir = self.csv_dir / str(year)
            csv_dir.mkdir(parents=True, exist_ok=True)

        # CSV 文件命名
        base_name = self.file_info['csv_base_name']
        if level:
            csv_filename = f"{variable}_{level}{suffix}_{base_name}.csv"
        else:
            csv_filename = f"{variable}{suffix}_{base_name}.csv"

        return csv_dir / csv_filename

    def fetch_data(self, start_date: str | datetime, end_date: str | datetime,
                   boundary: tuple, variables: Optional[list] = None,
                   pressure_levels: Optional[list] = None,
                   download_mode: Literal['all_at_once', 'monthly'] = "monthly") -> bool:
        """
        Query ERA5 data without downloading

        Parameters:
            start_date (str or datetime): Start date/time in user's timezone
            end_date (str or datetime): End date/time in user's timezone
            boundary (list or tuple): Geographic boundary (min_lon, max_lon, min_lat, max_lat)
            variables (list): List of variables to fetch
            pressure_levels (list or None): List of pressure levels (unit: hPa)
            download_mode (str): Download mode, either "monthly" or "all_at_once"

        Returns:
            bool: Whether the query was successful
        """
        # 創建時間信息
        self.time_info = self._create_time_info(start_date, end_date)

        # 保存日期屬性（供 process_data 使用）
        self.start_date = self.time_info['user_start']
        self.end_date = self.time_info['user_end']

        # 保存參數
        min_lon, max_lon, min_lat, max_lat = boundary
        self.boundary = (max_lat, min_lon, min_lat, max_lon)  # ERA5 格式：北西南東
        self.variables = variables
        self.pressure_levels = pressure_levels
        self.download_mode = download_mode

        # 創建文件路徑信息
        self.file_info = self._create_file_paths(
            self.time_info, self.variables, pressure_levels, download_mode
        )

        # 確保目錄存在
        self.file_info['file_path'].parent.mkdir(parents=True, exist_ok=True)

        # 確保必要目錄存在
        self.single_level_dir = self.raw_dir / "single_level"
        self.csv_dir = self.processed_dir / "csv"

        dirs_to_create = [self.single_level_dir, self.csv_dir]

        if self.is_pressure_level_data:
            self.pressure_level_dir = self.raw_dir / "pressure_level"
            dirs_to_create.append(self.pressure_level_dir)

        # 創建所有必要目錄
        for dir_path in dirs_to_create:
            dir_path.mkdir(parents=True, exist_ok=True)

        # 獲取變量縮寫
        self.available_vars_for_ds = [self.VAR_MAPPING.get(var, var[:3].lower()) for var in self.variables]
        self.var_str = '_'.join(self.available_vars_for_ds)

        # 記錄信息
        self.logger.info("ERA5 data query")
        self.logger.info(
            f"User time range: {self.time_info['user_start'].strftime('%Y-%m-%d %H:%M:%S %z')} to {self.time_info['user_end'].strftime('%Y-%m-%d %H:%M:%S %z')}")
        self.logger.info(f"Variables: {', '.join(self.variables)}")
        self.logger.info(f"Download mode: {download_mode}")

        if pressure_levels:
            self.logger.info(f"Pressure levels: {', '.join(map(str, pressure_levels))}")

        # 準備查詢結果 (簡化為單一查詢)
        self.query_results = [self.file_info]

        self.logger.info("ERA5 data query completed")
        return True

    def download_data(self) -> list:
        """
        Download ERA5 data

        Returns:
            list: Paths of downloaded netCDF files
        """
        if not hasattr(self, 'query_results') or not self.query_results:
            self.logger.warning("Please execute the fetch_data method first to get query results")
            return []

        downloaded_files = []

        for item in self.query_results:
            file_path = Path(item['file_path'])

            # 檢查文件是否已存在
            if file_path.exists():
                self.logger.info(f"File already exists: {file_path}")
                downloaded_files.append(str(file_path))
                continue

            # 準備 API 請求參數
            request_params = {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': self.variables,
                'date': f"{self.time_info['query_start'].strftime('%Y-%m-%d')}/{self.time_info['query_end'].strftime('%Y-%m-%d')}",
                'time': [f"{h:02d}:00" for h in range(24)],
                'area': self.boundary,
            }

            if self.pressure_levels:
                request_params['pressure_level'] = self.pressure_levels
                dataset = "reanalysis-era5-pressure-levels"
            else:
                dataset = "reanalysis-era5-single-levels"

            # 創建臨時文件
            temp_file = file_path.with_suffix('.temp')

            try:
                self.logger.info(f"Downloading: {item['filename']}")

                # 下載文件
                self.client.retrieve(dataset, request_params, str(temp_file))

                # 重命名到正式文件名
                temp_file.rename(file_path)
                self.logger.info(f"\nDownload completed: {file_path}")
                downloaded_files.append(str(file_path))

            except Exception as e:
                self.logger.error(f"Download failed: {item['filename']}")
                self.logger.error(f"Error: {str(e)}")

                # 清理臨時文件
                if temp_file.exists():
                    try:
                        temp_file.unlink()
                        self.logger.info(f"Deleted temporary file: {temp_file}")
                    except Exception as cleanup_error:
                        self.logger.error(f"Failed to clean up temporary file: {str(cleanup_error)}")

        if downloaded_files:
            self.logger.info(f"Download completed, {len(downloaded_files)} files in total")
        else:
            self.logger.info("No new files downloaded")

        return downloaded_files

    def _prepare_csv_paths(self, netcdf_file):
        """
        Prepare CSV path and directory directly from netCDF filename

        Parameters:
            netcdf_file (str or Path): Path to the netCDF file

        Returns:
            str: CSV file path
        """
        # Extract year-month information from netCDF filename
        file_stem = Path(netcdf_file).stem

        # Find date part in filename (YYYYMMDD_YYYYMMDD)
        date_match = re.search(r'(\d{8}_\d{8})', file_stem)

        if not date_match:
            self.logger.warning(f"Cannot extract date information from filename: {file_stem}")
            # Use default value if date extraction fails
            return None, self.csv_dir

        # Extract date range string
        date_str = date_match.group(1)

        # Create CSV output directory, using year subdirectory structure
        year = date_str[:4]
        csv_base_dir = self.csv_dir / year
        csv_base_dir.mkdir(parents=True, exist_ok=True)

        # Extract complete date range string
        csv_time_range_str = date_match.group(0)

        return csv_base_dir, csv_time_range_str

    def _filter_time_values(self, time_values_utc):
        """
        Convert UTC times to local timezone and adjust head/tail excess times based on timezone difference

        Parameters:
            time_values_utc (array): Array of UTC time values

        Returns:
            tuple: (time_values_local, time_idx_to_keep) - List of local timezone times and indices to keep
        """
        # Get timezone offset (hours)
        tz_offset = self.tz_offset

        # Correctly convert times based on timezone offset
        # For Eastern hemisphere (UTC+X), add hours
        # For Western hemisphere (UTC-X), subtract hours
        time_values_local = pd.DatetimeIndex(time_values_utc) + pd.Timedelta(hours=tz_offset)

        # Adjust head/tail based on timezone difference
        # For UTC+ (Eastern hemisphere), remove (24-tz_offset) hours from head, tz_offset hours from tail
        # For UTC- (Western hemisphere), remove -tz_offset hours from head, (24+tz_offset) hours from tail
        if tz_offset > 0:  # Eastern hemisphere (e.g., UTC+8)
            start_idx = int(24 - tz_offset)  # For UTC+8, remove first 16 hours
            end_idx = len(time_values_local) - int(tz_offset)  # For UTC+8, remove last 8 hours
        elif tz_offset < 0:  # Western hemisphere (e.g., UTC-5)
            start_idx = int(-tz_offset)  # For UTC-5, remove first 5 hours
            end_idx = len(time_values_local) - int(24 + tz_offset)  # For UTC-5, remove last 19 hours
        else:  # UTC+0
            start_idx = 0
            end_idx = len(time_values_local)

        # Ensure indices are within valid range
        start_idx = max(0, start_idx)
        end_idx = min(len(time_values_local), end_idx)

        # Get indices to keep
        time_idx_to_keep = list(range(start_idx, end_idx))

        # Filter out times to keep
        filtered_times = [time_values_local[i] for i in time_idx_to_keep]

        return filtered_times, time_idx_to_keep

    # 在 ERA5Hub 類中添加以下方法來替換原有的功能

    def _calculate_nearest_grid_points(self, stations, grid_lats, grid_lons, extract_surrounding=False):
        """
        Calculate the nearest grid point for each station

        Parameters:
            stations (list): List of stations, each containing name, lat, lon
            grid_lats (array): Grid latitude array
            grid_lons (array): Grid longitude array
            extract_surrounding (bool): Whether to extract surrounding 8 grids

        Returns:
            dict: Mapping of station names to grid indices
                  If extract_surrounding=False: {station_name: (lat_index, lon_index)}
                  If extract_surrounding=True: {station_name: [(lat_idx, lon_idx), ...]}  # 9 points list
        """
        nearest_points = {}

        for station in stations:
            # Calculate distance to all grid points and find the nearest one
            lat_idx = np.abs(grid_lats - station['lat']).argmin()
            lon_idx = np.abs(grid_lons - station['lon']).argmin()

            if not extract_surrounding:
                # Only return the nearest point
                nearest_points[station['name']] = (lat_idx, lon_idx)
            else:
                # Return 3x3 grid points
                surrounding_points = []

                # Define 3x3 grid relative offsets
                # Grid layout:  0  1  2
                #               3  4* 5
                #               6  7  8
                # Where 4 is the center point (closest to station)
                offsets = [(-1, -1), (-1, 0), (-1, 1),  # Top row: top-left, top, top-right
                           (0, -1), (0, 0), (0, 1),  # Middle row: left, center, right
                           (1, -1), (1, 0), (1, 1)]  # Bottom row: bottom-left, bottom, bottom-right

                valid_points = 0
                for d_lat, d_lon in offsets:
                    new_lat_idx = lat_idx + d_lat
                    new_lon_idx = lon_idx + d_lon

                    # Check boundaries
                    if (0 <= new_lat_idx < len(grid_lats) and
                            0 <= new_lon_idx < len(grid_lons)):
                        surrounding_points.append((new_lat_idx, new_lon_idx))
                        valid_points += 1
                    else:
                        # Mark out-of-boundary points as None
                        surrounding_points.append(None)

                nearest_points[station['name']] = surrounding_points

                # Log information
                center_lat = grid_lats[lat_idx]
                center_lon = grid_lons[lon_idx]
                distance = np.sqrt((center_lat - station['lat']) ** 2 + (center_lon - station['lon']) ** 2)

                self.logger.debug(f"Station {station['name']}: "
                                  f"Target({station['lat']:.3f}, {station['lon']:.3f}) -> "
                                  f"Center({center_lat:.3f}, {center_lon:.3f}), "
                                  f"Distance: {distance:.3f}°, "
                                  f"Valid surrounding points: {valid_points}/9")

        return nearest_points

    def extract_station_data_to_csv(self, netcdf_file, stations, variables, extract_surrounding=False):
        """
        Extract data for specific stations from a netCDF file and convert to CSV
        Time is automatically converted to the user's timezone and filtered to match the user's requested range

        Parameters:
            netcdf_file (str or Path): Path to the netCDF file
            stations (list): List of stations
            variables (list): List of variables to extract
            extract_surrounding (bool): Whether to extract 3x3 grid around each station

        Returns:
            list: List of paths to the generated CSV files
        """
        try:
            # Open netCDF file
            self.logger.info(f"Extracting station data from file: {netcdf_file}")
            if extract_surrounding:
                self.logger.info("Will extract 3x3 grid (9 points) around each station")

            try:
                ds = xr.open_dataset(netcdf_file, chunks='auto')
            except ValueError as e:
                # dask is not installed — fall back to eager load.
                if 'chunk manager' not in str(e):
                    raise
                self.logger.warning(
                    "dask not available; opening NetCDF without chunking. "
                    "Install with: pip install 'dask[array]'"
                )
                ds = xr.open_dataset(netcdf_file)

            # Ensure target variables exist in the dataset (considering aliases)
            self.logger.info(f"Requested variables: {variables}, ds.data_vars: {list(ds.data_vars)}")

            if not self.available_vars_for_ds:
                self.logger.warning(f"Warning: Requested variables not found in netCDF file")
                self.logger.warning(f"Requested variables: {variables}, Available: {list(ds.data_vars)}")
                ds.close()
                return []

            # Get grid coordinates and calculate nearest grid points
            grid_lats, grid_lons = ds.latitude.values, ds.longitude.values
            nearest_grid_points = self._calculate_nearest_grid_points(stations, grid_lats, grid_lons,
                                                                      extract_surrounding)

            csv_files = []

            # Prepare CSV file path and directory
            csv_base_dir, csv_time_range_str = self._prepare_csv_paths(netcdf_file)

            # Output station count
            self.logger.info(f"Extracting data for {len(stations)} stations, "
                             f"and converting UTC time to {self.timezone} (UTC{'+' if self.tz_offset >= 0 else ''}{int(self.tz_offset)}), "
                             f"path={csv_base_dir}")

            # Process each variable
            for var_name in self.available_vars_for_ds:
                var_data = ds[var_name]

                # Process data based on dimensions
                if len(var_data.dims) in (3, 4):
                    self._process_variable(
                        var_name, var_data, nearest_grid_points, stations, csv_base_dir, csv_time_range_str, csv_files,
                        extract_surrounding
                    )
                else:
                    self.logger.warning(f"Cannot process variable with {len(var_data.dims)} dimensions: {var_name}")

            ds.close()

            if csv_files:
                self.logger.debug(f"Generated {len(csv_files)} CSV files with {self.timezone} timezone")
            else:
                self.logger.debug("No CSV files generated - no data found in requested time range")

            return csv_files

        except Exception as e:
            self.logger.error(f"Failed to extract station data: {str(e)}")
            self.logger.error(f"Station data extraction error: {str(e)}", exc_info=True)
            return []

    def _process_variable(self, var_name, var_data, nearest_grid_points, stations,
                          csv_base_dir, csv_time_range_str, csv_files, extract_surrounding=False):
        """Process 3D (time, lat, lon) or 4D (time, level, lat, lon) variable"""
        has_levels = len(var_data.dims) == 4
        level_items = enumerate(var_data.level.values) if has_levels else [(None, None)]

        for level_idx, level_val in level_items:
            level_str = f"{int(level_val)}" if level_val is not None else None

            # Extract time values
            time_field = 'valid_time' if 'valid_time' in var_data.dims else 'time'
            time_values_utc = pd.to_datetime(var_data[time_field].values)
            filtered_times, time_idx_to_keep = self._filter_time_values(time_values_utc)

            label = f"{var_name} at level {level_str}" if level_str else var_name
            if not filtered_times:
                self.logger.warning(f"No data found for {label} in the requested time range")
                continue

            results = {
                'time': [t.strftime('%Y-%m-%d %H:%M:%S') for t in filtered_times]
            }

            def _get_time_series(lat_idx, lon_idx):
                """Extract full time series at a spatial point, handling 3D/4D."""
                if has_levels:
                    return var_data.values[:, level_idx, lat_idx, lon_idx]
                return var_data.values[:, lat_idx, lon_idx]

            if not extract_surrounding:
                for station in stations:
                    lat_idx, lon_idx = nearest_grid_points[station['name']]
                    full_ts = _get_time_series(lat_idx, lon_idx)
                    results[station['name']] = [full_ts[i] for i in time_idx_to_keep]
            else:
                for station in stations:
                    station_name = station['name']
                    grid_points = nearest_grid_points[station_name]
                    surrounding_values_by_time = [[] for _ in range(len(filtered_times))]

                    for i, point in enumerate(grid_points):
                        column_name = f"{station_name}_grid_{i}"
                        if point is None:
                            results[column_name] = [None] * len(filtered_times)
                        else:
                            lat_idx, lon_idx = point
                            full_ts = _get_time_series(lat_idx, lon_idx)
                            filtered_ts = [full_ts[j] for j in time_idx_to_keep]
                            results[column_name] = filtered_ts
                            for t_idx, value in enumerate(filtered_ts):
                                if not np.isnan(value) and not np.isinf(value):
                                    surrounding_values_by_time[t_idx].append(value)

                    mean_values = []
                    std_values = []
                    for t_idx in range(len(filtered_times)):
                        valid_values = surrounding_values_by_time[t_idx]
                        if valid_values:
                            mean_values.append(np.mean(valid_values))
                            std_values.append(np.std(valid_values) if len(valid_values) > 1 else 0.0)
                        else:
                            mean_values.append(None)
                            std_values.append(None)
                    results[f"{station_name}_mean"] = mean_values
                    results[f"{station_name}_std"] = std_values

            df = pd.DataFrame(results)
            if df.empty:
                self.logger.warning(f"No data found for {label} in the requested time range")
                continue

            suffix = "_3x3" if extract_surrounding else ""
            csv_file = self._save_csv_file(df, var_name, level_str, csv_base_dir, csv_time_range_str, suffix)
            if csv_file:
                csv_files.append(str(csv_file))
                self.logger.debug(f"Saved CSV for {label}, containing {len(filtered_times)} time points")

    def _save_csv_file(self, df, var_abbr, level_str, csv_base_dir, csv_time_str, suffix=""):
        """Save data to CSV file"""
        try:
            # Generate CSV filename (使用與 raw 檔案相同的命名邏輯)
            if not csv_time_str:
                raise FileNotFoundError(f"")

            # 判斷是單層還是氣壓層數據
            prefix = "era5_pl" if level_str else "era5_sfc"

            if level_str:
                csv_file = csv_base_dir / f"{prefix}_{var_abbr}_{level_str}{suffix}_{csv_time_str}.csv"
            else:
                csv_file = csv_base_dir / f"{prefix}_{var_abbr}{suffix}_{csv_time_str}.csv"

            # Save to CSV
            df.to_csv(csv_file, index=False)
            return csv_file

        except Exception as e:
            self.logger.error(f"Error saving CSV: {str(e)}")
            return None

    def run_pipeline(self, start_date, end_date, boundary, variables=None,
                     pressure_levels=None, download_mode="monthly",
                     stations=None, extract_surrounding=False):
        """One-call ERA5 pipeline: fetch -> download -> process (per-station CSV).

        Overrides the base because ERA5 fetch stores the query internally (no
        product list), download() takes no products, and process_data extracts
        each station's time series to CSV (no maps).
        """
        self.fetch_data(start_date, end_date, boundary, variables=variables,
                        pressure_levels=pressure_levels, download_mode=download_mode)
        self.download_data()
        self.process_data(stations=stations, extract_surrounding=extract_surrounding)

    def process_data(self, stations=None, extract_surrounding=False):
        """
        Process ERA5 data and create CSV files

        Parameters:
            stations (list or None): List of stations that you when to extract to csv
            extract_surrounding (bool): Whether to extract 3x3 grid around each station

        Returns:
            list: List of paths to the generated CSV files
        """
        # Ensure fetch_data has been executed
        self.stations = stations

        if not hasattr(self, 'start_date') or not hasattr(self, 'end_date'):
            self.logger.warning("No time range information found, please execute the fetch_data method first")
            return []

        # Check if station information is available
        if not hasattr(self, 'stations') or not self.stations:
            self.logger.warning("No station information provided, cannot extract station data")
            return []

        # Check if query results exist
        if not hasattr(self, 'query_results') or not self.query_results:
            self.logger.warning("No query results found, please execute fetch_data and download_data methods first")
            return []

        # Prepare date range string for logging
        date_range_str = f"Date range: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}"
        self.logger.info(f"Starting ERA5 data processing, {date_range_str}")

        if extract_surrounding:
            self.logger.info("Using 3x3 grid extraction mode")

        csv_files = []

        # Process each downloaded file
        for item in self.query_results:
            target_file = Path(item['file_path'])

            # Check if file exists
            if not target_file.exists():
                self.logger.warning(f"File does not exist: {target_file}")
                continue

            # Match _YYYYMMDD_YYYYMMDD.nc format
            if re.search(r'_(\d{8})_(\d{8})\.nc$', target_file.name):
                # Extract station data
                file_csv_files = self.extract_station_data_to_csv(
                    target_file,
                    self.stations,
                    self.variables,
                    extract_surrounding
                )

                if file_csv_files:
                    csv_files.extend(file_csv_files)
            else:
                self.logger.warning(f"Cannot extract year-month information from filename: {target_file}")

        # Return results
        if csv_files:
            extraction_type = "3x3 grid" if extract_surrounding else "single point"
            self.logger.info(
                f"Processing completed, generated {len(csv_files)} CSV files using {extraction_type} extraction")

            # 複製 CSV 檔案到額外的目錄
            self._copy_csv_to_processed_dir(csv_files)
        else:
            self.logger.info("No CSV files were generated")

        return csv_files

    def _copy_csv_to_processed_dir(self, csv_files):
        """
        複製 CSV 檔案到 ~/DataCenter/Processed/ERA5/

        Parameters:
            csv_files (list): CSV 檔案路徑列表
        """
        try:
            # 目標目錄
            target_dir = Path.home() / "DataCenter" / "Processed" / "ERA5"
            target_dir.mkdir(parents=True, exist_ok=True)

            copied_count = 0
            for csv_file in csv_files:
                csv_path = Path(csv_file)
                if csv_path.exists():
                    # 目標檔案路徑（保持原檔名）
                    target_file = target_dir / csv_path.name

                    # 複製檔案
                    shutil.copy2(csv_path, target_file)
                    copied_count += 1
                    self.logger.debug(f"已複製: {csv_path.name} -> {target_dir}")
                else:
                    self.logger.warning(f"檔案不存在，無法複製: {csv_path}")

            if copied_count > 0:
                self.logger.info(f"已複製 {copied_count} 個 CSV 檔案到 {target_dir}")

        except Exception as e:
            self.logger.error(f"複製 CSV 檔案失敗: {str(e)}")

    def analyze_3x3_grid_data(self, csv_file, station_name, variable_name, date_index=0, level=None):
        """
        Analyze 3x3 grid data for a specific station and variable

        Parameters:
            csv_file (str): CSV file path
            station_name (str): Station name
            variable_name (str): Variable name (abbreviated)
            date_index (int): Date index to analyze (row number)
            level (str): Pressure level (if applicable)

        Returns:
            dict: Contains grid data and statistics
        """
        try:
            df = pd.read_csv(csv_file)

            if date_index >= len(df):
                self.logger.error(f"Date index {date_index} out of range (max: {len(df) - 1})")
                return None

            # Check if this file has 3x3 grid data
            grid_cols = [f"{station_name}_grid_{i}" for i in range(9)]
            missing_cols = [col for col in grid_cols if col not in df.columns]

            if missing_cols:
                self.logger.error(f"Missing 3x3 grid columns for station {station_name}: {missing_cols}")
                return None

            # Extract specified date data
            row_data = df.iloc[date_index]
            time_str = row_data['time']

            # Build 3x3 grid
            grid_3x3 = np.full((3, 3), np.nan)
            grid_positions = [
                (0, 0), (0, 1), (0, 2),  # Top row
                (1, 0), (1, 1), (1, 2),  # Middle row
                (2, 0), (2, 1), (2, 2)  # Bottom row
            ]

            grid_values = []
            for i in range(9):
                value = row_data[f"{station_name}_grid_{i}"]
                if not pd.isna(value):
                    row, col = grid_positions[i]
                    grid_3x3[row, col] = value
                    grid_values.append(value)

            # Get statistics
            mean_value = row_data.get(f"{station_name}_mean", None)
            std_value = row_data.get(f"{station_name}_std", None)

            result = {
                'station_name': station_name,
                'variable_name': variable_name,
                'level': level,
                'time': time_str,
                'grid_3x3': grid_3x3,
                'grid_values': grid_values,
                'mean': mean_value,
                'std': std_value,
                'valid_points': len(grid_values),
                'center_value': grid_3x3[1, 1] if not np.isnan(grid_3x3[1, 1]) else None
            }

            return result

        except Exception as e:
            self.logger.error(f"Error analyzing 3x3 grid data: {str(e)}")
            return None