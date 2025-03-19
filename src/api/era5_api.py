import os
import re
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
        'boundary_layer_height': 'blh',
        'temperature': 'temp',
        'u_component_of_wind': 'u',
        'v_component_of_wind': 'v',
        'convective_available_potential_energy': 'cape'
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

    def authentication(self):
        """Implements authentication method, returns a configured CDSAPI client"""
        if not os.getenv('CDSAPI_URL') or not os.getenv('CDSAPI_KEY'):
            raise EnvironmentError(
                "Missing CDS API credentials. Please set CDSAPI_URL and CDSAPI_KEY environment variables"
            )
        return cdsapi.Client(quiet=True)

    def fetch_data(self, start_date, end_date, boundary, variables=None, pressure_levels=None, stations=None):
        """
        Query ERA5 data without downloading

        Parameters:
            start_date (str or datetime): Start date/time in user's timezone
            end_date (str or datetime): End date/time in user's timezone
            boundary (list or tuple): Geographic boundary (min_lon, max_lon, min_lat, max_lat)
            variables (list): List of variables to fetch, defaults to boundary layer height
            pressure_levels (list or None): List of pressure levels (unit: hPa)
            stations (list or None): List of stations

        Returns:
            bool: Whether the query was successful
        """
        # Save necessary parameters to class attributes and standardize times
        self.start_date, self.end_date = self._normalize_time_inputs(start_date, end_date)

        (min_lon, max_lon, min_lat, max_lat) = boundary
        self.boundary = (max_lat, min_lon, min_lat, max_lon)

        self.variables = variables if variables is not None else ['boundary_layer_height']
        self.pressure_levels = pressure_levels
        self.stations = stations

        # Display request information
        self.logger.info("ERA5 data query")
        self.logger.info(
            f"User time range: {self.start_date.strftime('%Y-%m-%d %H:%M:%S %z')} to {self.end_date.strftime('%Y-%m-%d %H:%M:%S %z')}")
        self.logger.info(f"Variables: {', '.join(self.variables)}")

        # Ensure all specific directories exist
        self.single_level_dir = self.raw_dir / "single_level"
        self.csv_dir = self.processed_dir / "csv"

        dirs_to_create = [self.single_level_dir, self.csv_dir]

        # Determine if it's single level or pressure level data
        self.is_pressure_level_data = pressure_levels is not None and len(pressure_levels) > 0

        if self.is_pressure_level_data:
            self.logger.info(f"Pressure levels: {', '.join(map(str, pressure_levels))}")
            self.pressure_level_dir = self.raw_dir / "pressure_level"
            dirs_to_create.append(self.pressure_level_dir)

        # Create all necessary directories
        for dir_path in dirs_to_create:
            dir_path.mkdir(parents=True, exist_ok=True)

        # Get variable abbreviations - calculate only once
        self.available_vars_for_ds = [self.VAR_MAPPING[var] if var in self.VAR_MAPPING else var[:3].lower() for var in
                                      self.variables]
        self.var_str = '_'.join(self.available_vars_for_ds)

        # Prepare query results
        self.query_results = []

        # Get complete month list
        current_date = self.start_date.replace(
            day=1)  # Start from the beginning of the month for easier monthly iteration

        # Generate queries by month
        while current_date <= self.end_date:
            # Get current month's year/month information
            year_str = f"{current_date.year}"

            # Choose appropriate directory and create it
            base_dir = self.pressure_level_dir if self.is_pressure_level_data else self.single_level_dir
            target_dir = base_dir / year_str
            target_dir.mkdir(parents=True, exist_ok=True)

            # Calculate current month's date range
            if current_date.year == self.start_date.year and current_date.month == self.start_date.month:
                # Start month uses actual start date
                query_start_date = self.start_date
            else:
                # Non-start months begin from the 1st
                query_start_date = current_date

            # Calculate month end date
            next_month = current_date + relativedelta(months=1)
            month_end = next_month - timedelta(days=1)  # Last day of the month

            if month_end > self.end_date:
                # If month end exceeds end date, use end date
                query_end_date = self.end_date
            else:
                # Otherwise use month end
                query_end_date = month_end

            # Adjust query dates considering timezone
            if self.tz_offset > 0:  # UTC+ timezone, adjust start date one day earlier
                query_start_date = query_start_date - timedelta(days=1)
            if self.tz_offset < 0:  # UTC- timezone, adjust end date one day later
                query_end_date = query_end_date + timedelta(days=1)

            # Create date range string for filename (YYYYMMDD_YYYYMMDD)
            if month_end > self.end_date:
                file_end_date = self.end_date
            else:
                file_end_date = month_end

            file_date_range = f"{current_date.strftime('%Y%m%d')}_{file_end_date.strftime('%Y%m%d')}"

            # Generate filename
            if self.is_pressure_level_data:
                filename = f"era5_pl_{self.var_str}_{'-'.join(map(str, pressure_levels))}_{file_date_range}.nc"
                dataset = "reanalysis-era5-pressure-levels"
            else:
                filename = f"era5_sfc_{self.var_str}_{file_date_range}.nc"
                dataset = "reanalysis-era5-single-levels"

            target_file = target_dir / filename

            # Create date string for query (YYYY-MM-DD/YYYY-MM-DD)
            query_date_str = f"{query_start_date.strftime('%Y-%m-%d')}/{query_end_date.strftime('%Y-%m-%d')}"

            # Prepare request parameters
            request_params = {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': self.variables,
                'date': query_date_str,
                'time': [f"{h:02d}:00" for h in range(24)],  # Add hour parameters
                'area': self.boundary, # [north, west, south, east]
            }

            if self.is_pressure_level_data:
                request_params['pressure_level'] = pressure_levels

            # Add to query results list
            self.query_results.append({
                'target_file': str(target_file),
                'filename': filename,
                'dataset': dataset,
                'request_params': request_params,
                'exists': target_file.exists()
            })

            # Move to next month
            current_date = next_month

        self.logger.info(f"ERA5 data query completed, need to download {len(self.query_results)} months of data")
        return True

    def download_data(self):
        """
        Download ERA5 data

        Returns:
            list: Paths of downloaded netCDF files
        """
        # Check if fetch_data has been executed
        if not hasattr(self, 'query_results') or not self.query_results:
            self.logger.warning("Please execute the fetch_data method first to get query results")
            return []

        downloaded_files = []

        # Download data for each month
        for item in self.query_results:
            target_file = Path(item['target_file'])
            target_dir = target_file.parent
            filename = item['filename']
            dataset = item['dataset']
            request_params = item['request_params']

            # Ensure directory exists
            target_dir.mkdir(parents=True, exist_ok=True)

            # Check if file already exists
            if target_file.exists():
                self.logger.info(f"File already exists: {target_file}")
                downloaded_files.append(str(target_file))
                continue

            # Create temporary file path
            temp_file = target_dir / f"{filename}.temp"

            # Download file
            try:
                self.logger.info(f"Downloading: {filename}")

                # Request data from CDS API to temporary file
                self.client.retrieve(
                    dataset,
                    request_params,
                    str(temp_file)
                )

                # Rename to official filename after download completes
                temp_file.rename(target_file)
                self.logger.info(f"\nDownload completed: {target_file}")
                downloaded_files.append(str(target_file))

            except Exception as e:
                self.logger.error(f"Download failed: {filename}")
                self.logger.error(f"Error: {str(e)}")

                # Clean up any temporary files left
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

    def process_data(self):
        """
        Process ERA5 data and create CSV files

        Returns:
            list: List of paths to the generated CSV files
        """
        # Ensure fetch_data has been executed
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

        csv_files = []

        # Process each downloaded file
        for item in self.query_results:
            target_file = Path(item['target_file'])

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
                )

                if file_csv_files:
                    csv_files.extend(file_csv_files)
            else:
                self.logger.warning(f"Cannot extract year-month information from filename: {target_file}")

        # Return results
        if csv_files:
            self.logger.info(f"Processing completed, generated {len(csv_files)} CSV files")
        else:
            self.logger.info("No CSV files were generated")

        return csv_files

    def extract_station_data_to_csv(self, netcdf_file, stations, variables):
        """
        Extract data for specific stations from a netCDF file and convert to CSV
        Time is automatically converted to the user's timezone and filtered to match the user's requested range

        Parameters:
            netcdf_file (str or Path): Path to the netCDF file
            stations (list): List of stations
            variables (list): List of variables to extract

        Returns:
            list: List of paths to the generated CSV files
        """
        try:
            # Open netCDF file
            self.logger.info(f"Extracting station data from file: {netcdf_file}")
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
            nearest_grid_points = self._calculate_nearest_grid_points(stations, grid_lats, grid_lons)

            csv_files = []

            # Prepare CSV file path and directory
            csv_base_dir, csv_time_range_str = self._prepare_csv_paths(netcdf_file)

            # Output station count
            self.logger.info(f"Extracting data for {len(stations)} stations, "
                             f"and converting UTC time to {self.timezone} (UTC{'+' if self.tz_offset >= 0 else ''}{int(self.tz_offset)})")

            # Process each variable
            for var_name in self.available_vars_for_ds:
                var_data = ds[var_name]

                # Process data based on dimensions
                if len(var_data.dims) == 3:  # time, latitude, longitude
                    self._process_3d_variable(
                        var_name, var_data, nearest_grid_points, stations, csv_base_dir, csv_time_range_str, csv_files
                    )

                elif len(var_data.dims) == 4:  # time, level, latitude, longitude (pressure level data)
                    self._process_4d_variable(
                        var_name, var_data, nearest_grid_points, stations, csv_base_dir, csv_time_range_str, csv_files
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

        # Extract year
        date_str = date_match.group(1)

        # Create CSV output directory, using only year subdirectory structure
        csv_base_dir = self.csv_dir / date_str[:4]
        csv_base_dir.mkdir(parents=True, exist_ok=True)

        # Extract complete date range string
        csv_time_range_str = date_match.group(0)

        return csv_base_dir, csv_time_range_str

    def _calculate_nearest_grid_points(self, stations, grid_lats, grid_lons):
        """
        Calculate the nearest grid point for each station

        Parameters:
            stations (list): List of stations, each containing name, lat, lon
            grid_lats (array): Grid latitude array
            grid_lons (array): Grid longitude array

        Returns:
            dict: Mapping of station names to grid indices {station_name: (lat_index, lon_index)}
        """
        nearest_points = {}

        for station in stations:
            # Calculate distance to all grid points and find the nearest one
            lat_idx = np.abs(grid_lats - station['lat']).argmin()
            lon_idx = np.abs(grid_lons - station['lon']).argmin()

            nearest_points[station['name']] = (lat_idx, lon_idx)

        return nearest_points

    def _process_3d_variable(self, var_name, var_data, nearest_grid_points, stations,
                             csv_base_dir, csv_time_range_str, csv_files):
        """Process 3D variable (time, latitude, longitude)"""
        # Extract data for each time point
        time_field = 'valid_time' if 'valid_time' in var_data.dims else 'time'
        time_values_utc = pd.to_datetime(var_data[time_field].values)

        # Filter times and data
        filtered_times, time_idx_to_keep = self._filter_time_values(time_values_utc)

        if not filtered_times:
            self.logger.warning(f"No data found for {var_name} in the requested time range")
            return

        # Create results dictionary
        results = {
            'time': [t.strftime('%Y-%m-%d %H:%M:%S') for t in filtered_times]
        }

        # Extract time series for each station (filtered to requested time range)
        for station in stations:
            lat_idx, lon_idx = nearest_grid_points[station['name']]
            # First get the complete time series
            full_time_series = var_data.values[:, lat_idx, lon_idx]
            # Then filter it to include only requested times
            filtered_time_series = [full_time_series[i] for i in time_idx_to_keep]
            results[station['name']] = filtered_time_series

        # Create DataFrame
        df = pd.DataFrame(results)

        # If DataFrame is empty (no data in requested time range), skip creating CSV
        if df.empty:
            self.logger.warning(f"No data found for {var_name} in the requested time range")
            return

        # Save CSV
        csv_file = self._save_csv_file(df, var_name, None, csv_base_dir, csv_time_range_str)
        if csv_file:
            csv_files.append(str(csv_file))
            self.logger.debug(f"Saved CSV containing {len(filtered_times)} time points (after filtering)")

    def _process_4d_variable(self, var_name, var_data, nearest_grid_points, stations,
                             csv_base_dir, csv_time_range_str, csv_files):
        """Process 4D variable (time, level, latitude, longitude)"""
        # Get level values
        level_values = var_data.level.values

        # Process each pressure level
        for level_idx, level_val in enumerate(level_values):
            level_str = f"{int(level_val)}"  # Simplify level label, remove "hPa"

            # Extract data for each time point
            time_values_utc = pd.to_datetime(var_data.time.values)

            # Filter times and data
            filtered_times, time_idx_to_keep = self._filter_time_values(time_values_utc)

            if not filtered_times:
                self.logger.warning(f"No data found for {var_name} at level {level_str} in the requested time range")
                continue

            # Create results dictionary
            results = {
                'time': [t.strftime('%Y-%m-%d %H:%M:%S') for t in filtered_times]
            }

            # Extract time series for each station (filtered to requested time range)
            for station in stations:
                lat_idx, lon_idx = nearest_grid_points[station['name']]
                # First get the complete time series
                full_time_series = var_data.values[:, level_idx, lat_idx, lon_idx]
                # Then filter it to include only requested times
                filtered_time_series = [full_time_series[i] for i in time_idx_to_keep]
                results[station['name']] = filtered_time_series

            # Create DataFrame
            df = pd.DataFrame(results)

            # If DataFrame is empty (no data in requested time range), skip creating CSV
            if df.empty:
                self.logger.warning(f"No data found for {var_name} at level {level_str} in the requested time range")
                continue

            # Save CSV
            csv_file = self._save_csv_file(df, var_name, level_str, csv_base_dir, csv_time_range_str)
            if csv_file:
                csv_files.append(str(csv_file))
                self.logger.warning(
                    f"Saved CSV for level {level_str}, containing {len(filtered_times)} time points (after filtering)")

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

    def _save_csv_file(self, df, var_abbr, level_str, csv_base_dir, csv_time_str):
        """Save data to CSV file"""
        try:
            # Generate CSV filename
            if not csv_time_str:
                raise FileNotFoundError(f"")

            if level_str:
                csv_file = csv_base_dir / f"{var_abbr}_{level_str}_{csv_time_str}.csv"
            else:
                csv_file = csv_base_dir / f"{var_abbr}_{csv_time_str}.csv"

            # Save to CSV
            df.to_csv(csv_file, index=False)
            return csv_file

        except Exception as e:
            self.logger.error(f"Error saving CSV: {str(e)}")
            return None
