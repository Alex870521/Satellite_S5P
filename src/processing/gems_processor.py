"""GEMS L2 處理器：把 GEMS swath (Data Fields / Geolocation Fields) 內插到規則網格、
存成標準 NetCDF 並繪圖。輸出格式與 SentinelProcessor 一致 (time, latitude, longitude)，
因此可直接沿用 src.visualization.plot_nc.plot_global_var。

GEMS 原始檔結構（GK2_GEMS_L2_*.nc，HDF5 群組）：
    Data Fields/        ColumnAmountNO2, CloudFraction, FinalAlgorithmFlags, ...
    Geolocation Fields/ Latitude, Longitude, Time, SolarZenithAngle, ...
    METADATA/
經緯度與資料同為 2D swath（如 2048 x 695）。
"""
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import xarray as xr

from src.processing.interpolators import DataInterpolator
from src.processing.grid_frame import GridFrame
from src.config.settings import FIGURE_BOUNDARY
from src.config.catalog import ProductConfig
from src.visualization.plot_nc import plot_global_var
from src.visualization.gif import animate_data


def gems_datetime_from_filename(filename: str) -> datetime | None:
    """GK2_GEMS_L2_20230515_0345_NO2_FW_DPRO_ORI.nc -> datetime(2023,5,15,3,45)（UTC）"""
    m = re.search(r'_(\d{8})_(\d{4})_', filename)
    if m:
        try:
            return datetime.strptime(m.group(1) + m.group(2), '%Y%m%d%H%M')
        except ValueError:
            return None
    return None


class GEMSProcessor:
    """處理 GEMS 數據並生成網格化 NetCDF 與圖像。"""

    DATA_GROUP = "Data Fields"
    GEO_GROUP = "Geolocation Fields"

    # 友善名稱 -> 產品設定。dataset_name = Data Fields 內的變數名（也是輸出網格變數名）。
    PRODUCTS: dict[str, ProductConfig] = {
        'NO2': ProductConfig('NO₂', 'ColumnAmountNO2', 0, 1.0e16, 'molecules cm-2',
                             'GEMS NO₂ Total Column', cmap='turbo'),
        'O3T': ProductConfig('O₃', 'ColumnAmountO3', 200, 400, 'DU',
                             'GEMS O₃ Total Column', cmap='viridis'),
        'HCHO': ProductConfig('HCHO', 'ColumnAmountHCHO', 0, 2.0e16, 'molecules cm-2',
                              'GEMS HCHO Column', cmap='turbo'),
        'SO2': ProductConfig('SO₂', 'ColumnAmountSO2', 0, 1.0e16, 'molecules cm-2',
                             'GEMS SO₂ Column', cmap='turbo'),
        'AERAOD': ProductConfig('AOD', 'AerosolOpticalDepth', 0, 2.0, 'unitless',
                                'GEMS Aerosol Optical Depth', cmap='YlOrBr'),
        'UVI': ProductConfig('UVI', 'UVIndex', 0, 12, 'unitless',
                             'GEMS UV Index', cmap='magma'),
    }

    # GEMS 像素 ~3.5 km (南北) x 8 km (東西)：GridFrame 解析度為 (x_km=經度, y_km=緯度)
    DEFAULT_RESOLUTION = (8.0, 3.5)

    def __init__(self,
                 file_type: str = 'NO2',
                 interpolation_method: str = 'rbf',
                 resolution: tuple[float, float] | None = None,
                 qc_flag_var: str | None = 'FinalAlgorithmFlags',
                 qc_good_value: int = 0,
                 cloud_max: float | None = None,
                 mask_negative: bool = True):
        """
        Parameters:
            file_type: 產品（見 PRODUCTS）
            interpolation_method: 'rbf' / 'kdtree' / 'griddata'
            resolution: 網格解析度 (x_km, y_km)；None 用 DEFAULT_RESOLUTION
            qc_flag_var: 品質旗標變數名，保留 == qc_good_value 的像素；None 表示不套用
            qc_good_value: 視為「最佳品質」的旗標值（0 = 無旗標設定）
            cloud_max: 雲量上限（CloudFraction <= cloud_max 才保留）；None 表示不過濾
            mask_negative: 是否剔除負值（柱量不應為負）
        """
        # 由 GEMSHub.processor 注入
        self.raw_dir: Path | None = None
        self.processed_dir: Path | None = None
        self.figure_dir: Path | None = None
        self.logger = None

        self.file_type = file_type
        self.interpolation_method = interpolation_method
        self.qc_flag_var = qc_flag_var
        self.qc_good_value = qc_good_value
        self.cloud_max = cloud_max
        self.mask_negative = mask_negative

        if resolution is not None:
            self.resolution = resolution
        else:
            cfg = self.PRODUCTS.get(file_type)
            self.resolution = (cfg.resolution if cfg and cfg.resolution else self.DEFAULT_RESOLUTION)
        self.grid_frame = GridFrame(self.resolution)

    # ------------------------------------------------------------------ #
    @property
    def config(self) -> ProductConfig:
        if self.file_type not in self.PRODUCTS:
            raise ValueError(f"未支援的 GEMS 產品: {self.file_type}. 支援: {sorted(self.PRODUCTS)}")
        return self.PRODUCTS[self.file_type]

    def extract_data(self, nc_file: Path, extract_range=FIGURE_BOUNDARY):
        """讀取單檔、套 QC、裁切到範圍，回傳散點 (lon, lat, var)。無有效資料回傳 None。"""
        cfg = self.config
        data = xr.open_dataset(nc_file, group=self.DATA_GROUP, engine='netcdf4', mask_and_scale=True)
        geo = xr.open_dataset(nc_file, group=self.GEO_GROUP, engine='netcdf4', mask_and_scale=True)
        try:
            if cfg.dataset_name not in data:
                self.logger.error(f"{nc_file.name}: 找不到變數 {cfg.dataset_name}（有: {list(data.data_vars)[:8]}…）")
                return None

            var = np.asarray(data[cfg.dataset_name].values, dtype='float64')
            lat = np.asarray(geo['Latitude'].values, dtype='float64')
            lon = np.asarray(geo['Longitude'].values, dtype='float64')

            mask = np.isfinite(var) & np.isfinite(lat) & np.isfinite(lon)
            n_raw = int(mask.sum())

            if self.mask_negative:
                mask &= (var > 0)
            if self.qc_flag_var and self.qc_flag_var in data:
                mask &= (np.asarray(data[self.qc_flag_var].values) == self.qc_good_value)
            if self.cloud_max is not None and 'CloudFraction' in data:
                cf = np.asarray(data['CloudFraction'].values, dtype='float64')
                mask &= np.isfinite(cf) & (cf <= self.cloud_max)

            if extract_range is not None:
                lon_min, lon_max, lat_min, lat_max = extract_range
                mask &= (lon >= lon_min) & (lon <= lon_max) & (lat >= lat_min) & (lat <= lat_max)

            n_keep = int(mask.sum())
            self.logger.info(f"{nc_file.name}: QC 後保留 {n_keep:,} / 原始有效 {n_raw:,} 點（範圍內）")
            if n_keep < 10:
                self.logger.info(f"{nc_file.name}: 範圍內有效點過少，跳過")
                return None

            return lon[mask], lat[mask], var[mask]
        finally:
            data.close()
            geo.close()

    def _process_data(self, nc_file: Path) -> xr.Dataset | None:
        result = self.extract_data(nc_file, extract_range=FIGURE_BOUNDARY)
        if result is None:
            return None
        lon, lat, var = result

        lon_grid, lat_grid = self.grid_frame.get_grid(custom_bounds=FIGURE_BOUNDARY)

        # 依資料密度動態調整內插搜尋半徑（以格數計）
        cell_deg = (abs(lon_grid[0, 1] - lon_grid[0, 0]) + abs(lat_grid[1, 0] - lat_grid[0, 0])) / 2.0
        n_valid = var.size
        n_cells = 2 if n_valid < 100 else 3 if n_valid < 500 else 4
        max_distance = n_cells * cell_deg

        var_grid = DataInterpolator.interpolate(
            lon, lat, var, lon_grid, lat_grid,
            method=self.interpolation_method,
            max_distance=max_distance,
            rbf_function='thin_plate',
        )

        file_time = gems_datetime_from_filename(nc_file.name) or datetime(1970, 1, 1)
        cfg = self.config
        return xr.Dataset(
            {cfg.dataset_name: (['time', 'latitude', 'longitude'], var_grid[np.newaxis, :, :])},
            coords={
                'time': np.array([np.datetime64(file_time, 'ns')]),
                'latitude': np.squeeze(lat_grid[:, 0]),
                'longitude': np.squeeze(lon_grid[0, :]),
            },
            attrs={
                'units': cfg.units,
                'time': str(np.datetime64(file_time, 's')),
                'description': cfg.title,
                'satellite': 'GEMS (GK-2B)',
                'processing_method': self.interpolation_method,
                'resolution_km': str(self.resolution),
                'qc_flag': f"{self.qc_flag_var}=={self.qc_good_value}" if self.qc_flag_var else 'none',
                'cloud_max': 'none' if self.cloud_max is None else str(self.cloud_max),
            },
        )

    def process_nc_file(self, nc_file: Path, output_dir: Path, skip_existing: bool = False) -> bool:
        output_file = output_dir / nc_file.name
        if skip_existing and output_file.exists():
            self.logger.info(f"跳過已存在: {nc_file.name}")
            return True

        ftime = gems_datetime_from_filename(nc_file.name)
        self.logger.info(f"處理: {nc_file.name} ({ftime})")
        try:
            ds = self._process_data(nc_file)
            if ds is None:
                return False
            output_dir.mkdir(parents=True, exist_ok=True)
            ds.to_netcdf(output_file)
            return True
        except Exception as e:
            self.logger.error(f"處理 {nc_file.name} 失敗: {e}")
            return False

    def process_one(self, nc_file: Path, make_figure: bool = True,
                    skip_existing: bool = False) -> str:
        """網格化單一 granule（並可選擇繪圖）。

        回傳狀態：
            'ok'    已輸出網格化 NetCDF（+圖）
            'empty' 檔案有效但範圍(台灣)內無有效資料 → 呼叫端可標記、不需重試
            'error' 解析/處理發生錯誤 → 呼叫端宜保留原始檔以便檢查
        """
        dt = gems_datetime_from_filename(nc_file.name)
        if dt is None:
            self.logger.warning(f"無法解析時間: {nc_file.name}")
            return 'error'
        year, month = dt.strftime('%Y'), dt.strftime('%m')
        out_dir = self.processed_dir / self.file_type / year / month
        processed = out_dir / nc_file.name
        if skip_existing and processed.exists() and processed.stat().st_size > 0:
            return 'ok'

        try:
            ds = self._process_data(nc_file)
        except Exception as e:
            self.logger.error(f"處理 {nc_file.name} 失敗: {e}")
            return 'error'
        if ds is None:
            # _process_data 僅在「範圍內無有效資料」時回 None（其餘狀況拋例外）
            return 'empty'

        out_dir.mkdir(parents=True, exist_ok=True)
        ds.to_netcdf(processed)
        if make_figure:
            try:
                fig_dir = self.figure_dir / self.file_type / year / month
                fig_dir.mkdir(parents=True, exist_ok=True)
                plot_global_var(
                    dataset=processed,
                    product_params=self.config,
                    savefig_path=fig_dir / f"{nc_file.stem}.png",
                    map_scale='Taiwan',
                    mark_stations=None,
                )
            except Exception as e:
                self.logger.error(f"繪圖 {nc_file.name} 失敗: {e}")
        return 'ok'

    def animate_month(self, year: str, month: str):
        """把某年月的 PNG 串成逐時動畫 GIF（需 >1 張）。"""
        fig_dir = self.figure_dir / self.file_type / year / month
        if len(sorted(fig_dir.glob("*.png"))) <= 1:
            return
        try:
            animate_data(
                image_dir=fig_dir,
                output_path=fig_dir / f"GEMS_{self.file_type}_{year}{month}_animation.gif",
                date_type='auto', fps=2,
            )
        except Exception as e:
            self.logger.error(f"製作 {year}-{month} 動畫失敗: {e}")

    def _discover_raw_files(self, pattern=None, start_date=None, end_date=None) -> list[Path]:
        """列出 raw_dir 內符合產品/日期範圍的原始檔，依時間排序。"""
        if pattern is None:
            pattern = f"**/{self.file_type}/**/*.nc"
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        out = []
        for f in self.raw_dir.glob(pattern):
            if f.name.startswith('._') or not f.is_file():
                continue
            d = gems_datetime_from_filename(f.name)
            if not d:
                continue
            if start_date and d < start_date:
                continue
            if end_date and d > end_date.replace(hour=23, minute=59, second=59):
                continue
            out.append(f)
        out.sort(key=lambda f: gems_datetime_from_filename(f.name))
        return out

    def process_all_files(self, pattern=None, start_date=None, end_date=None,
                          skip_existing: bool = False, make_figures: bool = True,
                          make_animation: bool = True) -> bool:
        """處理 raw_dir 內日期範圍的所有 GEMS 原始檔（逐檔網格化＋繪圖，按月製作動畫）。"""
        files = self._discover_raw_files(pattern, start_date, end_date)
        self.logger.info(f"找到 {len(files)} 個 GEMS {self.file_type} 原始檔")
        if not files:
            return False
        months: set[tuple[str, str]] = set()
        n = 0
        for nc_file in files:
            if self.process_one(nc_file, make_figure=make_figures, skip_existing=skip_existing) == 'ok':
                n += 1
                dt = gems_datetime_from_filename(nc_file.name)
                months.add((dt.strftime('%Y'), dt.strftime('%m')))
        if make_animation and make_figures:
            for y, m in sorted(months):
                self.animate_month(y, m)
        self.logger.info(f"GEMS 處理完成：成功 {n} 檔，共 {len(months)} 個月")
        return n > 0

    def merge_processed(self, start_date=None, end_date=None,
                        output_path: Path | str | None = None) -> Path | None:
        """把已網格化的逐時 NetCDF 沿時間軸合併成單一檔案 (time, latitude, longitude)。

        所有網格檔共用同一台灣網格，故可直接 concat。輸出含 zlib 壓縮。
        回傳合併檔路徑（無資料回 None）。
        """
        var = self.config.dataset_name
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        files = []
        for f in self.processed_dir.glob(f"{self.file_type}/**/*.nc"):
            if f.name.startswith('._'):
                continue
            d = gems_datetime_from_filename(f.name)
            if not d:
                continue
            if start_date and d < start_date:
                continue
            if end_date and d > end_date.replace(hour=23, minute=59, second=59):
                continue
            files.append(f)
        if not files:
            self.logger.warning("找不到可合併的網格化檔")
            return None
        files.sort(key=lambda f: gems_datetime_from_filename(f.name))

        self.logger.info(f"合併 {len(files)} 個網格化檔...")
        times, arrs = [], []
        lat = lon = None
        for f in files:
            ds = xr.open_dataset(f)
            if lat is None:
                lat, lon = ds['latitude'].values, ds['longitude'].values
            times.append(ds['time'].values[0])
            arrs.append(ds[var].values[0])
            ds.close()

        order = np.argsort(np.array(times))
        times = np.array(times)[order]
        data = np.stack(arrs)[order].astype('float32')

        merged = xr.Dataset(
            {var: (['time', 'latitude', 'longitude'], data)},
            coords={'time': times, 'latitude': lat, 'longitude': lon},
            attrs={
                'title': f"{self.config.title} — merged time series",
                'satellite': 'GEMS (GK-2B)', 'units': self.config.units,
                'n_timesteps': len(files),
                'time_coverage_start': str(times[0]),
                'time_coverage_end': str(times[-1]),
            },
        )
        merged[var].attrs['units'] = self.config.units

        if output_path is None:
            tag = f"{str(times[0])[:10]}_{str(times[-1])[:10]}".replace('-', '')
            output_path = self.processed_dir / f"GEMS_{self.file_type}_merged_{tag}.nc"
        output_path = Path(output_path)
        merged.to_netcdf(output_path, encoding={var: {'zlib': True, 'complevel': 4}})
        self.logger.info(f"合併完成 → {output_path} ({output_path.stat().st_size / 1024 / 1024:.1f} MB, "
                         f"{len(files)} 時間步)")
        return output_path
