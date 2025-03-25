import logging
import geopandas as gpd
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
from typing import Literal
from pathlib import Path
from shapely.geometry import Point
from scipy.signal import convolve2d
from matplotlib.ticker import ScalarFormatter, FixedLocator

from src.config.settings import FIGURE_BOUNDARY
from src.config.richer import DisplayManager
from src.config.catalog import PRODUCT_CONFIGS
from src.utils.extract_datetime_from_filename import extract_datetime_from_filename


plt.rcParams['mathtext.fontset'] = 'custom'
plt.rcParams['mathtext.rm'] = 'Times New Roman'
plt.rcParams['mathtext.it'] = 'Times New Roman: italic'
plt.rcParams['mathtext.bf'] = 'Times New Roman: bold'
plt.rcParams['mathtext.default'] = 'regular'
plt.rcParams['font.family'] = 'Times New Roman'
plt.rcParams['font.weight'] = 'normal'
plt.rcParams['font.size'] = 16

plt.rcParams['axes.titlesize'] = 'large'
plt.rcParams['axes.titleweight'] = 'bold'
plt.rcParams['axes.labelweight'] = 'bold'


logger = logging.getLogger(__name__)


def smooth_kernel(data, kernel_size=5):
    kernel = np.ones((kernel_size, kernel_size))
    return convolve2d(data, kernel, mode='same', boundary='wrap') / np.sum(kernel)


def plot_stations(ax, stations: list[str], label_offset: tuple[float, float] = (-0.2, 0)):
    """繪製測站標記和標籤

    Args:
        ax: matplotlib axes
        stations: 要標記的測站名稱列表
        label_offset: (x偏移, y偏移)，用於調整標籤位置
    """
    # 讀取測站資料
    station_data = gpd.read_file(
        Path(__file__).parents[2] / "data/shapefiles/stations/空氣品質監測站位置圖_121_10704.shp"
    )
    # 創建測站的 GeoDataFrame
    station_geometry = [Point(xy) for xy in zip(station_data['TWD97Lon'], station_data['TWD97Lat'])]

    # all station
    # geodata = gpd.GeoDataFrame(station_data, crs=ccrs.PlateCarree(), geometry=station_geometry)
    # geodata.plot(ax=ax, color='gray', markersize=10)

    # matplotlib 預設顏色和標記循環
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']  # 預設顏色循環
    markers = ['o', 's', '^', 'v', 'D', '<', '>', 'p', '*']  # 標記循環

    # 過濾出要標記的測站
    target_stations = station_data[station_data['SiteName'].isin(stations)]

    legend_labels = []
    legend_handles = []

    for i, (_, row) in enumerate(target_stations.iterrows()):
        # 循環使用顏色和標記
        color = colors[i % len(colors)]
        marker = markers[i % len(markers)]

        # 畫點並保存標記物件
        marker_obj = ax.scatter(row['TWD97Lon'], row['TWD97Lat'],
                                marker=marker,
                                color=color,
                                s=15,
                                transform=ccrs.PlateCarree(),
                                label=row['SiteEngNam'])

        legend_labels.append(row['SiteEngNam'])
        legend_handles.append(marker_obj)

    # 添加圖例
    ax.legend(handles=legend_handles,
              labels=legend_labels,
              loc='upper right',
              bbox_to_anchor=(1, 1),
              borderaxespad=0.)


def basic_map(ax, map_scale='Taiwan', mark_stations=None):
    """
    創建基本地圖，包含邊界、縣市、網格線等基礎元素

    Parameters:
    -----------
    ax : matplotlib.axes.Axes
        繪圖軸對象
    map_scale : str
        地圖比例尺，可選 'global', 'East_Asia', 'Taiwan'
    mark_stations : list or None
        需要標記的測站列表

    Returns:
    --------
    ax : matplotlib.axes.Axes
        添加了基本元素的繪圖軸對象
    """
    # 設定地圖範圍
    if map_scale == 'global':
        ax.set_global()
    elif map_scale == 'East_Asia':
        ax.set_extent([110, 130, 15, 35], crs=ccrs.PlateCarree())
    elif map_scale == 'Taiwan':
        ax.set_extent([119, 123, 21, 26], crs=ccrs.PlateCarree())
    else:
        raise ValueError("map_scale must be 'Taiwan' or 'East_Asia' or 'global'")

    # 根據地圖比例尺添加不同的元素
    if map_scale == 'Taiwan':
        try:
            # 讀取台灣縣市邊界
            Taiwan_gdf = gpd.read_file(Path(__file__).parents[2] / "data/shapefiles/Taiwan/COUNTY_MOI_1090820.shp")
            ax.add_geometries(Taiwan_gdf['geometry'], crs=ccrs.PlateCarree(), edgecolor='black', facecolor='none',
                              zorder=10)

            # 讀取中國邊界
            China_gdf = gpd.read_file(Path(__file__).parents[2] / "data/shapefiles/China/gadm36_CHN_0.shp")
            ax.add_geometries(China_gdf['geometry'], crs=ccrs.PlateCarree(), edgecolor='black', facecolor='none',
                              zorder=10)

        except Exception as e:
            print(f"讀取或處理縣市邊界時發生錯誤: {e}")

        # 設定網格線
        gl = ax.gridlines(draw_labels=True, linestyle='--', alpha=0.7)
        gl.top_labels = False
        gl.right_labels = False
        gl.xlocator = FixedLocator([119, 120, 121, 122, 123])  # 設定經度刻度

        # 標記測站
        if mark_stations:
            plot_stations(ax, mark_stations)

    else:
        # 添加地圖特徵
        ax.add_feature(cfeature.BORDERS.with_scale('10m'), zorder=10)
        ax.add_feature(cfeature.COASTLINE.with_scale('10m'), zorder=10)
        # ax.add_feature(cfeature.LAND.with_scale('10m'))
        # ax.add_feature(cfeature.OCEAN.with_scale('10m'))

        gl = ax.gridlines(draw_labels=True, linestyle='--', alpha=0.7)
        gl.top_labels = False
        gl.right_labels = False

    return ax


def plot_global_var(dataset: xr.Dataset | Path | str,
                    product_params,
                    show_info: bool = True,
                    savefig_path=None,
                    map_scale: Literal['global', 'East_Asia', 'Taiwan'] = 'global',
                    mark_stations: list | None = ['古亭', '忠明', '楠梓', '鳳山'],
                    mark_rectangle: bool = False,
                    ):
    """
    在全球地圖上繪製 var 分布圖
    """
    try:
        # 判斷輸入類型並適當處理
        from netCDF4 import Dataset
        if isinstance(dataset, (str, Path)):
            if 'PRODUCT' in Dataset(dataset, 'r').groups:
                ds = xr.open_dataset(dataset, engine='netcdf4', group='PRODUCT')
            else:
                ds = xr.open_dataset(dataset, engine='netcdf4')
        elif isinstance(dataset, xr.Dataset):
            ds = dataset
        else:
            raise NotImplementedError

        if show_info:
            if 'ground_pixel' not in ds._coord_names:
                lon = ds.longitude.values
                lat = ds.latitude.values

                nc_info = {'file_name': dataset.name if isinstance(dataset, Path) else '',
                           'time': np.datetime64(ds.time.values[0], 'D'),
                           'shape': f'{len(ds.latitude.values)}, {len(ds.longitude.values)}',
                           'latitude': f'{np.nanmin(lat):.2f} to {np.nanmax(lat):.2f}',
                           'longitude': f'{np.nanmin(lon):.2f} to {np.nanmax(lon):.2f}',
                           }
            else:
                lon = ds.longitude[0].values
                lat = ds.latitude[0].values

                nc_info = {'file_name': dataset.name if isinstance(dataset, Path) else '',
                           'time': np.datetime64(ds.time.values[0], 'D'),
                           'shape': ds.latitude[0].values.shape,
                           'latitude': f'{np.nanmin(lat):.2f} to {np.nanmax(lat):.2f}',
                           'longitude': f'{np.nanmin(lon):.2f} to {np.nanmax(lon):.2f}',
                           }

            DisplayManager().display_product_info(nc_info)

        # 創建圖形和投影
        fig = plt.figure(figsize=(12, 8) if map_scale == 'global' else (8, 8), dpi=300)
        ax = plt.axes(projection=ccrs.PlateCarree())

        # 使用basic_map函數創建基本地圖
        ax = basic_map(ax, map_scale=map_scale, mark_stations=mark_stations)

        # 繪製數據
        dataset = ds[product_params.dataset_name][0]

        # 嘗試應用平滑，如果失敗則使用原始數據
        try:
            dataset.data = smooth_kernel(dataset.data, kernel_size=3)
        except Exception as e:
            logger.warning(f"平滑處理失敗: {str(e)}，使用原始數據")

        # 方法1：使用 ScalarFormatter
        formatter = ScalarFormatter(useMathText=True)
        formatter.set_scientific(True)
        formatter.set_powerlimits((-2, 2))  # 可以調整使用科學記號的範圍

        im = dataset.plot(
            ax=ax,
            x='longitude', y='latitude',
            cmap='RdBu_r',
            transform=ccrs.PlateCarree(),
            robust=True,  # 自動處理極端值
            vmin=product_params.vmin,
            vmax=product_params.vmax,
            zorder=1,  # 添加 zorder 參數，設定圖層順序
            cbar_kwargs={
                'label': product_params.units,
                'fraction': 0.04,  # colorbar 的寬度 (預設是 0.15)
                'pad': 0.04,  # colorbar 和圖之間的間距
                'aspect': 30,  # colorbar 的長寬比，增加這個值會讓 colorbar 變長
                'shrink': 1,
                'format': formatter,
                'extend': 'neither',
            }
        )

        # 用矩形標記數據範圍
        if mark_rectangle:
            lon_min, lon_max = float(np.nanmin(ds.longitude)), float(np.nanmax(ds.longitude))
            lat_min, lat_max = float(np.nanmin(ds.latitude)), float(np.nanmax(ds.latitude))
            rect = plt.Rectangle(
                (lon_min, lat_min),
                lon_max - lon_min,
                lat_max - lat_min,
                fill=False,
                color='red',
                transform=ccrs.PlateCarree(),
                linewidth=2
            )
            ax.add_patch(rect)

        # 設定標題
        datetime_str = extract_datetime_from_filename(savefig_path.name)
        plt.title(f'{datetime_str}', pad=20, fontdict={'weight': 'bold', 'fontsize': 24})

        plt.tight_layout()
        plt.show()

        if savefig_path is not None:
            fig.savefig(savefig_path, dpi=600)

        ds.close()

    except Exception as e:
        logger.error(f"繪圖時發生錯誤: {str(e)}")
        raise


def plot_map(dataset, product_params,
             projection_type: Literal['platecarree', 'orthographic'] = 'platecarree',
             zoom: bool = False,
             path: Path | None = None,
             **kwargs):
    """繪製地圖"""
    # 設置投影
    if projection_type == 'orthographic':
        projection = ccrs.Orthographic(120, 25)
        figsize = (8, 6)
    else:  # platecarree
        projection = ccrs.PlateCarree()
        figsize = (7, 6)

    # 創建圖表
    fig, ax = plt.subplots(figsize=figsize, subplot_kw={'projection': projection})

    # 設置範圍和網格
    if zoom:
        ax.set_extent([119, 123, 21, 26], crs=ccrs.PlateCarree())
        gl = ax.gridlines(draw_labels=True, linewidth=1, color='gray', alpha=0.5)
        gl.xlocator = plt.FixedLocator([119, 120, 121, 122, 123])
        gl.top_labels = gl.right_labels = False
    else:
        ax.set_global()
        if projection_type == 'platecarree':
            gl = ax.gridlines(draw_labels=True, linewidth=1, color='gray', alpha=0.5)
            gl.top_labels = gl.right_labels = False

    # 增加海岸線
    ax.coastlines(resolution='10m')

    # 繪製數據
    var = product_params.dataset_name
    data = dataset[var][0]

    if projection_type == 'platecarree':
        plot = data.plot.pcolormesh(
            ax=ax,
            x='longitude',
            y='latitude',
            add_colorbar=False,
            cmap='jet',
            vmin=0,
            vmax=1.4e-4,
            transform=ccrs.PlateCarree()
        )
        # 添加 colorbar
        cbar = plt.colorbar(plot, ax=ax, shrink=1, pad=0.05)
        cbar.set_label(r'$\bf NO_{2}\ mole/m^2$')

        # 設置 colorbar 格式
        formatter = ScalarFormatter(useMathText=True)
        formatter.set_scientific(True)
        formatter.set_powerlimits((-2, 2))
        cbar.formatter = formatter
    else:
        plot = data.plot.pcolormesh(
            ax=ax,
            x='longitude',
            y='latitude',
            add_colorbar=False,
            cmap='jet',
            transform=ccrs.PlateCarree(),
            vmin=data.min()
        )
        cbar = plt.colorbar(plot, ax=ax, shrink=1, pad=0.05)
        cbar.set_label(r'$\bf NO_{2}\ mole/m^2$')

    if 'title' in kwargs:
        plt.title(kwargs['title'])

    if path is not None:
        fig.savefig(path)

    plt.show()


if __name__ == "__main__":
    file_group = '/Volumes/Transcend/Sentinel-5P/raw/NO2___/2024/03/S5P_OFFL_L2__NO2____20240314T031823_20240314T045953_33252_03_020600_20240315T192700.nc'
    file_ungroup = '/Volumes/Transcend/Sentinel-5P/processed/NO2___/2024/01/S5P_OFFL_L2__NO2____20240110T045402_20240110T063532_32345_03_020600_20240111T211523.nc'
    ds = xr.open_dataset(file_ungroup)

    from netCDF4 import Dataset
    nc = Dataset(file_ungroup, 'r').groups

    file = '/Volumes/Transcend/Sentinel-5P/raw/NO2___/2022/01/S5P_OFFL_L2__NO2____20220122T041706_20220122T055836_22158_02_020301_20220123T201801.nc'
    plot_global_var(file, product_params=PRODUCT_CONFIGS['NO2___'], map_scale='Taiwan')
    # plot_map(ds, product_params=PRODUCT_CONFIGS['NO2___'], projection_type='platecarree')
    # plot_map(ds, product_params=PRODUCT_CONFIGS['NO2___'], projection_type='orthographic')
