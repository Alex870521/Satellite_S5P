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


def plot_global_var(dataset: Path | str,
                    product_params,
                    show_info: bool = True,
                    savefig_path=None,
                    map_scale: Literal['global', 'Taiwan'] = 'global',
                    show_stations: bool = False,
                    mark_stations: list = ['古亭', '楠梓', '鳳山'],
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
            lon = ds.longitude[0].values
            lat = ds.latitude[0].values
            var = ds[product_params.dataset_name][0].values

            nc_info = {'file_name': dataset.name if isinstance(dataset, Path) else '',
                       'time': np.datetime64(ds.time.values[0], 'D'),
                       'shape': ds.latitude[0].values.shape,
                       'latitude': f'{np.nanmin(lat):.2f} to {np.nanmax(lat):.2f}',
                       'longitude': f'{np.nanmin(lon):.2f} to {np.nanmax(lon):.2f}',
                       }

            DisplayManager().display_product_info(nc_info)

        # 創建圖形和投影
        fig = plt.figure(figsize=(12, 8) if map_scale == 'global' else (8, 8))
        ax = plt.axes(projection=ccrs.PlateCarree())

        # 設定全球範圍
        if map_scale == 'global':
            ax.set_global()
        else:
            ax.set_extent(FIGURE_BOUNDARY, crs=ccrs.PlateCarree())

        # 繪製數據
        dataset = ds[product_params.dataset_name][0]
        dataset.data = smooth_kernel(dataset.data, kernel_size=3)

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

        # plot = dataset.plot.pcolormesh(ax=ax, x='longitude', y='latitude', add_colorbar=False, cmap='jet')

        # 如果是台灣範圍且需要顯示測站
        if map_scale == 'Taiwan':
            # ax.add_feature(cfeature.COASTLINE.with_scale('10m'))

            # 讀取縣市和測站資料並添加縣市邊界
            taiwan_counties = gpd.read_file(Path(__file__).parents[2] / "data/shapefiles/taiwan/COUNTY_MOI_1090820.shp")
            ax.add_geometries(taiwan_counties['geometry'], crs=ccrs.PlateCarree(), edgecolor='black', facecolor='none')

        if show_stations and mark_stations:
            plot_stations(ax, mark_stations)


        else:
            # 添加地圖特徵
            ax.add_feature(cfeature.BORDERS.with_scale('10m'), linestyle=':')
            ax.add_feature(cfeature.COASTLINE.with_scale('10m'))
            ax.add_feature(cfeature.LAND.with_scale('10m'), alpha=0.1)
            ax.add_feature(cfeature.OCEAN.with_scale('10m'), alpha=0.1)

        # 設定網格線
        gl = ax.gridlines(draw_labels=True, linestyle='--', alpha=0.7)
        gl.top_labels = False
        gl.right_labels = False
        gl.xlocator = FixedLocator([119, 120, 121, 122, 123])  # 設定經度刻度

        # 用矩形標記數據範圍
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
        time_str = np.datetime64(dataset.time.values, 'D').astype(str)
        plt.title(f'{product_params.title} {time_str}', pad=20, fontdict={'weight': 'bold', 'fontsize': 24})

        plt.tight_layout()
        plt.show()

        if savefig_path is not None:
            fig.savefig(savefig_path, dpi=600)

        ds.close()

    except Exception as e:
        logger.error(f"繪圖時發生錯誤: {str(e)}")
        raise


def platecarree_plot(dataset, product_params, zoom=True, path=None, **kwargs):
    fig, ax = plt.subplots(figsize=(7, 6), subplot_kw={'projection': ccrs.PlateCarree()})

    if zoom:
        # 添加經緯度網格線和標籤
        ax.set_extent([119, 123, 21, 26], crs=ccrs.PlateCarree())
        gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True, linewidth=1, color='gray', alpha=0.5)
        gl.xlocator = plt.FixedLocator([119, 120, 121, 122, 123])
        gl.top_labels = gl.right_labels = False
    else:
        ax.set_global()
        gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True, linewidth=1, color='gray', alpha=0.5)
        gl.top_labels = gl.right_labels = False

    var = product_params.dataset_name

    plot = dataset[var][0].plot.pcolormesh(ax=ax, x='longitude', y='latitude', add_colorbar=False, cmap='jet', vmin=0,
                                           vmax=1.4e-4)
    cbar = plt.colorbar(plot, ax=ax, shrink=1, pad=0.05)
    cbar.set_label(r'$\bf NO_{2}\ mole/m^2$')

    # 设置colorbar刻度标签格式为科学记数法
    formatter = ScalarFormatter(useMathText=True)
    formatter.set_scientific(True)
    formatter.set_powerlimits((-2, 2))  # 可以調整使用科學記號的範圍

    plt.title(kwargs.get('title'))
    if path is not None:
        fig.savefig(path)
    plt.show()


def orthographic_plot(dataset, product_params):
    projection = ccrs.Orthographic(120, 25)
    fig, ax = plt.subplots(figsize=(8, 6), subplot_kw={'projection': projection})

    var = product_params.dataset_name
    vmin = dataset[var][0].min()

    dataset[var][0].plot.pcolormesh(ax=ax, x='longitude', y='latitude',
                                    add_colorbar=True, cmap='jet',
                                    transform=ccrs.PlateCarree(),
                                    vmin=vmin)

    ax.set_global()
    ax.coastlines(resolution='10m')
    plt.show()


if __name__ == "__main__":
    file_group = '/Users/chanchihyu/Sentinel-5P/raw/NO2___/2024/03/S5P_OFFL_L2__NO2____20240314T031823_20240314T045953_33252_03_020600_20240315T192700.nc'
    file_ungroup = '/Users/chanchihyu/Sentinel-5P/processed/NO2___/2024/01/S5P_OFFL_L2__NO2____20240110T045402_20240110T063532_32345_03_020600_20240111T211523.nc'
    ds = xr.open_dataset(file_ungroup)

    from netCDF4 import Dataset
    nc = Dataset(file_ungroup, 'r').groups

    # plot_global_var(file)
