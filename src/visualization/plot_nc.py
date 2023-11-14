import cartopy.crs as ccrs
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.mpl.gridliner as gridliner
import time
import geopandas as gpd

from scipy.signal import convolve2d
from netCDF4 import Dataset
from matplotlib.ticker import ScalarFormatter
from pathlib import Path
from src.processing.taiwan_frame import TaiwanFrame


taiwan_counties = gpd.read_file(Path(__file__).parent / "taiwan/COUNTY_MOI_1090820.shp")
station = gpd.read_file(Path(__file__).parent / "stations/空氣品質監測站位置圖_121_10704.shp")


# @setFigure
def platecarree_plot(dataset, zoom=True, path=None, **kwargs):
    fig, ax = plt.subplots(figsize=(7, 6), subplot_kw={'projection': ccrs.PlateCarree()})

    if zoom:
        # 添加經緯度網格線和標籤
        ax.set_extent([119, 123, 21, 26], crs=ccrs.PlateCarree())
        gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True, linewidth=1, color='gray', alpha=0.5)
        gl.xlocator = plt.FixedLocator([119, 120, 121, 122, 123])
        gl.top_labels = gl.right_labels = False
        # ax.coastlines(resolution='10m', color='gray', linewidth=1)
        ax.add_geometries(taiwan_counties['geometry'], crs=ccrs.PlateCarree(), edgecolor='black', facecolor='none', linewidth=1)
    else:
        ax.set_global()
        gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True, linewidth=1, color='gray', alpha=0.5)
        gl.top_labels = gl.right_labels = False
        # ax.coastlines(resolution='10m', color='black', linewidth=0.5)
        ax.add_geometries(taiwan_counties['geometry'], resolution='10m', crs=ccrs.PlateCarree(), edgecolor='black', facecolor='none')

    var = 'nitrogendioxide_tropospheric_column'

    plot = dataset[var][0].plot.pcolormesh(ax=ax, x='longitude', y='latitude', add_colorbar=False, cmap='jet', vmin=0,
                                           vmax=1.4e-4)
    cbar = plt.colorbar(plot, ax=ax, shrink=1, pad=0.05)
    cbar.set_label(r'$\bf NO_{2}\ mole/m^2$')

    # 设置colorbar刻度标签格式为科学记数法
    cbar.formatter = ScalarFormatter(useMathText=True, useOffset=True)
    cbar.formatter.set_powerlimits((-2, 2))  # 设置指数的显示范围
    cbar.update_ticks()

    plt.title(kwargs.get('title'))
    if path is not None:
        fig.savefig(path)
    plt.show()


def orthographic_plot(dataset):
    projection = ccrs.Orthographic(120, 25)
    fig, ax = plt.subplots(figsize=(8, 6), subplot_kw={'projection': projection})

    var = 'nitrogendioxide_tropospheric_column'
    vmin = dataset[var][0].min()

    dataset[var][0].plot.pcolormesh(ax=ax, x='longitude', y='latitude',
                                    add_colorbar=True, cmap='jet',
                                    transform=ccrs.PlateCarree(),
                                    vmin=vmin)

    ax.set_global()
    ax.coastlines(resolution='10m')


def smooth_kernel(data, kernel_size=5):
    """

    :param data:
    :param kernel_size:
    :return:
    """
    kernel = np.ones((kernel_size, kernel_size))

    return convolve2d(data, kernel, mode='same', boundary='wrap') / np.sum(kernel)


if __name__ == '__main__':
    year = '2022'  # 改年度即可使用
    folder_path = Path(__file__).parent / "ncData"
    output_folder = Path(__file__).parent / "Figure"

    lon_coordinate, lat_coordinate = TaiwanFrame().frame()
    container = []

    # 遍历文件夹中符合条件的文件
    for file_path in folder_path.glob(f"NO2_{year}*.nc"):
        dataset = xr.open_dataset(file_path)

        # 提取数据并进行平滑处理
        traget_data = dataset.nitrogendioxide_tropospheric_column[0].data
        container.append(traget_data)

        dataset.nitrogendioxide_tropospheric_column[0] = smooth_kernel(traget_data)

        # 生成图像文件并保存到输出文件夹
        # print('plot: ' + file_path.name)
        # output_path = output_folder / f"{file_path.stem}.png"
        # platecarree_plot(dataset, path=output_path,
        #                  title=rf'$\bf {file_path.stem[4:8]}-{file_path.stem[8:10]}\ \ NO_{2}$')


    # 计算年平均值
    no2_year_average = np.nanmean(container, axis=0)
    original_shape = no2_year_average.shape

    ds_time = np.array(np.datetime64(year, 'ns'))
    ds_result = xr.Dataset(
        {'nitrogendioxide_tropospheric_column': (['latitude', 'longitude'], no2_year_average.reshape(*original_shape))},
        coords={'latitude': lat_coordinate[:, 0],
                'longitude': lon_coordinate[0]})

    ds_result = ds_result.expand_dims(time=[ds_time])

    # 畫年平均
    year_ds = ds_result.copy(deep=True)
    traget_data = year_ds.nitrogendioxide_tropospheric_column[0].data
    year_ds['nitrogendioxide_tropospheric_column'][0] = smooth_kernel(traget_data)

    output_path = output_folder / f"NO2_{year}.png"
    platecarree_plot(year_ds, path=output_path, title=rf'$\bf {year}\ \ NO_{2}$')
