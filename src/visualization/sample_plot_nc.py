import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import warnings
from typing import Literal
from cartopy.io import DownloadWarning
from pathlib import Path
import logging

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


# warnings.filterwarnings('ignore', category=DownloadWarning)
logger = logging.getLogger(__name__)


def plot_global_no2(data,
                    savefig_path=None,
                    close_after=True,
                    map_scale: Literal['global', 'Taiwan'] = 'global'
                    ):
    """
    在全球地圖上繪製 NO2 分布圖
    """
    try:
        # 判斷輸入類型並適當處理
        if isinstance(data, (str, Path)):
            ds = xr.open_dataset(data)
            should_close = True
        else:
            ds = data
            should_close = False and close_after

        # 創建圖形和投影
        fig = plt.figure(figsize=(15, 8) if map_scale == 'global' else (10, 8))
        ax = plt.axes(projection=ccrs.PlateCarree())

        # 設定全球範圍
        if map_scale == 'global':
            ax.set_global()
        else:
            ax.set_extent([100, 145, 0, 45], crs=ccrs.PlateCarree())

        # 繪製 NO2 數據
        data = ds.nitrogendioxide_tropospheric_column[0]

        im = data.plot(
            ax=ax,
            cmap='RdBu_r',
            transform=ccrs.PlateCarree(),
            robust=True,  # 自動處理極端值
            cbar_kwargs={
                'label': f'NO$_2$ Tropospheric Column (mol/m$^2$)',
                'fraction': 0.046,  # colorbar 的寬度 (預設是 0.15)
                'pad': 0.04,  # colorbar 和圖之間的間距
                'aspect': 20,  # colorbar 的長寬比，增加這個值會讓 colorbar 變長
                'shrink': 0.8 if map_scale == 'global' else 0.9
            }
        )

        # 添加地圖特徵
        ax.add_feature(cfeature.BORDERS.with_scale('50m'), linestyle=':')
        ax.add_feature(cfeature.COASTLINE.with_scale('50m'))
        ax.add_feature(cfeature.LAND.with_scale('50m'), alpha=0.1)
        ax.add_feature(cfeature.OCEAN.with_scale('50m'), alpha=0.1)

        # 設定網格線
        gl = ax.gridlines(draw_labels=True, linestyle='--', alpha=0.5)
        gl.top_labels = False
        gl.right_labels = False

        # 用矩形標記數據範圍
        lon_min, lon_max = float(ds.longitude.min()), float(ds.longitude.max())
        lat_min, lat_max = float(ds.latitude.min()), float(ds.latitude.max())
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
        time_str = np.datetime64(data.time.values).astype(str)
        plt.title(f'NO$_2$ Tropospheric Column {time_str}', pad=20, fontdict={'weight': 'bold', 'fontsize': 24})

        plt.tight_layout()
        plt.show()

        if savefig_path is not None:
            fig.savefig(savefig_path, dpi=600)

        if should_close:
            ds.close()

    except Exception as e:
        logger.error(f"繪圖時發生錯誤: {str(e)}")
        raise


# 主程式
if __name__ == "__main__":
    file_list = ["/Users/chanchihyu/Sentinel_data/raw/2024/04/S5P_OFFL_L2__NO2____20240409T051555_20240409T065725_33622_03_020600_20240410T213619.nc"]

    for file in file_list:
        plot_global_no2(file)
