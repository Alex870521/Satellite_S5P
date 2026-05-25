import matplotlib.pyplot as plt
import geopandas as gpd
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pathlib import Path
from shapely.ops import unary_union
from cartopy.feature import ShapelyFeature


def plot_taiwan_map(map_scale='Taiwan', fig=None, ax=None, counties_path=None, dpi=600):
    """
    繪製台灣地圖，使用遮罩避免海岸線與縣市邊界重疊

    參數:
    - map_scale: 字符串，'Taiwan' 或 'East_Asia'，設定地圖範圍
    - fig: matplotlib 圖形對象，如果為 None 則創建新圖形
    - ax: matplotlib 坐標軸對象，如果為 None 則創建新坐標軸
    - counties_path: 台灣縣市邊界 shapefile 路徑，如果為 None 則使用預設路徑
    - dpi: 圖形分辨率

    返回:
    - fig: matplotlib 圖形對象
    - ax: matplotlib 坐標軸對象
    """
    # 設置範圍
    if map_scale == 'Taiwan':
        extent = [119, 123, 21, 26]
    elif map_scale == 'East_Asia':
        extent = [105, 140, 15, 45]
    elif map_scale == 'Global':
        extent = None
    else:
        raise ValueError("map_scale 必須是 'Taiwan' or 'East_Asia' or 'global'")

    # 創建圖形和坐標軸（如果未提供）
    if fig is None:
        fig = plt.figure(figsize=(14, 10), dpi=dpi)
    if ax is None:
        ax = plt.axes(projection=ccrs.PlateCarree())

    # 設置地圖範圍
    ax.set_extent(extent, crs=ccrs.PlateCarree())

    # 設置路徑
    if counties_path is None:
        counties_path = Path(__file__).parents[2] / "data/shapefiles/taiwan/COUNTY_MOI_1090820.shp"

    # 添加背景地圖特徵
    ax.add_feature(cfeature.LAND.with_scale('10m'), linewidth=0.5, color='gray', alpha=0.3, zorder=0)
    ax.add_feature(cfeature.BORDERS.with_scale('10m'), linewidth=0.5, zorder=1)
    # ax.add_feature(cfeature.COASTLINE.with_scale('10m'), linewidth=0.5, zorder=1)

    try:
        # 讀取台灣縣市邊界
        counties_gdf = gpd.read_file(counties_path)

        # 創建台灣形狀的遮罩
        taiwan_shape = unary_union(counties_gdf['geometry'].tolist())

        # 擴大遮罩區域
        expanded_mask = taiwan_shape.buffer(0.05)

        # 創建遮罩特徵
        mask_feature = ShapelyFeature([expanded_mask], ccrs.PlateCarree(),
                                      edgecolor='none', facecolor='white', alpha=1)

        # 添加遮罩，覆蓋掉標準海岸線
        ax.add_feature(mask_feature, zorder=2)

        # 添加縣市邊界
        counties_feature = ShapelyFeature(counties_gdf['geometry'], ccrs.PlateCarree(),
                                          edgecolor=(0, 0, 0, 0.3), facecolor='gray', alpha=0.3, linewidth=0.5)
        ax.add_feature(counties_feature, zorder=4)

    except Exception as e:
        print(f"讀取或處理縣市邊界時發生錯誤: {e}")

    # 添加網格線
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    plt.tight_layout()

    return fig, ax


# 使用範例
if __name__ == "__main__":
    # 範例 1: 只繪製台灣
    fig1, ax1 = plot_taiwan_map(map_scale='Taiwan')
    fig1.show()

    # 範例 2: 繪製東亞範圍內的台灣
    fig2, ax2 = plot_taiwan_map(map_scale='East_Asia')
    fig2.show()

    # 範例 3: 在已有的 fig 和 ax 上繪製
    # fig3 = plt.figure(figsize=(16, 12), dpi=600)
    # ax3 = plt.axes(projection=ccrs.PlateCarree())
    # plot_taiwan_map(map_range='Taiwan', fig=fig3, ax=ax3)
    # fig3.show()