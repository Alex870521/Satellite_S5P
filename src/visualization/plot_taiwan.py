import matplotlib.pyplot as plt
import geopandas as gpd
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pathlib import Path
from shapely.ops import unary_union
from cartopy.feature import ShapelyFeature

from src.config.settings import FIGURE_DPI


# map_scale 的預設範圍 [lon_min, lon_max, lat_min, lat_max]。
# ⚠️ 以前 plot_taiwan_power_plant.py 另有一份同名同簽名的函式,同樣的 'Taiwan' 卻畫
#    中北部(台中電廠周邊);2026-10 合併後那個範圍改叫 'Central','Taiwan' 一律是全島。
EXTENTS = {
    'Taiwan': [119, 123, 21, 26],
    'Central': [120, 121.5, 23.4, 25],
    'East_Asia': [105, 140, 15, 45],
    'Global': None,
}


def plot_taiwan_map(map_scale='Taiwan', fig=None, ax=None, counties_path=None, dpi=FIGURE_DPI,
                    *, extent=None, land_color='gray', figsize=(14, 10), tight_layout=True):
    """
    繪製台灣地圖，使用遮罩避免海岸線與縣市邊界重疊

    參數:
    - map_scale: 'Taiwan'(全島)/ 'Central'(中北部,電廠圖用)/ 'East_Asia' / 'Global'
    - fig, ax: 既有的 figure / GeoAxes;None 則新建
    - counties_path: 台灣縣市邊界 shapefile;None 用 repo 內預設
    - dpi: 新建 figure 時的解析度
    - extent: 直接給 [lon_min, lon_max, lat_min, lat_max],優先於 map_scale
    - land_color: 陸地與縣市填色('gray';電廠圖用 'lightgray')
    - figsize: 新建 figure 時的尺寸
    - tight_layout: 畫完是否呼叫 plt.tight_layout()

    返回: (fig, ax)
    """
    if extent is None:
        if map_scale not in EXTENTS:
            raise ValueError(f"map_scale 必須是 {list(EXTENTS)} 之一,得到 {map_scale!r}")
        extent = EXTENTS[map_scale]

    if fig is None:
        fig = plt.figure(figsize=figsize, dpi=dpi)
    if ax is None:
        ax = plt.axes(projection=ccrs.PlateCarree())

    if extent is None:
        ax.set_global()                       # 'Global':以前 set_extent(None) 會直接報錯
    else:
        ax.set_extent(extent, crs=ccrs.PlateCarree())

    if counties_path is None:
        counties_path = Path(__file__).parents[2] / "data/shapefiles/taiwan/COUNTY_MOI_1090820.shp"

    ax.add_feature(cfeature.LAND.with_scale('10m'), linewidth=0.5, color=land_color, alpha=0.3, zorder=0)
    ax.add_feature(cfeature.BORDERS.with_scale('10m'), linewidth=0.5, zorder=1)

    try:
        counties_gdf = gpd.read_file(counties_path)
        # 台灣形狀外擴一點當白色遮罩,蓋掉標準海岸線,避免和縣市邊界雙線重疊
        expanded_mask = unary_union(counties_gdf['geometry'].tolist()).buffer(0.05)
        ax.add_feature(ShapelyFeature([expanded_mask], ccrs.PlateCarree(),
                                      edgecolor='none', facecolor='white', alpha=1), zorder=2)
        ax.add_feature(ShapelyFeature(counties_gdf['geometry'], ccrs.PlateCarree(),
                                      edgecolor=(0, 0, 0, 0.3), facecolor=land_color,
                                      alpha=0.3, linewidth=0.5), zorder=4)
    except Exception as e:
        # 讀不到縣市邊界時退回標準海岸線(原本只有電廠版這麼做,全島版會畫出沒有海岸線的圖)
        print(f"讀取或處理縣市邊界時發生錯誤: {e};改用標準海岸線")
        ax.add_feature(cfeature.COASTLINE.with_scale('10m'), linewidth=0.5, zorder=1)

    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    if tight_layout:
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