import matplotlib.pyplot as plt
import geopandas as gpd
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pathlib import Path
from shapely.ops import unary_union
from cartopy.feature import ShapelyFeature
import pandas as pd
from matplotlib.patches import Circle
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle
import os


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
        extent = [120, 121.5, 23.4, 25]
    elif map_scale == 'East_Asia':
        extent = [105, 140, 15, 45]
    elif map_scale == 'Global':
        extent = None
    else:
        raise ValueError("map_scale 必須是 'Taiwan' or 'East_Asia' or 'global'")

    # 創建圖形和坐標軸（如果未提供）
    if fig is None:
        fig = plt.figure(figsize=(10, 10), dpi=dpi)
    if ax is None:
        ax = plt.axes(projection=ccrs.PlateCarree())

    # 設置地圖範圍
    ax.set_extent(extent, crs=ccrs.PlateCarree())

    # 設置路徑
    if counties_path is None:
        counties_path = Path(__file__).parents[2] / "data/shapefiles/taiwan/COUNTY_MOI_1090820.shp"

    # 添加背景地圖特徵
    ax.add_feature(cfeature.LAND.with_scale('10m'), linewidth=0.5, color='lightgray', alpha=0.3, zorder=0)
    ax.add_feature(cfeature.BORDERS.with_scale('10m'), linewidth=0.5, zorder=1)

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
                                          edgecolor=(0, 0, 0, 0.3), facecolor='lightgray', alpha=0.3, linewidth=0.5)
        ax.add_feature(counties_feature, zorder=4)

    except Exception as e:
        print(f"讀取或處理縣市邊界時發生錯誤: {e}")
        print("將繼續使用標準海岸線...")
        ax.add_feature(cfeature.COASTLINE.with_scale('10m'), linewidth=0.5, zorder=1)

    # 添加網格線
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    return fig, ax


def load_station_data(env_file_path, taipower_file_path):
    """
    從檔案讀取測站資料

    參數:
    - env_file_path: 環境部測站 CSV 檔案路徑
    - taipower_file_path: 台電測站 CSV 檔案路徑

    返回:
    - DataFrame: 合併後的測站資料
    """
    stations_list = []

    # 讀取環境部測站 CSV
    try:
        env_df = pd.read_csv(env_file_path)
        # 轉換欄位名稱以符合統一格式
        env_df_formatted = pd.DataFrame({
            '測站歸屬': '環境部',
            '測站名稱/編號': env_df['sitename'],
            '空品區': env_df['areaname'],
            '縣市': env_df['county'],
            '市/區/鄉/鎮': env_df['township'],
            '經度': env_df['twd97lon'],
            '緯度': env_df['twd97lat'],
            '測站類型': env_df['sitetype']
        })
        stations_list.append(env_df_formatted)
        print(f"成功讀取 {len(env_df_formatted)} 筆環境部測站資料")
    except Exception as e:
        print(f"讀取環境部測站檔案時發生錯誤: {e}")

    # 讀取台電測站 CSV（嘗試多種編碼）
    encodings = ['utf-8', 'big5', 'cp950', 'gb2312', 'gbk', 'latin1']
    taipower_df = None

    for encoding in encodings:
        try:
            taipower_df = pd.read_csv(taipower_file_path, encoding=encoding)
            print(f"成功使用 {encoding} 編碼讀取台電測站檔案")
            break
        except (UnicodeDecodeError, Exception) as e:
            continue

    if taipower_df is not None:
        # 檢查是否需要轉換欄位（如果是舊的 Excel 格式轉成 CSV）
        if '測站歸屬' in taipower_df.columns:
            stations_list.append(taipower_df)
        else:
            # 如果欄位名稱不同，可能需要轉換
            print(f"台電測站欄位: {taipower_df.columns.tolist()}")
            stations_list.append(taipower_df)
        print(f"成功讀取 {len(taipower_df)} 筆台電測站資料")
    else:
        print(f"讀取台電測站檔案時發生錯誤: 無法使用任何編碼讀取檔案")

    # 合併資料
    if len(stations_list) > 0:
        combined_df = pd.concat(stations_list, ignore_index=True)
        print(f"合併後總測站數: {len(combined_df)}")
        return combined_df
    else:
        print("無法讀取任何測站資料")
        return None


def get_power_plant_data():
    """
    返回台灣火力發電廠位置資料

    返回:
    - DataFrame: 發電廠資料
    """
    power_plants = [
        # 台電火力電廠
        {'name': '林口', 'lat': 25.08, 'lon': 121.28, 'type': '台電火力'},
        {'name': '大潭', 'lat': 25.04, 'lon': 121.03, 'type': '台電火力'},
        {'name': '通霄', 'lat': 24.48, 'lon': 120.70, 'type': '台電火力'},
        {'name': '台中', 'lat': 24.21, 'lon': 120.48, 'type': '台電火力'},
        {'name': '興達', 'lat': 22.87, 'lon': 120.26, 'type': '台電火力'},
        {'name': '大林', 'lat': 22.55, 'lon': 120.40, 'type': '台電火力'},
        {'name': '協和', 'lat': 25.13, 'lon': 121.74, 'type': '台電火力'},
        {'name': '南部', 'lat': 22.91, 'lon': 120.20, 'type': '台電火力'},
        # 民營火力電廠
        {'name': '和平', 'lat': 24.30, 'lon': 121.74, 'type': '民營火力'},
        {'name': '新桃', 'lat': 24.95, 'lon': 121.18, 'type': '民營火力'},
        {'name': '星元', 'lat': 24.18, 'lon': 120.42, 'type': '民營火力'},
        {'name': '星能', 'lat': 24.15, 'lon': 120.43, 'type': '民營火力'},
        {'name': '嘉惠', 'lat': 23.47, 'lon': 120.27, 'type': '民營火力'},
        {'name': '豐德', 'lat': 22.67, 'lon': 120.45, 'type': '民營火力'},
        # 離島
        {'name': '塔山', 'lat': 23.54, 'lon': 119.59, 'type': '台電火力'},
        {'name': '尖山', 'lat': 26.18, 'lon': 119.97, 'type': '台電火力'},
    ]
    return pd.DataFrame(power_plants)


def plot_stations_and_plants(env_station_file, taipower_station_file, counties_path=None,
                             output_file='taiwan_stations_plants.png'):
    """
    繪製測站和發電廠分布圖

    參數:
    - env_station_file: 環境部測站 CSV 檔案路徑
    - taipower_station_file: 台電測站 Excel 檔案路徑
    - counties_path: 縣市邊界 shapefile 路徑
    - output_file: 輸出圖片檔案名稱
    """
    # 設置中文字體（標楷體）和英文字體（Times New Roman）
    plt.rcParams['font.sans-serif'] = ['DFKai-SB', 'BiauKai', '標楷體', 'Arial Unicode MS']
    plt.rcParams['font.serif'] = ['Times New Roman', 'DFKai-SB', 'BiauKai', '標楷體']
    plt.rcParams['axes.unicode_minus'] = False
    # 不強制使用 serif，讓中文自動使用 sans-serif 字體
    # plt.rcParams['font.family'] = 'serif'
    plt.rcParams['font.size'] = 16

    # 讀取測站資料
    stations_df = load_station_data(env_station_file, taipower_station_file)
    if stations_df is None:
        return

    # 讀取發電廠資料
    plants_df = get_power_plant_data()

    # 定義地圖範圍
    lon_min, lon_max = 120, 121.5
    lat_min, lat_max = 23.4, 25

    # 過濾測站：只保留在地圖範圍內的
    stations_df = stations_df[
        (stations_df['經度'] >= lon_min) & (stations_df['經度'] <= lon_max) &
        (stations_df['緯度'] >= lat_min) & (stations_df['緯度'] <= lat_max)
        ]
    print(f"地圖範圍內的測站數: {len(stations_df)}")

    # 過濾發電廠：只保留在地圖範圍內的
    plants_df = plants_df[
        (plants_df['lon'] >= lon_min) & (plants_df['lon'] <= lon_max) &
        (plants_df['lat'] >= lat_min) & (plants_df['lat'] <= lat_max)
        ]
    print(f"地圖範圍內的電廠數: {len(plants_df)}")

    # 創建地圖
    fig, ax = plot_taiwan_map(map_scale='Taiwan', counties_path=counties_path, dpi=300)

    # 如果有縣市邊界資料，繪製空品區
    if counties_path is not None and Path(counties_path).exists():
        try:
            counties_gdf = gpd.read_file(counties_path)

            # 竹苗空品區：新竹市、新竹縣、苗栗縣
            zhumiao_counties = ['新竹市', '新竹縣', '苗栗縣']
            zhumiao_geom = counties_gdf[counties_gdf['COUNTYNAME'].isin(zhumiao_counties)]
            if len(zhumiao_geom) > 0:
                zhumiao_union = unary_union(zhumiao_geom.geometry)
                zhumiao_feature = ShapelyFeature([zhumiao_union], ccrs.PlateCarree(),
                                                 edgecolor='#10B981', facecolor='#10B981',
                                                 alpha=0.15, linewidth=2.5, linestyle='--')
                ax.add_feature(zhumiao_feature, zorder=3)

                # 計算竹苗空品區的中心點（用於放置標籤）
                zhumiao_centroid = zhumiao_union.centroid
                ax.text(zhumiao_centroid.x, zhumiao_centroid.y, '竹苗空品區',
                        fontsize=14, weight='bold', color='#059669', ha='center',
                        bbox=dict(boxstyle='round,pad=0.5', facecolor='white',
                                  edgecolor='#10B981', alpha=0.9, linewidth=2),
                        transform=ccrs.PlateCarree(), zorder=9)

            # 中部空品區：臺中市、彰化縣、南投縣
            central_counties = ['臺中市', '彰化縣', '南投縣']
            central_geom = counties_gdf[counties_gdf['COUNTYNAME'].isin(central_counties)]
            if len(central_geom) > 0:
                central_union = unary_union(central_geom.geometry)
                central_feature = ShapelyFeature([central_union], ccrs.PlateCarree(),
                                                 edgecolor='#8B5CF6', facecolor='#8B5CF6',
                                                 alpha=0.15, linewidth=2.5, linestyle='--')
                ax.add_feature(central_feature, zorder=3)

                # 計算中部空品區的中心點（用於放置標籤）
                central_centroid = central_union.centroid
                ax.text(central_centroid.x, central_centroid.y, '中部空品區',
                        fontsize=14, weight='bold', color='#7C3AED', ha='center',
                        bbox=dict(boxstyle='round,pad=0.5', facecolor='white',
                                  edgecolor='#8B5CF6', alpha=0.9, linewidth=2),
                        transform=ccrs.PlateCarree(), zorder=9)

            print("已繪製空品區邊界")
        except Exception as e:
            print(f"繪製空品區時發生錯誤: {e}")
            print("將使用簡化的矩形區域...")
            # 如果讀取失敗，使用原來的矩形方式
            zhumiao_bounds = Rectangle((120.7, 24.3), 0.9, 0.9,
                                       linewidth=2, edgecolor='#10B981', facecolor='#10B981',
                                       alpha=0.15, linestyle='--',
                                       transform=ccrs.PlateCarree(), zorder=3)
            ax.add_patch(zhumiao_bounds)

            central_bounds = Rectangle((120.4, 23.5), 1.0, 1.1,
                                       linewidth=2, edgecolor='#8B5CF6', facecolor='#8B5CF6',
                                       alpha=0.15, linestyle='--',
                                       transform=ccrs.PlateCarree(), zorder=3)
            ax.add_patch(central_bounds)

            ax.text(121.15, 24.9, '竹苗空品區', fontsize=14, weight='bold',
                    color='#059669', ha='center',
                    bbox=dict(boxstyle='round,pad=0.5', facecolor='white',
                              edgecolor='#10B981', alpha=0.8, linewidth=2),
                    transform=ccrs.PlateCarree(), zorder=9)

            ax.text(120.9, 24.3, '中部空品區', fontsize=14, weight='bold',
                    color='#7C3AED', ha='center',
                    bbox=dict(boxstyle='round,pad=0.5', facecolor='white',
                              edgecolor='#8B5CF6', alpha=0.8, linewidth=2),
                    transform=ccrs.PlateCarree(), zorder=9)
    else:
        print("未提供縣市邊界檔案，將使用簡化的矩形區域...")
        # 使用簡化的矩形方式
        zhumiao_bounds = Rectangle((120.7, 24.3), 0.9, 0.9,
                                   linewidth=2, edgecolor='#10B981', facecolor='#10B981',
                                   alpha=0.15, linestyle='--',
                                   transform=ccrs.PlateCarree(), zorder=3)
        ax.add_patch(zhumiao_bounds)

        central_bounds = Rectangle((120.4, 23.5), 1.0, 1.1,
                                   linewidth=2, edgecolor='#8B5CF6', facecolor='#8B5CF6',
                                   alpha=0.15, linestyle='--',
                                   transform=ccrs.PlateCarree(), zorder=3)
        ax.add_patch(central_bounds)

        ax.text(121.15, 24.9, '竹苗空品區', fontsize=14, weight='bold',
                color='#059669', ha='center',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='white',
                          edgecolor='#10B981', alpha=0.8, linewidth=2),
                transform=ccrs.PlateCarree(), zorder=9)

        ax.text(120.9, 24.3, '中部空品區', fontsize=14, weight='bold',
                color='#7C3AED', ha='center',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='white',
                          edgecolor='#8B5CF6', alpha=0.8, linewidth=2),
                transform=ccrs.PlateCarree(), zorder=9)

    # 繪製測站點
    # 環境部測站
    env_stations = stations_df[stations_df['測站歸屬'] == '環境部']
    if len(env_stations) > 0:
        ax.scatter(env_stations['經度'], env_stations['緯度'],
                   c='#3B82F6', s=50, alpha=0.8, edgecolors='white', linewidth=1.5,
                   transform=ccrs.PlateCarree(), zorder=6, label='環境部測站')

    # 台電測站
    taipower_stations = stations_df[stations_df['測站歸屬'] == '台電']
    if len(taipower_stations) > 0:
        ax.scatter(taipower_stations['經度'], taipower_stations['緯度'],
                   c='#F97316', s=50, alpha=0.8, edgecolors='white', linewidth=1.5,
                   transform=ccrs.PlateCarree(), zorder=6, label='台電測站')

    # 繪製發電廠（模仿原圖樣式：大圓圈，半透明）
    # 台電火力電廠
    taipower_plants = plants_df[plants_df['type'] == '台電火力']
    ax.scatter(taipower_plants['lon'], taipower_plants['lat'],
               c='#FF9999', s=800, alpha=0.6, edgecolors='none',
               marker='o', transform=ccrs.PlateCarree(), zorder=5, label='台電火力電廠')

    # 民營火力電廠（較小的圓圈）
    private_plants = plants_df[plants_df['type'] == '民營火力']
    ax.scatter(private_plants['lon'], private_plants['lat'],
               c='#8B6F47', s=100, alpha=0.6, edgecolors='none',
               marker='o', transform=ccrs.PlateCarree(), zorder=5, label='民營火力電廠')

    # 標註發電廠名稱（黑色文字，簡潔樣式）
    for idx, plant in plants_df.iterrows():
        # 根據電廠名稱調整標籤位置和對齊方式
        if plant['name'] == '台中':
            # 台中電廠標籤放上面
            offset_x = 0
            offset_y = 0.08
            ha = 'center'
            va = 'bottom'
        elif plant['name'] == '星元':
            # 星元電廠標籤放左邊
            offset_x = -0.04
            offset_y = 0
            ha = 'right'
            va = 'center'
        elif plant['name'] == '星能':
            # 星能電廠標籤放下面
            offset_x = 0
            offset_y = -0.04
            ha = 'center'
            va = 'top'
        else:
            # 其他電廠標籤放下面（原設定）
            if plant['type'] == '台電火力':
                offset_x = 0
                offset_y = -0.08
            else:
                offset_x = 0
                offset_y = -0.04
            ha = 'center'
            va = 'top'

        ax.text(plant['lon'] + offset_x, plant['lat'] + offset_y, plant['name'],
                fontsize=13, ha=ha, va=va, weight='bold',
                color='black',
                transform=ccrs.PlateCarree(), zorder=8)

    # 添加標題和圖例
    ax.set_title('台灣空氣品質測站與火力發電廠分布圖', fontsize=20, weight='bold', pad=20)

    # 創建圖例
    legend_elements = [
        Line2D([0], [0], marker='o', color='w', markerfacecolor='#3B82F6',
               markersize=12, label=f'環境部測站 ({len(env_stations)}站)', markeredgecolor='white',
               markeredgewidth=1.5),
        Line2D([0], [0], marker='o', color='w', markerfacecolor='#F97316',
               markersize=12, label=f'台電測站 ({len(taipower_stations)}站)', markeredgecolor='white',
               markeredgewidth=1.5),
        Line2D([0], [0], marker='o', color='w', markerfacecolor='#FF9999',
               markersize=16, label=f'台電火力電廠 ({len(taipower_plants)}廠)', alpha=0.6),
        Line2D([0], [0], marker='o', color='w', markerfacecolor='#8B6F47',
               markersize=16, label=f'民營火力電廠 ({len(private_plants)}廠)', alpha=0.6),
        Line2D([0], [0], color='#10B981', linewidth=2.5, linestyle='--',
               label='竹苗空品區'),
        Line2D([0], [0], color='#8B5CF6', linewidth=2.5, linestyle='--',
               label='中部空品區'),
    ]

    ax.legend(handles=legend_elements, loc='lower right', fontsize=13,
              frameon=True, fancybox=True, shadow=True, framealpha=0.95)

    # 添加統計資訊
    # total_stations = len(stations_df)
    # info_text = f'總測站數: {total_stations}\n總電廠數: {len(plants_df)}'
    # ax.text(0.02, 0.02, info_text, transform=ax.transAxes,
    #         fontsize=12, verticalalignment='bottom',
    #         bbox=dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor='gray', linewidth=1.5))

    plt.tight_layout()

    # 儲存圖片
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"圖片已儲存至: {output_file}")

    # 顯示圖片
    plt.show()

    return fig, ax


# 使用範例
if __name__ == "__main__":
    # 報告圖組輸入資料夾（依個人環境調整）
    desktop = Path.home() / "NTU/2024_台電計畫/計畫報告圖組/台灣火力電廠分布與空品測站分布"

    # 檔案名稱
    env_station_file = "環境部空品測站基本資料.csv"
    taipower_station_file = "台電空品測站基本資料.csv"

    # 完整檔案路徑
    env_file_path = desktop / env_station_file
    taipower_file_path = desktop / taipower_station_file

    # 縣市邊界 shapefile 路徑（repo 內相對路徑）
    counties_shapefile = (
        Path(__file__).parents[2] / "data/shapefiles/Taiwan/COUNTY_MOI_1090820.shp")

    # 檢查檔案是否存在
    missing_files = []
    if not env_file_path.exists():
        missing_files.append(env_station_file)
    if not taipower_file_path.exists():
        missing_files.append(taipower_station_file)

    if missing_files:
        print(f"錯誤: 找不到以下檔案:")
        for file in missing_files:
            print(f"  - {file}")
        print(f"請確認檔案是否在桌面上")
    else:
        # 繪製地圖
        plot_stations_and_plants(
            env_station_file=env_file_path,
            taipower_station_file=taipower_file_path,
            counties_path=counties_shapefile if counties_shapefile.exists() else None,
            output_file=desktop / 'taiwan_stations_plants.png'
        )

        if not counties_shapefile.exists():
            print(f"提示: 找不到縣市邊界檔案 {counties_shapefile}")
            print("將使用簡化的空品區邊界")