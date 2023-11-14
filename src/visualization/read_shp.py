import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point, Polygon

taiwan_counties = gpd.read_file(Path(__file__).parents[2] / "data/shapefiles/taiwan/COUNTY_MOI_1090820.shp")
station = gpd.read_file(Path(__file__).parents[2] / "data/shapefiles/stations/空氣品質監測站位置圖_121_10704.shp")

geometry = [Point(xy) for xy in zip(station['TWD97Lon'], station['TWD97Lat'])]
geodata = gpd.GeoDataFrame(station, crs=ccrs.PlateCarree(), geometry=geometry)

# 创建地图
fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()})
ax.set_extent([119, 123, 21, 26], crs=ccrs.PlateCarree())

# 添加县市边界
ax.add_geometries(taiwan_counties['geometry'], crs=ccrs.PlateCarree(), edgecolor='black', facecolor='none')
ax.add_geometries(station['geometry'], crs=ccrs.PlateCarree(), edgecolor='Red', facecolor='none')
geodata.plot(ax=ax, color='red', markersize=5)

plt.show()
