import numpy as np


class GridFrame:
    def __init__(self, resolution=(5.5, 3.5), bounds=(118, 124, 20, 27)):
        """初始化網格框架，預設台灣區域，但可以用於任何區域

        Args:
            resolution: (x_km, y_km) 網格解析度，單位為公里
            bounds: (lon_min, lon_max, lat_min, lat_max) 區域範圍
        """
        self.km_resolution = resolution
        self.bounds = bounds
        self._create_grid()

    def _km_to_degrees(self, km, latitude=None):
        """將公里轉換為經緯度度數"""
        LAT_KM_PER_DEGREE = 111.32

        if latitude is not None:
            # 經度轉換需要考慮緯度
            EARTH_RADIUS = 6371
            if abs(latitude) > 89.9:
                return 0
            return km / (EARTH_RADIUS * np.cos(np.radians(latitude)) * 2 * np.pi / 360)
        else:
            # 緯度轉換
            return km / LAT_KM_PER_DEGREE

    def _create_grid(self):
        """創建網格"""
        lon_min, lon_max, lat_min, lat_max = self.bounds
        center_lat = (lat_max + lat_min) / 2

        # 計算解析度
        lon_res = self._km_to_degrees(self.km_resolution[0], center_lat)
        lat_res = self._km_to_degrees(self.km_resolution[1])

        # 創建網格點
        self.lon = np.arange(lon_min, lon_max + lon_res, lon_res)
        self.lat = np.arange(lat_min, lat_max + lat_res, lat_res)

    def get_grid(self, custom_bounds=None):
        """獲取網格

        Args:
            custom_bounds: 可選的自定義範圍 (lon_min, lon_max, lat_min, lat_max)

        Returns:
            網格的經緯度陣列
        """
        if custom_bounds:
            lon_min, lon_max, lat_min, lat_max = custom_bounds
            mask_lon = (self.lon >= lon_min) & (self.lon <= lon_max)
            mask_lat = (self.lat >= lat_min) & (self.lat <= lat_max)
            lon_subset = self.lon[mask_lon]
            lat_subset = self.lat[mask_lat]
            return np.meshgrid(lon_subset, lat_subset)
        return np.meshgrid(self.lon, self.lat)

    @property
    def container(self):
        """創建數據容器"""
        return np.zeros(shape=(len(self.lat), len(self.lon)))