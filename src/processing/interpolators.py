"""src/processing/interpolators.py"""
from scipy.interpolate import griddata, Rbf
from scipy.ndimage import map_coordinates
from scipy.spatial import cKDTree
import numpy as np


class DataInterpolator:
    """數據插值器，支援多種插值方法"""

    @staticmethod
    def griddata_interpolation(lon, lat, data, lon_grid, lat_grid, max_distance=0.1):
        """使用 griddata 進行插值，只填充距離較近的網格點

       Parameters:
       -----------
       lon, lat : ndarray
           原始經緯度數據
       data : ndarray
           要插值的數據
       lon_grid, lat_grid : ndarray
           目標網格的經緯度
       max_distance : float
           最大插值距離（單位：度），超過此距離的網格點不進行插值
       """
        # 移除無效值（NaN）
        valid_mask = ~np.isnan(lon) & ~np.isnan(lat) & ~np.isnan(data)
        valid_lon = lon[valid_mask]
        valid_lat = lat[valid_mask]
        valid_data = data[valid_mask]

        if len(valid_data) == 0:
            return np.full_like(lon_grid, np.nan)

        points = np.column_stack((valid_lon.flatten(), valid_lat.flatten()))
        values = valid_data.flatten()

        # 建立 KDTree 用於距離檢查
        tree = cKDTree(points)

        # 將網格點轉換為適合 griddata 的格式
        grid_points = np.column_stack((lon_grid.flatten(), lat_grid.flatten()))

        # 查找每個網格點最近的原始數據點的距離
        distances, _ = tree.query(grid_points, k=1)

        # 創建遮罩，只對距離在閾值內的點進行插值
        mask = distances <= max_distance

        # 初始化結果數組為 NaN
        grid_values = np.full(grid_points.shape[0], np.nan)

        # 只對符合距離條件的點進行插值
        if np.any(mask):
            grid_values[mask] = griddata(points,
                                         values,
                                         grid_points[mask],
                                         method='linear')

        return grid_values.reshape(lon_grid.shape)

    @staticmethod
    def kdtree_interpolation(lon, lat, data, lon_grid, lat_grid, max_distance=0.1):
        """使用 KDTree 進行插值，只填充距離較近的網格點"""
        # 移除無效值（NaN）
        valid_mask = ~np.isnan(lon) & ~np.isnan(lat) & ~np.isnan(data)
        valid_lon = lon[valid_mask]
        valid_lat = lat[valid_mask]
        valid_data = data[valid_mask]

        if len(valid_data) == 0:
            return np.full_like(lon_grid, np.nan)

        # 建立 KD 樹
        tree = cKDTree(np.column_stack((valid_lon.flatten(), valid_lat.flatten())))

        # 將網格點轉換為適合查詢的格式
        grid_points = np.column_stack((lon_grid.flatten(), lat_grid.flatten()))

        # 使用 query 方法查找最近的點和距離
        distances, indices = tree.query(grid_points, k=1)

        # 創建遮罩，只對距離在閾值內的點進行插值
        mask = distances <= max_distance

        # 初始化結果數組為 NaN
        interpolated_values = np.full_like(lon_grid.flatten(), np.nan)

        # 只對符合距離條件的點進行插值
        if np.any(mask):
            valid_data_flat = valid_data.flatten()
            interpolated_values[mask] = valid_data_flat[indices[mask]]

        return interpolated_values.reshape(lon_grid.shape)

    @staticmethod
    def rbf_interpolation(lon, lat, data, lon_grid, lat_grid, max_distance=0.1, function='thin_plate'):
        """使用徑向基函數 (RBF) 進行插值，只填充距離較近的網格點

        Parameters:
        -----------
        lon, lat : ndarray
            原始經緯度數據
        data : ndarray
            要插值的數據
        lon_grid, lat_grid : ndarray
            目標網格的經緯度
        max_distance : float
            最大插值距離（單位：度），超過此距離的網格點不進行插值
        function : str
            RBF 函數類型，可選：'multiquadric', 'inverse', 'gaussian',
            'linear', 'cubic', 'quintic', 'thin_plate'
        """
        # 移除無效值（NaN）
        valid_mask = ~np.isnan(lon) & ~np.isnan(lat) & ~np.isnan(data)
        valid_lon = lon[valid_mask]
        valid_lat = lat[valid_mask]
        valid_data = data[valid_mask]

        if len(valid_data) == 0:
            return np.full_like(lon_grid, np.nan)

        # 為了提高效率，如果數據點過多，隨機選擇一部分點
        max_points = 5000  # 最大使用點數，可根據需要調整
        if len(valid_data) > max_points:
            idx = np.random.choice(len(valid_data), max_points, replace=False)
            valid_lon = valid_lon.flatten()[idx]
            valid_lat = valid_lat.flatten()[idx]
            valid_data = valid_data.flatten()[idx]
        else:
            valid_lon = valid_lon.flatten()
            valid_lat = valid_lat.flatten()
            valid_data = valid_data.flatten()

        # 建立 KDTree 用於距離檢查
        points = np.column_stack((valid_lon, valid_lat))
        tree = cKDTree(points)

        # 將網格點轉換為適合查詢的格式
        grid_lon = lon_grid.flatten()
        grid_lat = lat_grid.flatten()
        grid_points = np.column_stack((grid_lon, grid_lat))

        # 查找每個網格點最近的原始數據點的距離
        distances, _ = tree.query(grid_points, k=1)

        # 創建遮罩，只對距離在閾值內的點進行插值
        mask = distances <= max_distance

        # 初始化結果數組為 NaN
        interpolated_values = np.full(grid_points.shape[0], np.nan)

        # 只對符合距離條件的點進行插值
        if np.any(mask):
            # 創建 RBF 插值器
            rbf = Rbf(valid_lon, valid_lat, valid_data, function=function)

            # 對符合距離條件的點進行插值
            interpolated_values[mask] = rbf(grid_lon[mask], grid_lat[mask])

        return interpolated_values.reshape(lon_grid.shape)

    @classmethod
    def interpolate(cls, lon, lat, data, lon_grid, lat_grid, method='griddata', max_distance=0.1, rbf_function='thin_plate'):
        """統一的插值介面

       Parameters:
       -----------
       lon, lat : ndarray
           原始經緯度數據
       data : ndarray
           要插值的數據
       lon_grid, lat_grid : ndarray
           目標網格的經緯度
       method : str
           插值方法，可選 'griddata', 'kdtree' 或 'rbf'
       max_distance : float
           最大插值距離（單位：度），超過此距離的網格點不進行插值
       rbf_function : str
           當 method='rbf' 時使用的 RBF 函數類型

       Returns:
       --------
       ndarray
           插值後的數據，距離過遠的點將為 NaN
       """
        if method == 'griddata':
            return cls.griddata_interpolation(lon, lat, data, lon_grid, lat_grid, max_distance)
        elif method == 'kdtree':
            return cls.kdtree_interpolation(lon, lat, data, lon_grid, lat_grid, max_distance)
        elif method == 'rbf':
            return cls.rbf_interpolation(lon, lat, data, lon_grid, lat_grid, max_distance, rbf_function)
        else:
            raise ValueError(f"Unsupported interpolation method: {method}")