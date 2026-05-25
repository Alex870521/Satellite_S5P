"""src/processing/interpolators.py"""
from scipy.interpolate import griddata, Rbf
from scipy.ndimage import map_coordinates
from scipy.spatial import cKDTree
import numpy as np


class DataInterpolator:
    """數據插值器，支援多種插值方法"""

    @staticmethod
    def griddata_interpolation(lon, lat, data, lon_grid, lat_grid, max_distance=0.1):
        """使用 griddata 進行插值，支持低解析度到高解析度的重採樣
        當檢測到低解析度到高解析度重採樣時，自動使用最近鄰重採樣（像元複寫）

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

        # 估算原始數據的像元大小
        if len(valid_lon) > 1:
            temp_tree = cKDTree(np.column_stack((valid_lon.flatten(), valid_lat.flatten())))
            distances, _ = temp_tree.query(np.column_stack((valid_lon.flatten(), valid_lat.flatten())), k=2)
            pixel_size = np.median(distances[:, 1])  # 第二近鄰距離
        else:
            pixel_size = max_distance

        # 計算目標網格解析度
        if lon_grid.shape[1] > 1:
            grid_resolution_lon = abs(lon_grid[0, 1] - lon_grid[0, 0])
        else:
            grid_resolution_lon = 0.01
            
        if lat_grid.shape[0] > 1:
            grid_resolution_lat = abs(lat_grid[1, 0] - lat_grid[0, 0])
        else:
            grid_resolution_lat = 0.01

        # 判斷是否為低解析度到高解析度的重採樣
        is_upsampling = (pixel_size > grid_resolution_lon * 1.5) or (pixel_size > grid_resolution_lat * 1.5)
        
        if is_upsampling:
            # 低解析度到高解析度：使用最近鄰重採樣（像元複寫），以「格數」控制半徑
            interpolated_values = np.full_like(lon_grid, np.nan)

            # 使用經緯兩方向解析度的平均作為單一格距（近似）
            cell_deg = float((grid_resolution_lon + grid_resolution_lat) / 2.0)
            # 以 max_distance 換算要填補的格數（至少 1 格）
            cells_to_fill = max(1, int(np.ceil(float(max_distance) / cell_deg)))
            influence_radius = cells_to_fill * cell_deg

            # 為每個原始觀測點找到所有在影響範圍內的高解析度網格點
            for obs_lon, obs_lat, obs_value in zip(valid_lon.flatten(), valid_lat.flatten(), valid_data.flatten()):
                distances_to_obs = np.sqrt((lon_grid - obs_lon)**2 + (lat_grid - obs_lat)**2)
                influence_mask = distances_to_obs <= influence_radius
                interpolated_values[influence_mask] = obs_value

            return interpolated_values
        else:
            # 一般情況：使用傳統的griddata插值
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