"""src/processing/interpolators.py"""
from scipy.interpolate import griddata
from scipy.ndimage import map_coordinates
from scipy.spatial import cKDTree
import numpy as np


class DataInterpolator:
    """數據插值器，支援多種插值方法"""

    @staticmethod
    def griddata_interpolation(lon, lat, data, lon_grid, lat_grid):
        """使用 griddata 進行插值"""
        points = np.column_stack((lon.flatten(), lat.flatten()))
        values = data.flatten()

        # 移除無效值
        valid = ~np.isnan(values)
        points = points[valid]
        values = values[valid]

        # 將網格點轉換為適合 griddata 的格式
        grid_points = np.column_stack((lon_grid.flatten(), lat_grid.flatten()))

        # 進行插值
        grid_values = griddata(points, values, grid_points, method='linear')
        return grid_values.reshape(lon_grid.shape)

    @staticmethod
    def kdtree_interpolation(lon, lat, data, lon_grid, lat_grid):
        """使用 KDTree 進行插值"""
        lon_flat = lon_grid.flatten()
        lat_flat = lat_grid.flatten()

        # 构建 KD 树
        tree = cKDTree(np.column_stack((lon.flatten(), lat.flatten())))

        # 使用 query 方法查找最近的点
        distances, indices = tree.query(np.column_stack((lon_flat, lat_flat)), k=1)

        x_index, y_index = np.unravel_index(indices, lon.shape)
        interpolated_values = map_coordinates(data, [x_index, y_index], order=1, mode='nearest')

        return interpolated_values.reshape(lon_grid.shape)

    @classmethod
    def interpolate(cls, lon, lat, data, lon_grid, lat_grid, method='griddata'):
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
            插值方法，可選 'griddata' 或 'kdtree'

        Returns:
        --------
        ndarray
            插值後的數據
        """
        if method == 'griddata':
            return cls.griddata_interpolation(lon, lat, data, lon_grid, lat_grid)
        elif method == 'kdtree':
            return cls.kdtree_interpolation(lon, lat, data, lon_grid, lat_grid)
        else:
            raise ValueError(f"Unsupported interpolation method: {method}")
