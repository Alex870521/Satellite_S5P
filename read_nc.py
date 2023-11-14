import numpy as np
import xarray as xr
import time
import os

from scipy.ndimage import map_coordinates
from scipy.spatial import cKDTree
from netCDF4 import Dataset
from functools import wraps
from pathlib import Path


def timer(func=None, *, print_args=False):
    """ 輸出函式耗時

    :param func:
    :param print_args:
    :return:
    """

    def decorator(_func):
        @wraps(_func)
        def wrapper(*args, **kwargs):
            st = time.perf_counter()
            result = _func(*args, **kwargs)
            print(f'{_func.__name__}' + 'time cost: {:.3f} seconds'.format(time.perf_counter() - st))
            return result

        return wrapper

    if func is None:
        return decorator

    else:
        return decorator(func)


class TaiwanFrame:
    def __init__(self, resolution=0.01, lat_Taiwan=(21, 26), lon_Taiwan=(119, 123)):
        self.lat = np.arange(lat_Taiwan[0], lat_Taiwan[1] + resolution, resolution)
        self.lon = np.arange(lon_Taiwan[0], lon_Taiwan[1] + resolution, resolution)

    def frame(self):
        return np.meshgrid(self.lon, self.lat)

    @property
    def container(self):
        return np.zeros(shape=(self.lat.size, self.lon.size))


@timer
def extract_data(dataset, *, mask_value=0.75, **kwargs):
    """ Extract data from the dataset based on a mask.

    :param dataset: (xarray.Dataset): The input dataset containing the data to be extracted.
    :param mask_value: (float): The threshold value for the mask (default is 0.75).
    :return: ndarray: three ndarray of longitude, latitude, and NO2
             This method returns three NumPy arrays: masked_lon, masked_lat, and masked_no2, which represent
             the longitude,latitude, and nitrogendioxide_tropospheric_column values after applying the specified mask.
    """

    # set condition
    mask_lon = ((dataset.longitude >= 118) & (dataset.longitude <= 124))
    mask_lat = ((dataset.latitude >= 20) & (dataset.latitude <= 27))
    masked_lon_lat_ds = dataset.where((mask_lon & mask_lat), drop=True)

    mask_qa = (masked_lon_lat_ds.qa_value >= mask_value)
    masked_ds = masked_lon_lat_ds.where(mask_qa)

    masked_lon = masked_ds.longitude[0].data
    masked_lat = masked_ds.latitude[0].data
    masked_no2 = masked_ds.nitrogendioxide_tropospheric_column[0].data

    return masked_lon, masked_lat, masked_no2


@timer
def interp_data(nc_lon, nc_lat, nc_no2, lon_coordinate, lat_coordinate):
    """ This method is used to interpolate data for the purpose of using the map_coordinates function.
        It utilizes a KD-tree to find the nearest neighbors in a 2D array and returns a 2D NO2 array.

    :param nc_lon: 2D array of longitudes from the netCDF dataset.
    :param nc_lat: 2D array of latitudes from the netCDF dataset.
    :param nc_no2: 2D array of nitrogendioxide_tropospheric_column values from the netCDF dataset.
    :param lon_coordinate: 2D array of target longitudes for interpolation.
    :param lat_coordinate: 2D array of target longitudes for interpolation.
    :return: This method returns a 2D NumPy array (no2_array) that represents interpolated
             nitrogendioxide_tropospheric_column values at the specified target longitudes and latitudes.
    """
    lon_flat = lon_coordinate.flatten()
    lat_flat = lat_coordinate.flatten()

    # 构建 KD 树
    tree = cKDTree(np.column_stack((nc_lon.flatten(), nc_lat.flatten())))

    # 使用 query 方法查找最近的点
    distances, indices = tree.query(np.column_stack((lon_flat, lat_flat)), k=1)

    x_index, y_index = np.unravel_index(indices, nc_lon.shape)

    interpolated_values = map_coordinates(nc_no2, [x_index, y_index], order=1, mode='nearest')

    return interpolated_values.reshape(lon_coordinate.shape)


if __name__ == '__main__':
    year = '2022' # 改時間即可使用

    base_folder = Path("E:/S5P_NO2/data")
    sorted_folders = sorted(list(base_folder.glob(f"{year}*")))
    store_folder = Path(__file__).parent / 'ncData'

    lon_coordinate, lat_coordinate = TaiwanFrame().frame()

    for folder_path in sorted_folders:
        month = folder_path.name
        nc_file_path = store_folder / f"NO2_{month}.nc"

        if nc_file_path.exists():
            print(f"File {nc_file_path} exists, skipping folder {month}")
            continue

        container = []

        for file_path in folder_path.glob("S5P_OFFL_L2__NO2*.nc"):
            print('Open: ' + file_path.name)
            dataset = xr.open_dataset(file_path, group='PRODUCT')

            try:
                extracted_result = extract_data(dataset)
                no2_array = interp_data(*extracted_result, lon_coordinate, lat_coordinate)
                container.append(no2_array)

            except RuntimeError:
                print("RuntimeError: NetCDF: HDF error.")
                nan_array = np.full(lon_coordinate.shape, np.nan)
                container.append(nan_array)

        no2_average = np.nanmean(container, axis=0)

        original_shape = no2_average.shape

        ds_time = np.array(np.datetime64(f'{month[:4]}-{month[4:]}', 'ns'))

        ds_result = xr.Dataset(
            {'nitrogendioxide_tropospheric_column': (['latitude', 'longitude'], no2_average.reshape(*original_shape))},
            coords={'latitude': lat_coordinate[:, 0],
                    'longitude': lon_coordinate[0]})

        ds_result = ds_result.expand_dims(time=[ds_time])

        ds_result.to_netcdf(nc_file_path)
        print(f"{folder_path} mission completed")
