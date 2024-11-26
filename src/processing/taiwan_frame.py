import numpy as np


class TaiwanFrame:
    def __init__(self, resolution=0.01, lat_Taiwan=(20, 27), lon_Taiwan=(118, 124)):
        self.lat = np.arange(lat_Taiwan[0], lat_Taiwan[1] + resolution, resolution)
        self.lon = np.arange(lon_Taiwan[0], lon_Taiwan[1] + resolution, resolution)

    def frame(self):
        return np.meshgrid(self.lon, self.lat)

    @property
    def container(self):
        return np.zeros(shape=(self.lat.size, self.lon.size))