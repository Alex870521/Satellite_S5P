from dataclasses import dataclass
from typing import Literal, TypeAlias
from enum import Enum


ClassInput = Literal['NRTI', 'OFFL', 'RPRO']
TypeInput = Literal['O3____', 'O3_TCL', 'O3__PR', 'CH4___', 'CO____', 'NO2___', 'HCHO__', 'SO2___', 'CLOUD_', 'FRESCO', 'AER_LH', 'AER_AI']


@dataclass
class ProductConfig:
    """產品配置"""
    display_name: str      # 顯示名稱（帶下標）
    dataset_name: str      # 數據集名稱
    vmin: float | None     # 最小值
    vmax: float | None     # 最大值
    units: str             # 單位
    title: str             # 標題
    cmap: str = 'viridis'  # 預設色階


class ProductType(str, Enum):
    """Available product types"""
    # Ozone products
    O3____ = 'O3____'
    O3_TCL = 'O3_TCL'  # No data ??
    O3__PR = 'O3__PR'  # start from 2022 have total_vertical_columns and tropospheric_column and ozone_profile

    # Other gas products
    CH4___ = 'CH4___'
    CO____ = 'CO____'
    NO2___ = 'NO2___'
    HCHO__ = 'HCHO__'
    SO2___ = 'SO2___'

    # Cloud and aerosol products
    CLOUD_ = 'CLOUD_'
    FRESCO = 'FRESCO'
    AER_LH = 'AER_LH'
    AER_AI = 'AER_AI'


class ProductLevel(str, Enum):
    Level0 = 'L0__'
    Level1B = 'L1B_'
    Level2 = 'L2__'


class ProductClass(str, Enum):
    """可用的處理類型

    Available classes:
        Main processing types:
            - NRTI : Near-real time processing (最快但精確度較低)
            - OFFL : Offline processing (標準處理)
            - RPRO : Reprocessing (重新處理的歷史數據)

        Testing types:
            - TEST : Internal testing
            - OGCA : On-ground calibration
            - GSOV : Ground segment validation
            - OPER : Operational processing
    """
    # Main processing types
    NRTI = 'NRTI'
    OFFL = 'OFFL'
    RPRO = 'RPRO'

    # Testing types
    TEST = 'TEST'
    OGCA = 'OGCA'
    GSOV = 'GSOV'
    OPER = 'OPER'


PRODUCT_CONFIGS: dict[str, ProductConfig] = {
    'O3____': ProductConfig(
        display_name='O\u2083',
        dataset_name='ozone_total_vertical_column',
        vmin=0.1, vmax=0.2,
        units=f'O$_3$ Total Vertical Column (mol/m$^2$)',
        title=f'O$_3$ Total Vertical Column',
        cmap='RdBu_r'
    ),
    'O3_TCL': ProductConfig(
        display_name='O\u2083_TCL',
        dataset_name='ozone_tropospheric_column',
        vmin=0.1, vmax=0.2,
        units=f'O$_3$ Tropospheric Column (mol/m$^2$)',
        title=f'O$_3$ Tropospheric Column',
        cmap='RdBu_r'
    ),
    'O3__PR': ProductConfig(
        display_name='O\u2083_PR',
        dataset_name='ozone_tropospheric_column',
        vmin=0.01, vmax=0.04,
        units=f'O$_3$ Tropospheric Column (mol/m$^2$)',
        title=f'O$_3$ Tropospheric Column',
        cmap='jet'
    ),
    'CH4___': ProductConfig(
        display_name='CH\u2084',
        dataset_name='methane_mixing_ratio',
        vmin=None, vmax=None,
        units=f'CH$_4$ Methane Mixing Ratio',
        title=f'CH$_4$ Methane Mixing Ratio',
        cmap='RdBu_r'
    ),
    'CO____': ProductConfig(
        display_name='CO',
        dataset_name='carbonmonoxide_total_column',
        vmin=None, vmax=None,
        units=f'CO Total Column (mol/m$^2$)',
        title=f'CO Total Column',
        cmap='RdBu_r'
    ),
    'NO2___': ProductConfig(
        display_name='NO\u2082',
        dataset_name='nitrogendioxide_tropospheric_column',
        vmin=-2e-4, vmax=2e-4,
        units=f'NO$_2$ Tropospheric Column (mol/m$^2$)',
        title=f'NO$_2$ Tropospheric Column',
        cmap='RdBu_r'
    ),
    'HCHO__': ProductConfig(
        display_name='HCHO',
        dataset_name='formaldehyde_tropospheric_vertical_column',
        vmin=-4e-4, vmax=4e-4,
        units=f'HCHO Tropospheric Vertical Column (mol/m$^2$)',
        title=f'HCHO Tropospheric Vertical Column',
        cmap='RdBu_r'
    ),
    'SO2___': ProductConfig(
        display_name='SO\u2082',
        dataset_name='sulfurdioxide_total_vertical_column',
        vmin=-4e-3, vmax=4e-3,
        units=f'SO$_2$ Total Vertical Column (mol/m$^2$)',
        title=f'SO$_2$ Total Vertical Column',
        cmap='RdBu_r'
    ),
    'AER_AI': ProductConfig(
        display_name='Aerosol Index',
        dataset_name='aerosol_index_340_380',
        vmin=None, vmax=None,
        units=f'Aerosol Index',
        title=f'Aerosol Index',
        cmap='viridis'
    )
}
