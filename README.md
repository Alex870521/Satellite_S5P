## <div align="center">Satellite Data Processing Toolkit</div>

<div align="center">

![Python](https://img.shields.io/badge/Python-3.12-blue.svg)
![GitHub last commit](https://img.shields.io/github/last-commit/Alex870521/aeroviz?logo=github)

</div>

<div align="center">
<a href="https://github.com/Alex870521"><img src="https://cdn.simpleicons.org/github/0A66C2" width="3%" alt="LinkedIn"></a>
<span style="margin: 0 1%"></span>
<a href="https://www.linkedin.com/in/Alex870521/"><img src="https://cdn.simpleicons.org/linkedin/0A66C2" width="3%" alt="LinkedIn"></a>
<span style="margin: 0 1%"></span>
<a href="https://medium.com/@alex870521"><img src="https://cdn.simpleicons.org/medium/0A66C2" width="3%" alt="Medium"></a></div>

---

A comprehensive Python toolkit for retrieving, processing, and visualizing satellite data from multiple sources: Sentinel-5P, MODIS, and ERA5. This toolkit focuses on atmospheric data including air pollutants (NO₂, CO, SO₂, O₃, HCHO), aerosol optical depth, and meteorological parameters.

## <div align="center">Features</div>

- **Multi-platform Support**:
  - **Sentinel-5P**: Trace gases and air pollutants
  - **MODIS**: Aerosol optical depth (AOD) measurements
  - **ERA5**: Reanalysis of atmospheric, land, and oceanic climate variables

- **Unified Data Access**:
  - Automated data retrieval from Copernicus Open Access Hub, NASA Earthdata, and Climate Data Store
  - Consistent API across different data sources

- **Advanced Processing**:
  - Quality control and filtering
  - Spatial interpolation and regridding
  - Temporal aggregation
  - Station-based data extraction

- **Visualization**:
  - High-quality concentration and parameter maps
  - Customizable geographic boundaries
  - Time series analysis capabilities

- **Resource Management**:
  - Efficient download handling with caching
  - Built-in file retention management

## <div align="center">Prerequisites</div>

Before using this toolkit, you need to complete the following steps:

1. **Copernicus Account** (for Sentinel-5P and ERA5):
   - Register for a free account at [Copernicus Open Access Hub](https://scihub.copernicus.eu/dhus/#/home)
   - For ERA5, also register at [Climate Data Store](https://cds.climate.copernicus.eu/)

2. **NASA Earthdata Account** (for MODIS):
   - Register at [NASA Earthdata](https://urs.earthdata.nasa.gov/)

3. **Environment Configuration**:
   - Create a `.env` file in the project root directory with your credentials:
     ```
     # Sentinel-5P credentials
     COPERNICUS_USERNAME=your_username
     COPERNICUS_PASSWORD=your_password
     
     # ERA5 credentials
     CDSAPI_URL=https://cds.climate.copernicus.eu/api/v2
     CDSAPI_KEY=your_key
     
     # NASA Earthdata credentials
     EARTHDATA_USERNAME=your_username
     EARTHDATA_PASSWORD=your_password
     ```

## <div align="center">Installation</div>

```bash
# Clone the repository
git clone https://github.com/Alex870521/Satellite_S5P.git

# Navigate to the directory
cd Satellite_S5P

# Install required packages
pip install -r requirements.txt
```

## <div align="center">Usage</div>

### Sentinel-5P Example

```python
"""SENTINEL-5P Data Processing Example"""
from datetime import datetime
from src.api import SENTINEL5PHub

# 1. Set parameters
start_date, end_date = datetime(2025, 3, 1), datetime(2025, 3, 13)

# File class: 'NRTI' (Near Real-Time) or 'OFFL' (Offline processed)
file_class = 'NRTI'

# Available file types: 'NO2___', 'O3____', 'CO____', 'SO2___', 'CH4___', 'CLOUD_', 'AER_AI'
file_type = 'NO2___'

# Define region boundary (min_lon, max_lon, min_lat, max_lat)
boundary = (120, 122, 22, 25)

# 2. Create data hub instance
sentinel_hub = SENTINEL5PHub(max_workers=3)

# 3. Fetch data
products = sentinel_hub.fetch_data(
    file_class=file_class,
    file_type=file_type,
    start_date=start_date,
    end_date=end_date,
    boundary=boundary
)

# 4. Download data
sentinel_hub.download_data(products)

# 5. Process data
sentinel_hub.process_data()
```

### MODIS Example

```python
"""MODIS Data Processing Example"""
from datetime import datetime
from src.api import MODISHub

# 1. Set parameters
start_date, end_date = datetime(2025, 3, 1), datetime(2025, 3, 12)

# Product types: 'MOD04_L2' (Terra) or 'MYD04_L2' (Aqua) Level-2 Products
modis_product_type = "MYD04_L2"

# 2. Create data hub instance
modis_hub = MODISHub()

# 3. Fetch data
products = modis_hub.fetch_data(
    file_type=modis_product_type,
    start_date=start_date,
    end_date=end_date
)

# 4. Download data
modis_hub.download_data(products)

# 5. Process data
modis_hub.process_data()
```

### ERA5 Example

```python
"""ERA5 Data Processing Example"""
from datetime import datetime
from src.api import ERA5Hub

# 1. Set parameters
start_date, end_date = datetime(2025, 3, 1), datetime(2025, 3, 19)

# Variables to retrieve (more options available)
variables = ['boundary_layer_height']

# Pressure levels in hPa (set to None for surface data only)
pressure_levels = None

# Region boundary (min_lon, max_lon, min_lat, max_lat)
boundary = (119, 123, 21, 26)

# Define observation stations
STATIONS = [
    {"name": "FS", "lat": 22.6294, "lon": 120.3461},  # Kaohsiung Fengshan
    {"name": "NZ", "lat": 22.7422, "lon": 120.3339},  # Kaohsiung Nanzi
    {"name": "TH", "lat": 24.1817, "lon": 120.5956},  # Taichung
    {"name": "TP", "lat": 25.0330, "lon": 121.5654}   # Taipei
]

# 2. Create data hub instance
era5_hub = ERA5Hub(timezone='Asia/Taipei')

# 3. Fetch data
era5_hub.fetch_data(
    start_date=start_date,
    end_date=end_date,
    boundary=boundary,
    variables=variables,
    pressure_levels=pressure_levels,
    stations=STATIONS,
)

# 4. Download data
era5_hub.download_data()

# 5. Process data
era5_hub.process_data()
```

## <div align="center">Data Sources</div>

### Sentinel-5P
- **Provider**: European Space Agency (ESA)
- **Products**: NO₂, O₃, CO, SO₂, HCHO, Cloud, Aerosol Index
- **Resolution**: 7 km x 3.5 km (at nadir)
- **Frequency**: Daily global coverage

### MODIS
- **Provider**: NASA
- **Products**: Aerosol Optical Depth (AOD)
- **Platforms**: Terra (MOD04) and Aqua (MYD04) satellites
- **Resolution**: 10 km at nadir
- **Frequency**: 1-2 days global coverage

### ERA5
- **Provider**: European Centre for Medium-Range Weather Forecasts (ECMWF)
- **Products**: Reanalysis dataset with 100+ atmospheric, land and oceanic parameters
- **Resolution**: 0.25° x 0.25° global grid (about 31 km)
- **Frequency**: Hourly data, monthly updates

## <div align="center">Processing Pipeline</div>

All data sources follow a consistent workflow:

1. **Data Discovery**: Query available products based on date range and region
2. **Download Management**: Efficient parallel downloading with error handling
3. **Quality Control**: Filtering based on data quality flags
4. **Spatial Processing**: 
   - Sentinel-5P: RBF interpolation of sparse satellite data
   - MODIS: Processing of gridded AOD values
   - ERA5: Extraction of point values for weather stations
5. **Visualization**: Generation of standardized maps and plots
6. **Export**: Structured data storage in NetCDF and CSV formats

## <div align="center">Automatic Data Management</div>

The toolkit includes built-in data retention management to prevent disk space issues:

- Automatically cleans files older than the configured retention period
- Maintains directory structure while removing outdated files
- Can be scheduled for periodic execution or triggered manually

## <div align="center">Contact</div>

For bug reports and feature requests please visit [GitHub Issues](https://github.com/Alex870521/Satellite_DataKit/issues).