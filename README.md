## <div align="center">Satellite_S5P</div>

<div align="center">

![Python](https://img.shields.io/badge/Python-3.12-blue.svg)
![GitHub last commit](https://img.shields.io/github/last-commit/Alex870521/aeroviz?logo=github)

</div>

<div align="center">
<a href="https://github.com/Alex870521"><img src="https://cdn.simpleicons.org/github/0A66C2" width="3%" alt="GitHub"></a>
<span style="margin: 0 1%"></span>
<a href="https://www.linkedin.com/in/Alex870521/"><img src="https://cdn.simpleicons.org/linkedin/0A66C2" width="3%" alt="LinkedIn"></a>
<span style="margin: 0 1%"></span>
<a href="https://medium.com/@alex870521"><img src="https://cdn.simpleicons.org/medium/0A66C2" width="3%" alt="Medium"></a></div>

---

A Python toolkit for retrieving, processing, and visualizing Sentinel-5P satellite data, with a focus on air pollutants like NO₂, CO, SO₂, O₃, and HCHO.

## <div align="center">Features</div>

- Automated data retrieval from Copernicus Open Access Hub
- Advanced data processing with interpolation and quality control
- High-quality visualization of pollutant concentration maps
- Time series analysis capabilities
- Built-in file retention management to prevent storage overflow

## <div align="center">Prerequisites</div>

Before using this toolkit, you need to complete the following steps:

1. **Copernicus Account Setup**:
   - Register for a free account at [Copernicus Open Access Hub](https://scihub.copernicus.eu/dhus/#/home)
   - Save your username and password for API access

2. **Environment Configuration**:
   - Create a `.env` file in the project root directory with your credentials:
     ```
     COPERNICUS_USERNAME=your_username
     COPERNICUS_PASSWORD=your_password
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

### Fetching Satellite Data
```python
from src.api.sentinel_api import S5PFetcher

# Initialize fetcher
fetcher = S5PFetcher(max_workers=3)

# Fetch products
products = await fetcher.fetch_data(
    file_class='OFFL',  # 'OFFL' for offline data, 'NRTI' for near-real-time
    file_type='NO2___',  # Available types: 'NO2___', 'CO____', 'SO2___', 'O3__PR', 'HCHO__'
    start_date='2025-02-20',
    end_date='2025-03-06',
    boundary=(120, 122, 22, 25),
    limit=None
)

# Download products
fetcher.parallel_download(products)
```

Example output:
```
╭──────────────────────────────────────────────────────────────────────────────────────────────────╮
│              Fetching sentinel-5p products (NO₂) from 2025-02-20 to 2025-03-06 ...               │
╰──────────────────────────────────────────────────────────────────────────────────────────────────╯
╭──────────────────────────────────────── Found 7 Products ────────────────────────────────────────╮
│                                                                                                  │
│                                       Product Information                                        │
│┏━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┓ │
│┃ No. ┃ Time                ┃ Name                                                  ┃      Size ┃ │
│┡━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━┩ │
││   1 │ 2025-02-25T04:54:18 │ S5P_OFFL_L2__NO2____20250225T043243...50226T204825.nc │ 589.88 MB │ │
││   2 │ 2025-02-24T05:13:14 │ S5P_OFFL_L2__NO2____20250224T045140...50225T210805.nc │ 591.67 MB │ │
││   3 │ 2025-02-24T03:31:44 │ S5P_OFFL_L2__NO2____20250224T031009...50226T055814.nc │ 593.46 MB │ │
││   4 │ 2025-02-23T03:50:40 │ S5P_OFFL_L2__NO2____20250223T032906...50224T235016.nc │ 593.48 MB │ │
││   5 │ 2025-02-22T04:09:37 │ S5P_OFFL_L2__NO2____20250222T034802...50223T200228.nc │ 590.79 MB │ │
││   6 │ 2025-02-21T04:28:32 │ S5P_OFFL_L2__NO2____20250221T040659...50222T202822.nc │ 589.21 MB │ │
││   7 │ 2025-02-20T04:47:30 │ S5P_OFFL_L2__NO2____20250220T042555...50221T204529.nc │ 564.02 MB │ │
│└─────┴─────────────────────┴───────────────────────────────────────────────────────┴───────────┘ │
│                                                                                                  │
╰──────────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Processing Data
```python
from src.processing.data_processor import S5Processor

# Initialize processor
processor = S5Processor(
    interpolation_method='rbf',
    resolution=(5.5, 3.5),  # Resolution in km
    mask_qc_value=0.5       # Quality control threshold
)

# Process data
processor.process_each_data(
    file_class='OFFL',
    file_type='NO2___',
    start_date='2025-02-20',
    end_date='2025-03-06',
)
```

## <div align="center">Data Description</div>

### NO2 Data Processing
This toolkit processes nitrogen dioxide (NO2) and other pollutant data from satellite observations with these key features:

- **Quality Control**: Filters out low-quality measurements
- **Spatial Interpolation**: Converts sparse satellite readings to regular grid
- **Temporal Aggregation**: Daily, weekly, and monthly averages
- **Visualization**: Generates high-quality maps and time series plots
- **Data Export**: Creates NetCDF files compatible with other analysis tools

## <div align="center">Contact</div>

For bug reports and feature requests please visit [GitHub Issues](https://github.com/Alex870521/Satellite_S5P/issues).