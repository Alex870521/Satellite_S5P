"""ERA5 Data Processing Main Program"""
from datetime import datetime
from src.api import ERA5Hub



def main():
    # 1. Set parameters
    start_date, end_date = datetime(2025, 1, 1), datetime(2025, 6, 30)
    # TODO: 結束時間會因為資料延遲6天的原因 檔名會是end_date，但資料只到前六天左右 (開始日期跟檔名的關係也要調整)

    # Variables to retrieve, options:
    # Dataset: reanalysis-era5-pressure-levels
    # - 'relative_humidity'
    # - 'temperature'
    # - 'u_component_of_wind'
    # - 'v_component_of_wind'

    # Dataset: reanalysis-era5-single-levels
    # - '10m_u_component_of_wind'
    # - '10m_v_component_of_wind'
    # - '2m_temperature'
    # - '2m_dewpoint_temperature'
    # - 'boundary_layer_height'
    # - 'surface_pressure'
    # - 'total_column_water_vapour'

    # TODO: deal with Single-layer and Pressure levels
    # Due to the Request size maximum, it can only download two variable each time.
    variables = ['10m_u_component_of_wind', '10m_v_component_of_wind', ]
    # variables = ['2m_dewpoint_temperature', '2m_temperature',]
    # variables = ['boundary_layer_height']
    # variables = ['total_column_water_vapour', 'surface_pressure']

    # For pressure levels
    # variables = ['temperature']

    # Pressure levels (in hPa), set to None for surface data only
    # Common levels: = ["1", "2", "3", "5", "7", "10", "20", "30", "50", "70",
    #                   "100", "125", "150", "175", "200", "225", "250", "300", "350",
    #                   "400", "450", "500", "550", "600", "650", "700", "750", "775",
    #                   "800", "825", "850", "875", "900", "925", "950", "975", "1000"]

    pressure_levels = None

    # Set Taiwan regional boundary (min_lon, max_lon, min_lat, max_lat)
    boundary = (119, 123, 21, 26)

    # 2. Create data hub instance
    era5_hub = ERA5Hub(timezone='Asia/Taipei')

    # 3. Fetch data
    era5_hub.fetch_data(
        start_date=start_date,
        end_date=end_date,
        boundary=boundary,
        variables=variables,
        pressure_levels=pressure_levels,
        download_mode='all_at_once'
    )

    # 4. Download data
    era5_hub.download_data()

    # 5. Process data
    # Define observation stations
    # STATIONS = [
    #     {"name": "FS", "lat": 22.6294, "lon": 120.3461},  # Kaohsiung Fengshan
    #     {"name": "NZ", "lat": 22.7422, "lon": 120.3339},  # Kaohsiung Nanzi
    #     {"name": "TH", "lat": 24.1817, "lon": 120.5956},  # Taichung
    #     {"name": "TP", "lat": 25.0330, "lon": 121.5654}   # Taipei
    # ]
    #
    # era5_hub.process_data(STATIONS)


if __name__ == "__main__":
    main()