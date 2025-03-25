"""ERA5 Data Processing Main Program"""
from datetime import datetime
from src.api import ERA5Hub



def main():
    # 1. Set parameters
    start_date, end_date = datetime(2022, 1, 1), datetime(2024, 12, 31)

    # Variables to retrieve, options:
    # - 'boundary_layer_height'
    # - 'temperature'
    # - 'u_component_of_wind'
    # - 'v_component_of_wind'
    variables = ['boundary_layer_height']

    # Pressure levels (in hPa), set to None for surface data only
    # Common levels: 1000, 925, 850, 700, 500, 250, 100, 50, 10
    pressure_levels = None

    # Set Taiwan regional boundary (min_lon, max_lon, min_lat, max_lat)
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
        download_mode='all_at_once'
    )

    # 4. Download data
    era5_hub.download_data()

    # 5. Process data
    era5_hub.process_data()


if __name__ == "__main__":
    main()