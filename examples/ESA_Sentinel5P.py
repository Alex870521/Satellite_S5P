"""SENTINEL-5P Data Processing Main Program"""
from datetime import datetime
from src.api import SENTINEL5PHub


def main():
    # 1. Set parameters
    start_date, end_date = datetime(2025, 3, 1), datetime(2025, 3, 13)

    # File class options:
    # - 'NRTI': Near Real-Time data
    # - 'OFFL': Offline processed data
    file_class = 'NRTI'

    # File type options:
    # - 'NO2___': Nitrogen dioxide
    # - 'O3____': Ozone
    # - 'CO____': Carbon monoxide
    # - 'SO2___': Sulfur dioxide
    # - 'CH4___': Methane
    # - 'CLOUD_': Cloud information
    # - 'AER_AI': Aerosol index
    file_type = 'NO2___'

    # Define Taiwan regional boundary (min_lon, max_lon, min_lat, max_lat)
    boundary = (120, 122, 22, 25)

    # 2. Create data hub instance
    sentinel_hub = SENTINEL5PHub(max_workers=3)

    # 3. Fetch data
    products = sentinel_hub.fetch_data(
        file_class=file_class,
        file_type=file_type,
        start_date=start_date,
        end_date=end_date,
        boundary=boundary,
        limit=None
    )
    # 4. Download data
    sentinel_hub.download_data(products)

    # 5. Process data
    sentinel_hub.process_data()


if __name__ == "__main__":
    main()