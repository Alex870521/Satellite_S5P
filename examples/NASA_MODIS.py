"""MODIS Data Processing Main Program"""
from datetime import datetime
from src.api import MODISHub


def main():
    # 1. Set parameters
    start_date, end_date = datetime(2025, 3, 1), datetime(2025, 3, 12)

    # Product type options:
    # - 'MOD04': Terra satellite MODIS aerosol product
    # - 'MYD04': Aqua satellite MODIS aerosol product
    # - 'MCD04': Combined Terra and Aqua MODIS aerosol product (Not implemented)
    modis_product_type = "MYD04"

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


if __name__ == "__main__":
    main()