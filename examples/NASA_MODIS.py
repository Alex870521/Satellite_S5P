"""MODIS Data Processing Main Program"""
from datetime import datetime
from src.api import MODISHub


def main():
    # 1. Set parameters
    start_date, end_date = datetime(2025, 10, 1), datetime(2025, 10, 31)

    # Product type options:
    # - 'MOD04_L2': Terra satellite MODIS aerosol product
    # - 'MYD04_L2': Aqua satellite MODIS aerosol product
    # - 'MOD04_3K': Terra satellite MODIS high-resolution aerosol product
    # - 'MYD04_3K': Aqua satellite MODIS high-resolution aerosol product
    # - 'MCD19A1': Combined Terra and Aqua MODIS surface spectral reflectance product
    # - 'MCD19A2': Combined Terra and Aqua MODIS aerosol product
    # - 'MCD19A3D': Combined Terra and Aqua MODIS BRDF and VI
    modis_product_type = "MYD04_L2"

    # 2. Create data hub instance
    modis_hub = MODISHub()

    # 3. Run the full pipeline: fetch -> download -> process.
    #    Use fetch_data() / download_data() / process_data() separately for granular control.
    modis_hub.run_pipeline(
        file_type=modis_product_type,
        start_date=start_date,
        end_date=end_date,
    )


if __name__ == "__main__":
    main()