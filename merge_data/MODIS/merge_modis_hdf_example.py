#!/usr/bin/env python3
"""
MODIS HDF 文件合併示例

這個腳本展示如何使用 MODISProcessor 將多個 HDF 文件合併成一個 NetCDF 文件。
支援 MCD19A2、MOD04_L2、MYD04_L2 等數據類型。
合併後的數據將包含時間、經度和緯度三個維度。

注意解析度差異：
- MCD19A2: 1km × 1km 原始解析度
- MOD04_L2/MYD04_L2: 10km × 10km 標準產品，3km × 3km 高解析度產品
- 預設使用 AOD_550_Dark_Target_Deep_Blue_Combined (DT+DB 合併算法)
"""

import logging
from pathlib import Path
from src.processing.modis_processor import MODISProcessor
from src.config.settings import BASE_DIR
import numpy as np

def setup_logging():
    """設置日誌"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def get_resolution_info(data_type):
    """
    獲取不同數據類型的解析度信息
    
    Parameters:
    -----------
    data_type : str
        數據類型
    
    Returns:
    --------
    dict : 包含解析度信息的字典
    """
    resolution_info = {
        "MCD19A2": {
            "original_resolution": "1km × 1km",
            "grid_resolution": 0.01,  # 度
            "description": "Combined Terra and Aqua MODIS aerosol product",
            "aod_variable": "Optical_Depth_047 or Optical_Depth_055"
        },
        "MOD04_L2": {
            "original_resolution": "10km × 10km (標準) / 3km × 3km (高解析度)", 
            "grid_resolution": 0.1,  # 度
            "description": "Terra satellite MODIS aerosol product (DT+DB Combined)",
            "aod_variable": "AOD_550_Dark_Target_Deep_Blue_Combined (預設)"
        },
        "MYD04_L2": {
            "original_resolution": "10km × 10km (標準) / 3km × 3km (高解析度)",
            "grid_resolution": 0.1,  # 度
            "description": "Aqua satellite MODIS aerosol product (DT+DB Combined)",
            "aod_variable": "AOD_550_Dark_Target_Deep_Blue_Combined (預設)"
        }
    }
    
    return resolution_info.get(data_type, {
        "original_resolution": "Unknown",
        "grid_resolution": 0.01,
        "description": "Unknown product"
    })

def merge_modis_data(data_type, start_date="2024-01-01", end_date="2024-12-31", 
                     use_appropriate_resolution=True, aod_variable='AOD_550_Dark_Target_Deep_Blue_Combined'):
    """
    合併指定類型的 MODIS 數據
    
    Parameters:
    -----------
    data_type : str
        數據類型 ('MCD19A2', 'MOD04_L2', 'MYD04_L2')
    start_date : str
        開始日期 (YYYY-MM-DD)
    end_date : str
        結束日期 (YYYY-MM-DD)
    use_appropriate_resolution : bool
        是否使用適當的解析度（True: 根據數據類型調整，False: 統一使用1km）
    aod_variable : str
        MOD04/MYD04 使用的 AOD 變量名稱
        - 'AOD_550_Dark_Target_Deep_Blue_Combined' (推薦，DT+DB合併產品)
        - 'Optical_Depth_Land_And_Ocean' (舊版)
        - 'Image_Optical_Depth_Land_And_Ocean' (繪圖用)
    """
    logger = setup_logging()
    
    # 獲取解析度信息
    resolution_info = get_resolution_info(data_type)
    
    logger.info(f"開始處理 {data_type} 數據")
    logger.info(f"原始解析度: {resolution_info['original_resolution']}")
    logger.info(f"產品描述: {resolution_info['description']}")
    logger.info(f"時間範圍: {start_date} 到 {end_date}")
    
    if data_type in ['MOD04_L2', 'MYD04_L2']:
        logger.info(f"使用 AOD 變量: {aod_variable}")
    
    if use_appropriate_resolution:
        logger.info(f"將使用適當的網格解析度: {resolution_info['grid_resolution']}°")
    else:
        logger.info("將統一使用 0.01° 網格解析度（1km）")
    
    # 初始化 MODIS 處理器（傳入 AOD 變量名稱）
    processor = MODISProcessor(aod_variable=aod_variable)
    
    # 設置目錄路徑（BASE_DIR 可用環境變數 SATELLITE_BASE_DIR 覆寫）
    processor.raw_dir = BASE_DIR / "MODIS" / "raw"
    processor.processed_dir = BASE_DIR / "MODIS" / "processed"
    processor.figure_dir = BASE_DIR / "MODIS" / "figures"
    processor.logger = logger
    processor.file_type = data_type
    
    # 根據數據類型設置不同的模式
    if data_type == "MCD19A2":
        pattern = f"**/{data_type}/**/*.hdf"
        output_filename = f"{data_type}_{start_date.replace('-', '')}_{end_date.replace('-', '')}"
    elif data_type in ["MOD04_L2", "MYD04_L2"]:
        pattern = f"**/{data_type}/**/*.hdf"
        output_filename = f"{data_type}_{start_date.replace('-', '')}_{end_date.replace('-', '')}"
    else:
        logger.error(f"不支援的數據類型: {data_type}")
        return False
    
    # 執行合併
    success = processor.merge_hdf_files_to_netcdf(
        pattern=pattern,
        start_date=start_date,
        end_date=end_date,
        output_filename=output_filename,
        merge_by_month=False
    )
    
    if success:
        logger.info(f"{data_type} 數據合併成功！")
        logger.info(f"輸出檔案: {output_filename}.nc")
        return True
    else:
        logger.error(f"{data_type} 數據合併失敗！")
        return False

def inspect_merged_netcdf(data_type, start_date="2024-01-01", end_date="2024-12-31"):
    """
    檢查合併後的 NetCDF 文件
    
    Parameters:
    -----------
    data_type : str
        數據類型
    start_date : str
        開始日期
    end_date : str
        結束日期
    """
    import xarray as xr
    
    logger = setup_logging()
    
    # 獲取解析度信息
    resolution_info = get_resolution_info(data_type)
    
    # 構建檔案路徑
    output_filename = f"{data_type}_{start_date.replace('-', '')}_{end_date.replace('-', '')}"
    nc_file = BASE_DIR / "MODIS" / "processed" / data_type / f"{output_filename}.nc"
    
    if nc_file.exists():
        logger.info(f"讀取合併的 NetCDF 文件: {nc_file}")
        logger.info(f"原始解析度: {resolution_info['original_resolution']}")
        if 'aod_variable' in resolution_info:
            logger.info(f"AOD 變量: {resolution_info['aod_variable']}")
        
        # 打開數據集
        ds = xr.open_dataset(nc_file)
        
        # 顯示數據集信息
        logger.info("數據集信息:")
        logger.info(f"  維度: {dict(ds.dims)}")
        logger.info(f"  變量: {list(ds.data_vars.keys())}")
        logger.info(f"  座標: {list(ds.coords.keys())}")
        
        # 顯示主要變量信息
        for var_name in ds.data_vars.keys():
            var = ds[var_name]
            logger.info(f"\n{var_name} 數據信息:")
            logger.info(f"  形狀: {var.shape}")
            logger.info(f"  數據類型: {var.dtype}")
            
            # 檢查是否有時間維度
            if 'time' in var.dims:
                logger.info(f"  時間範圍: {var.time.min().values} 到 {var.time.max().values}")
            
            # 檢查是否有經緯度維度
            if 'lon' in var.dims and 'lat' in var.dims:
                logger.info(f"  經度範圍: {var.lon.min().values:.2f} 到 {var.lon.max().values:.2f}")
                logger.info(f"  緯度範圍: {var.lat.min().values:.2f} 到 {var.lat.max().values:.2f}")
                
                # 計算實際網格解析度
                if len(var.lon) > 1 and len(var.lat) > 1:
                    lon_res = (var.lon.max().values - var.lon.min().values) / (len(var.lon) - 1)
                    lat_res = (var.lat.max().values - var.lat.min().values) / (len(var.lat) - 1)
                    logger.info(f"  實際網格解析度: 經度 {lon_res:.3f}°, 緯度 {lat_res:.3f}°")
                    
                    # 轉換為公里
                    lon_km = lon_res * 111.0 * np.cos(np.radians(23.5))  # 台灣緯度約23.5°
                    lat_km = lat_res * 111.0
                    logger.info(f"  實際空間解析度: 經度 {lon_km:.1f}km, 緯度 {lat_km:.1f}km")
            
            # 顯示數值範圍
            if hasattr(var, 'min') and hasattr(var, 'max'):
                logger.info(f"  數值範圍: {var.min().values:.4f} 到 {var.max().values:.4f}")
            
            # 顯示單位（如果有）
            if hasattr(var, 'units'):
                logger.info(f"  單位: {var.units}")
        
        # 顯示屬性
        logger.info("\n數據集屬性:")
        for key, value in ds.attrs.items():
            logger.info(f"  {key}: {value}")
        
        ds.close()
        return True
    else:
        logger.warning(f"NetCDF 文件不存在: {nc_file}")
        return False

def main():
    """主函數 - 互動式選擇數據類型"""
    print("🌍 MODIS HDF 文件合併工具")
    print("=" * 50)
    print("📊 數據產品信息:")
    print("  - MCD19A2: 1km × 1km 解析度")
    print("  - MOD04_L2/MYD04_L2: 10km (標準) / 3km (高解析度)")
    print("\n⭐ AOD 變量 (MOD04/MYD04):")
    print("  - 預設: AOD_550_Dark_Target_Deep_Blue_Combined")
    print("  - 優勢: DT+DB 合併算法，最高質量")
    print("=" * 50)
    
    # 可用的數據類型
    data_types = {
        "1": "MCD19A2",
        "2": "MOD04_L2", 
        "3": "MYD04_L2"
    }
    
    print("\n請選擇要處理的數據類型:")
    print("  1. MCD19A2 (AOD 數據, 1km解析度)")
    print("  2. MOD04_L2 (Terra AOD 數據, ~10km解析度, DT+DB)")
    print("  3. MYD04_L2 (Aqua AOD 數據, ~10km解析度, DT+DB)")
    print("  4. 處理所有數據類型")
    
    try:
        choice = input("\n請輸入選項 (1-4): ").strip()
        
        # 詢問是否使用適當解析度
        use_appropriate_resolution = input("使用適當的解析度設置？(y/n, 預設: y): ").strip().lower() != 'n'
        
        # 詢問 AOD 變量選擇（僅對 MOD04/MYD04）
        aod_variable = 'AOD_550_Dark_Target_Deep_Blue_Combined'
        if choice in ["2", "3", "4"]:
            print("\n請選擇 AOD 變量:")
            print("  1. AOD_550_Dark_Target_Deep_Blue_Combined (推薦，DT+DB合併)")
            print("  2. Optical_Depth_Land_And_Ocean (舊版)")
            print("  3. Image_Optical_Depth_Land_And_Ocean (繪圖用)")
            aod_choice = input("請輸入選項 (1-3, 預設: 1): ").strip() or "1"
            aod_options = {
                "1": "AOD_550_Dark_Target_Deep_Blue_Combined",
                "2": "Optical_Depth_Land_And_Ocean",
                "3": "Image_Optical_Depth_Land_And_Ocean"
            }
            aod_variable = aod_options.get(aod_choice, "AOD_550_Dark_Target_Deep_Blue_Combined")
            print(f"✅ 選擇了 AOD 變量: {aod_variable}")
        
        if choice == "4":
            # 處理所有數據類型
            print("\n🔄 開始處理所有數據類型...")
            for data_type in data_types.values():
                print(f"\n--- 處理 {data_type} ---")
                success = merge_modis_data(data_type, use_appropriate_resolution=use_appropriate_resolution, 
                                          aod_variable=aod_variable)
                if success:
                    inspect_merged_netcdf(data_type)
                print("-" * 30)
        elif choice in data_types:
            data_type = data_types[choice]
            print(f"\n✅ 選擇了: {data_type}")
            
            # 顯示解析度信息
            resolution_info = get_resolution_info(data_type)
            print(f"原始解析度: {resolution_info['original_resolution']}")
            print(f"產品描述: {resolution_info['description']}")
            
            # 詢問是否要自訂日期範圍
            use_custom_dates = input("使用自訂日期範圍？(y/n, 預設: n): ").strip().lower() == 'y'
            
            if use_custom_dates:
                start_date = input("開始日期 (YYYY-MM-DD, 預設: 2024-01-01): ").strip() or "2022-01-01"
                end_date = input("結束日期 (YYYY-MM-DD, 預設: 2024-12-31): ").strip() or "2022-12-31"
            else:
                start_date = "2022-01-01"
                end_date = "2022-12-31"
            
            # 執行合併
            success = merge_modis_data(data_type, start_date, end_date, use_appropriate_resolution, 
                                      aod_variable=aod_variable)
            
            if success:
                # 檢查結果
                inspect_merged_netcdf(data_type, start_date, end_date)
        else:
            print("❌ 無效選項，使用預設 MCD19A2")
            merge_modis_data("MCD19A2", use_appropriate_resolution=use_appropriate_resolution,
                           aod_variable=aod_variable)
            inspect_merged_netcdf("MCD19A2")
            
    except KeyboardInterrupt:
        print("\n\n⏹️  操作已取消")
    except Exception as e:
        print(f"\n❌ 錯誤: {e}")

if __name__ == "__main__":
    main()
