#!/usr/bin/env python3
"""
測試插值前後比較
使用2022年1月的數據，比較10天左右的插值效果
"""

import logging
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Tuple

from src.utils.extract_datetime_from_filename import extract_datetime_from_filename
from src.config.settings import BASE_DIR
from merge_data.S5P.merge_s5p_unified import S5PProcessor


def setup_logging():
    """設置日誌"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)


def test_interpolation_comparison():
    """測試原始檔案 vs 已處理檔案的插值差異"""
    logger = setup_logging()
    
    print("🔬 原始檔案 vs 已處理檔案 插值比較測試")
    print("=" * 60)
    
    # 設定測試參數
    year = "2022"
    start_date = "2022-01-01"
    end_date = "2022-01-10"  # 測試10天
    gas_type = 'O3'
    
    print(f"測試期間: {start_date} 到 {end_date}")
    print(f"測試氣體: {gas_type}")
    
    # 檢查目錄
    raw_dir = BASE_DIR / "Sentinel-5P" / "raw"
    processed_dir = BASE_DIR / "Sentinel-5P" / "processed"
    output_dir = Path.home() / "Desktop"
    
    print(f"\n📁 目錄檢查:")
    print(f"原始檔案目錄: {raw_dir} - {'✅ 存在' if raw_dir.exists() else '❌ 不存在'}")
    print(f"已處理檔案目錄: {processed_dir} - {'✅ 存在' if processed_dir.exists() else '❌ 不存在'}")
    print(f"輸出目錄: {output_dir} - {'✅ 存在' if output_dir.exists() else '❌ 不存在'}")
    
    if not raw_dir.exists() or not processed_dir.exists():
        print("❌ 必要目錄不存在，無法進行比較")
        return
    
    # 初始化兩個處理器
    print(f"\n🔧 初始化處理器...")
    raw_processor = S5PProcessor(gas_type=gas_type, use_processed_files=False)
    raw_processor.raw_dir = raw_dir
    raw_processor.output_dir = output_dir
    raw_processor.logger = logger
    
    processed_processor = S5PProcessor(gas_type=gas_type, use_processed_files=True)
    processed_processor.raw_dir = processed_dir
    processed_processor.output_dir = output_dir
    processed_processor.logger = logger
    
    # 找到測試文件
    print(f"\n🔍 尋找測試文件...")
    raw_files = raw_processor.find_s5p_files(start_date=start_date, end_date=end_date, year=year)
    processed_files = processed_processor.find_s5p_files(start_date=start_date, end_date=end_date, year=year)
    
    print(f"原始檔案: {len(raw_files)} 個")
    print(f"已處理檔案: {len(processed_files)} 個")
    
    if not raw_files or not processed_files:
        print("❌ 沒有找到足夠的測試文件")
        return
    
    # 比較原始檔案和已處理檔案
    print(f"\n📊 開始比較分析...")
    
    # 選擇第一個檔案進行比較
    print(f"\n📋 檔案列表:")
    print("原始檔案:")
    for i, f in enumerate(raw_files[:3]):
        print(f"  {i+1}. {f.name}")
    print("已處理檔案:")
    for i, f in enumerate(processed_files[:3]):
        print(f"  {i+1}. {f.name}")
    
    # 使用第一個檔案進行比較
    raw_test_file = raw_files[0]
    processed_test_file = processed_files[0]
    
    print(f"\n🔍 選擇比較檔案:")
    print(f"  原始檔案: {raw_test_file.name}")
    print(f"  已處理檔案: {processed_test_file.name}")
    
    # 分析原始檔案
    print(f"\n🔍 分析原始檔案...")
    raw_result = analyze_file_structure(raw_test_file, raw_processor, "原始檔案")
    
    # 分析已處理檔案
    print(f"\n🔍 分析已處理檔案...")
    processed_result = analyze_file_structure(processed_test_file, processed_processor, "已處理檔案")
    
    # 比較結果
    print(f"\n📈 比較結果:")
    print("-" * 60)
    print(f"{'項目':<20} {'原始檔案':<15} {'已處理檔案':<15} {'差異':<10}")
    print("-" * 60)
    print(f"{'緯度維度':<20} {raw_result['lat_size']:<15} {processed_result['lat_size']:<15} {processed_result['lat_size'] - raw_result['lat_size']:<10}")
    print(f"{'經度維度':<20} {raw_result['lon_size']:<15} {processed_result['lon_size']:<15} {processed_result['lon_size'] - raw_result['lon_size']:<10}")
    print(f"{'有效數據點':<20} {raw_result['valid_count']:<15} {processed_result['valid_count']:<15} {processed_result['valid_count'] - raw_result['valid_count']:<10}")
    print(f"{'數據完整度':<20} {raw_result['valid_ratio']:.1f}%{'':<10} {processed_result['valid_ratio']:.1f}%{'':<10} {processed_result['valid_ratio'] - raw_result['valid_ratio']:.1f}%")
    
    # 測試插值效果
    if raw_result and processed_result:
        print(f"\n🧪 測試插值效果...")
        test_interpolation_effect_comparison(raw_processor, processed_processor, 
                                           [raw_test_file], [processed_test_file], logger)
        
        # 為每一天創建詳細比較圖
        print(f"\n📊 為每一天創建詳細比較圖...")
        create_daily_comparison_plots(raw_processor, processed_processor, raw_files, processed_files, logger)
        
        # 創建所有檔案的比較圖
        print(f"\n📊 創建所有檔案比較圖...")
        create_all_files_comparison(raw_processor, processed_processor, raw_files, processed_files, logger)


def analyze_file_structure(file_path, processor, file_type):
    """分析檔案結構"""
    try:
        if processor.use_processed_files:
            ds = xr.open_dataset(file_path)
            gas_data = ds[processor.config['variable_name']].values[0]
            lat_data = ds.latitude.values
            lon_data = ds.longitude.values
        else:
            ds = xr.open_dataset(file_path, engine='netcdf4', group='PRODUCT')
            gas_data = ds[processor.config['variable_name']].values[0]
            lat_data = ds.latitude.values[0]
            lon_data = ds.longitude.values[0]
        
        # 計算統計信息
        lat_size = len(lat_data)
        lon_size = len(lon_data)
        valid_count = np.sum(~np.isnan(gas_data))
        total_count = gas_data.size
        valid_ratio = valid_count / total_count * 100
        
        print(f"  {file_type} 維度: lat={lat_size}, lon={lon_size}")
        print(f"  {file_type} 有效數據: {valid_count}/{total_count} ({valid_ratio:.1f}%)")
        
        ds.close()
        
        return {
            'lat_size': lat_size,
            'lon_size': lon_size,
            'valid_count': valid_count,
            'total_count': total_count,
            'valid_ratio': valid_ratio
        }
        
    except Exception as e:
        print(f"❌ 分析 {file_type} 時發生錯誤: {e}")
        return None


def test_interpolation_effect_comparison(raw_processor, processed_processor, raw_files, processed_files, logger):
    """測試插值效果比較"""
    
    # 讀取原始檔案
    raw_file = raw_files[0]
    logger.info(f"讀取原始檔案: {raw_file.name}")
    
    raw_result = raw_processor._read_raw_file(raw_file)
    if raw_result is None:
        logger.error("無法讀取原始檔案")
        return
    
    gas_data_raw, lat_data_raw, lon_data_raw, time_raw = raw_result
    logger.info(f"原始檔案維度: lat={len(lat_data_raw)}, lon={len(lon_data_raw)}")
    logger.info(f"原始檔案有效數據: {np.sum(~np.isnan(gas_data_raw))}/{gas_data_raw.size}")
    
    # 讀取已處理檔案
    processed_file = processed_files[0]
    logger.info(f"讀取已處理檔案: {processed_file.name}")
    
    processed_result = processed_processor._read_processed_file(processed_file)
    if processed_result is None:
        logger.error("無法讀取已處理檔案")
        return
    
    gas_data_processed, lat_data_processed, lon_data_processed, time_processed = processed_result
    logger.info(f"已處理檔案維度: lat={len(lat_data_processed)}, lon={len(lon_data_processed)}")
    logger.info(f"已處理檔案有效數據: {np.sum(~np.isnan(gas_data_processed))}/{gas_data_processed.size}")
    
    # 對原始檔案進行插值
    logger.info("對原始檔案進行插值...")
    interpolated_raw = raw_processor.reproject_to_taiwan_grid(gas_data_raw, lat_data_raw, lon_data_raw)
    logger.info(f"插值後維度: {interpolated_raw.shape}")
    logger.info(f"插值後有效數據: {np.sum(~np.isnan(interpolated_raw))}/{interpolated_raw.size}")
    
    # 比較插值效果
    print(f"\n📊 插值效果比較:")
    print(f"原始檔案插值前有效數據: {np.sum(~np.isnan(gas_data_raw))}")
    print(f"原始檔案插值後有效數據: {np.sum(~np.isnan(interpolated_raw))}")
    print(f"已處理檔案有效數據: {np.sum(~np.isnan(gas_data_processed))}")
    
    # 計算數據保持率
    raw_retention = np.sum(~np.isnan(interpolated_raw)) / np.sum(~np.isnan(gas_data_raw)) * 100
    print(f"原始檔案插值數據保持率: {raw_retention:.1f}%")
    
    # 創建比較圖
    create_comparison_plots_enhanced(gas_data_raw, interpolated_raw, gas_data_processed,
                                   lat_data_raw, lon_data_raw, 
                                   processed_processor.taiwan_lat, processed_processor.taiwan_lon,
                                   time_raw, time_processed, raw_processor, processed_processor)


def create_comparison_plots_enhanced(gas_data_raw, interpolated_raw, gas_data_processed,
                                   lat_raw, lon_raw, lat_processed, lon_processed,
                                   time_raw, time_processed, raw_processor, processed_processor):
    """Create enhanced comparison plots"""
    
    try:
        import matplotlib.pyplot as plt
        import cartopy.crs as ccrs
        import cartopy.feature as cfeature
        
        fig = plt.figure(figsize=(20, 16))
        
        # Create subplots with map projections
        ax1 = plt.subplot(2, 2, 1, projection=ccrs.PlateCarree())
        ax2 = plt.subplot(2, 2, 2, projection=ccrs.PlateCarree())
        ax3 = plt.subplot(2, 2, 3, projection=ccrs.PlateCarree())
        ax4 = plt.subplot(2, 2, 4, projection=ccrs.PlateCarree())
        
        # Set extent for Taiwan region (for processed data)
        extent = [119.0, 123.0, 21.0, 26.0]
        
        
        # Raw data (before interpolation) - show full extent
        # Create xarray Dataset for proper plotting like in plot_nc.py
        import xarray as xr
        import numpy as np
        
        # Handle 3D coordinates from Sentinel-5P data
        # Extract 2D coordinates from 3D arrays (time=1, scanline, ground_pixel)
        if lat_raw.ndim == 3 and lon_raw.ndim == 3:
            # Take the first time slice and convert to 2D
            lat_2d = lat_raw[0, :, :]  # Remove time dimension
            lon_2d = lon_raw[0, :, :]  # Remove time dimension
        elif lat_raw.ndim == 2 and lon_raw.ndim == 2:
            # Already 2D
            lat_2d = lat_raw
            lon_2d = lon_raw
        else:
            # 1D coordinates - create meshgrid
            lat_2d, lon_2d = np.meshgrid(lat_raw, lon_raw, indexing='ij')
        
        # Fix longitude coordinates if they cross the 180/-180 boundary
        lon_2d_fixed = lon_2d.copy()
        if lon_2d.min() < 0 and lon_2d.max() > 0:
            # Convert negative longitudes to 0-360 range
            lon_2d_fixed = np.where(lon_2d < 0, lon_2d + 360, lon_2d)
        
        # Use the 2D coordinates
        lat_coords = lat_2d
        lon_coords = lon_2d_fixed
        
        # Create xarray DataArray with proper dimension handling
        data_array = xr.DataArray(
            gas_data_raw,
            coords={'latitude': (['y', 'x'], lat_coords), 'longitude': (['y', 'x'], lon_coords)},
            dims=['y', 'x']
        )
        
        # Helper: haversine distance in km
        def haversine_km(lat1, lon1, lat2, lon2):
            R = 6371.0
            dlat = np.radians(lat2 - lat1)
            dlon = np.radians(lon2 - lon1)
            a = np.sin(dlat / 2.0) ** 2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2.0) ** 2
            return 2 * R * np.arcsin(np.sqrt(a))

        # Compute raw resolution (deg + km) from 2D coordinates using adjacent centers (median)
        # X direction
        lat_mid_x = 0.5 * (lat_coords[:, 1:] + lat_coords[:, :-1])
        lon_x1 = lon_coords[:, :-1]
        lon_x2 = lon_coords[:, 1:]
        km_x = np.nanmedian(haversine_km(lat_mid_x, lon_x1, lat_mid_x, lon_x2))
        deg_x = np.nanmedian(np.abs(lon_coords[:, 1:] - lon_coords[:, :-1]))
        # Y direction
        lat_y1 = lat_coords[:-1, :]
        lat_y2 = lat_coords[1:, :]
        lon_mid_y = 0.5 * (lon_coords[1:, :] + lon_coords[:-1, :])
        km_y = np.nanmedian(haversine_km(lat_y1, lon_mid_y, lat_y2, lon_mid_y))
        deg_y = np.nanmedian(np.abs(lat_coords[1:, :] - lat_coords[:-1, :]))

        # Use pcolormesh like in plot_nc.py
        plot1 = data_array.plot.pcolormesh(
            ax=ax1,
            x='longitude',
            y='latitude',
            add_colorbar=False,
            cmap='jet',
            transform=ccrs.PlateCarree()
        )
        
        # Degree resolution (median)
        lat_res = deg_y
        lon_res = deg_x
        
        ax1.set_title(
            f'Raw Data (No Processing)\n{time_raw.strftime("%Y-%m-%d")}\n'
            f'Shape: {gas_data_raw.shape}\n'
            f'Resolution: {lat_res:.3f}° x {lon_res:.3f}°  |  {km_y:.1f} km x {km_x:.1f} km'
        )
        # Don't set extent - show full raw data range
        ax1.add_feature(cfeature.COASTLINE)
        ax1.add_feature(cfeature.BORDERS)
        ax1.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
        plt.colorbar(plot1, ax=ax1, label=raw_processor.config['units'], shrink=0.8)
        
        # Raw data (zoomed to Taiwan, no processing)
        # Use the same data_array but zoom to Taiwan with same color scale as plot1
        plot2 = data_array.plot.pcolormesh(
            ax=ax2,
            x='longitude',
            y='latitude',
            add_colorbar=False,
            cmap='jet',
            vmin=plot1.get_clim()[0],  # Use same color scale as plot1
            vmax=plot1.get_clim()[1],  # Use same color scale as plot1
            transform=ccrs.PlateCarree()
        )
        ax2.set_extent(extent, crs=ccrs.PlateCarree())
        ax2.set_title(
            f'Raw Data (Zoomed to Taiwan)\n{time_raw.strftime("%Y-%m-%d")}\n'
            f'Shape: {gas_data_raw.shape}\n'
            f'Resolution: {lat_res:.3f}° x {lon_res:.3f}°  |  {km_y:.1f} km x {km_x:.1f} km'
        )
        ax2.add_feature(cfeature.COASTLINE)
        ax2.add_feature(cfeature.BORDERS)
        ax2.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
        plt.colorbar(plot2, ax=ax2, label=raw_processor.config['units'], shrink=0.8)
        
        # Processed data (fast path): directly render using extent without extra computations
        proc_lat_min = np.nanmin(processed_processor.taiwan_lat)
        proc_lat_max = np.nanmax(processed_processor.taiwan_lat)
        proc_lon_min = np.nanmin(processed_processor.taiwan_lon)
        proc_lon_max = np.nanmax(processed_processor.taiwan_lon)

        im3 = ax3.imshow(
            gas_data_processed,
            extent=[proc_lon_min, proc_lon_max, proc_lat_min, proc_lat_max],
            origin='lower',
            transform=ccrs.PlateCarree(),
            cmap='jet',
            aspect='auto'
        )
        ax3.set_title(
            f'Processed Data (from /processed/ folder)\n{time_processed.strftime("%Y-%m-%d")}\n'
            f'Shape: {gas_data_processed.shape}'
        )
        ax3.set_extent(extent, crs=ccrs.PlateCarree())
        ax3.add_feature(cfeature.COASTLINE)
        ax3.add_feature(cfeature.BORDERS)
        ax3.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
        plt.colorbar(im3, ax=ax3, label=processed_processor.config['units'], shrink=0.8)
        
        # Difference plot (fast path): render with imshow and processed extent
        if interpolated_raw.shape == gas_data_processed.shape:
            diff_data = interpolated_raw - gas_data_processed

            im4 = ax4.imshow(
                diff_data,
                extent=[proc_lon_min, proc_lon_max, proc_lat_min, proc_lat_max],
                origin='lower',
                transform=ccrs.PlateCarree(),
                cmap='RdBu_r',
                aspect='auto'
            )
            ax4.set_title('Difference (Raw Interpolated - Processed Data)\nBlue: Raw > Processed, Red: Raw < Processed')
            ax4.set_extent(extent, crs=ccrs.PlateCarree())
            ax4.add_feature(cfeature.COASTLINE)
            ax4.add_feature(cfeature.BORDERS)
            ax4.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
            plt.colorbar(im4, ax=ax4, label=processed_processor.config['units'], shrink=0.8)
        else:
            ax4.text(0.5, 0.5, 'Different dimensions\nCannot compare', ha='center', va='center', 
                    transform=ax4.transAxes)
            ax4.set_title('Difference (Cannot compare)')
        
        plt.tight_layout()
        
        # Save plot with date-specific filename
        date_str = time_raw.strftime('%Y%m%d')
        output_path = processed_processor.output_dir / f"daily_comparison_{date_str}.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"  💾 Day {date_str} comparison plot saved: {output_path}")
        
        plt.show()
        
    except ImportError as e:
        print(f"⚠️  Required libraries not installed: {e}")
        print("Please install: pip install matplotlib cartopy")
    except Exception as e:
        print(f"❌ Error creating comparison plot: {e}")


def create_daily_comparison_plots(raw_processor, processed_processor, raw_files, processed_files, logger):
    """Create detailed comparison plots for each day"""
    
    try:
        import matplotlib.pyplot as plt
        import cartopy.crs as ccrs
        import cartopy.feature as cfeature
        
        n_files = min(len(raw_files), len(processed_files))
        
        for i in range(n_files):
            print(f"Processing day {i+1}/{n_files}...")
            
            # Read raw file
            raw_result = raw_processor._read_raw_file(raw_files[i])
            if raw_result is None:
                print(f"  ❌ Failed to read raw file {i+1}")
                continue
                
            gas_data_raw, lat_data_raw, lon_data_raw, time_raw = raw_result
            
            # Read processed file
            processed_result = processed_processor._read_processed_file(processed_files[i])
            if processed_result is None:
                print(f"  ❌ Failed to read processed file {i+1}")
                continue
                
            gas_data_processed, lat_data_processed, lon_data_processed, time_processed = processed_result
            
            # Interpolate raw data
            interpolated_raw = raw_processor.reproject_to_taiwan_grid(gas_data_raw, lat_data_raw, lon_data_raw)
            
            # Create the enhanced comparison plot for this day
            create_comparison_plots_enhanced(gas_data_raw, interpolated_raw, gas_data_processed,
                                           lat_data_raw, lon_data_raw, lat_data_processed, lon_data_processed,
                                           time_raw, time_processed, raw_processor, processed_processor)
            
            print(f"  ✅ Day {i+1} comparison plot created")
        
        print(f"\n💾 All daily comparison plots saved to: {processed_processor.output_dir}")
        
    except ImportError as e:
        print(f"⚠️  Required libraries not installed: {e}")
        print("Please install: pip install matplotlib cartopy")
    except Exception as e:
        print(f"❌ Error creating daily comparison plots: {e}")


def create_all_files_comparison(raw_processor, processed_processor, raw_files, processed_files, logger):
    """Create comparison for all 11 files"""
    
    try:
        import matplotlib.pyplot as plt
        import cartopy.crs as ccrs
        import cartopy.feature as cfeature
        
        # Create a large figure for all files
        fig = plt.figure(figsize=(24, 20))
        
        # Calculate grid layout (3x4 for 11 files + 1 summary)
        n_files = min(len(raw_files), len(processed_files), 11)
        n_cols = 4
        n_rows = (n_files + n_cols - 1) // n_cols
        
        for i in range(n_files):
            ax = plt.subplot(n_rows, n_cols, i+1, projection=ccrs.PlateCarree())
            
            # Read raw file
            raw_result = raw_processor._read_raw_file(raw_files[i])
            if raw_result is None:
                continue
                
            gas_data_raw, lat_data_raw, lon_data_raw, time_raw = raw_result
            
            # Interpolate raw data
            interpolated_raw = raw_processor.reproject_to_taiwan_grid(gas_data_raw, lat_data_raw, lon_data_raw)
            
            # Read processed file
            processed_result = processed_processor._read_processed_file(processed_files[i])
            if processed_result is None:
                continue
                
            gas_data_processed, lat_data_processed, lon_data_processed, time_processed = processed_result
            
            # Create difference plot
            if interpolated_raw.shape == gas_data_processed.shape:
                diff_data = interpolated_raw - gas_data_processed
                im = ax.imshow(diff_data, extent=[processed_processor.taiwan_lon.min(), processed_processor.taiwan_lon.max(), 
                                                processed_processor.taiwan_lat.min(), processed_processor.taiwan_lat.max()], 
                              origin='lower', transform=ccrs.PlateCarree(), aspect='auto')
            else:
                # If dimensions don't match, show interpolated data
                im = ax.imshow(interpolated_raw, extent=[processed_processor.taiwan_lon.min(), processed_processor.taiwan_lon.max(), 
                                                        processed_processor.taiwan_lat.min(), processed_processor.taiwan_lat.max()], 
                              origin='lower', transform=ccrs.PlateCarree(), aspect='auto')
            
            # Set up map
            extent = [119.0, 123.0, 21.0, 26.0]
            ax.set_extent(extent, crs=ccrs.PlateCarree())
            ax.add_feature(cfeature.COASTLINE)
            ax.add_feature(cfeature.BORDERS)
            ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
            
            # Title
            ax.set_title(f'File {i+1}\n{time_raw.strftime("%Y-%m-%d")}\nDiff: {interpolated_raw.shape} vs {gas_data_processed.shape}')
            
            # Colorbar
            plt.colorbar(im, ax=ax, label=processed_processor.config['units'], shrink=0.6)
        
        plt.suptitle('All Files Comparison: Raw (Interpolated) vs Processed', fontsize=16)
        plt.tight_layout()
        
        # Save plot
        output_path = processed_processor.output_dir / f"all_files_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"\n💾 All files comparison plot saved: {output_path}")
        
        plt.show()
        
    except ImportError as e:
        print(f"⚠️  Required libraries not installed: {e}")
        print("Please install: pip install matplotlib cartopy")
    except Exception as e:
        print(f"❌ Error creating all files comparison: {e}")




if __name__ == "__main__":
    test_interpolation_comparison()
