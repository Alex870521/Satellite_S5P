#!/usr/bin/env python3
"""
Advanced ERA5 File Merger

This script provides advanced merging capabilities for ERA5 NetCDF files:
1. Multi-file temporal merging (multiple files along time dimension)
2. Multi-variable merging (same time, different variables)
3. Multi-species merging (different pollutants/species)
4. Hybrid merging (combination of above)
"""

import logging
import xarray as xr
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Union, Optional, Tuple
import warnings

from src.config.settings import BASE_DIR

warnings.filterwarnings('ignore')

def setup_logging():
    """Setup logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

class AdvancedERA5Merger:
    """Advanced ERA5 file merger with multiple merge strategies"""
    
    def __init__(self):
        self.logger = setup_logging()
        
    def merge_multiple_temporal(self, file_paths: List[Path], output_path: Path, 
                               remove_duplicates: bool = True,
                               time_dimension: str = 'valid_time') -> bool:
        """
        合併多個檔案沿時間維度
        
        Parameters:
        -----------
        file_paths : List[Path]
            要合併的檔案路徑列表（按時間順序）
        output_path : Path
            輸出檔案路徑
        remove_duplicates : bool
            是否移除重複時間點
        time_dimension : str
            時間維度名稱
        """
        self.logger.info("=== 多檔案時間合併 ===")
        self.logger.info(f"檔案數量: {len(file_paths)}")
        self.logger.info(f"輸出檔案: {output_path}")
        
        # 檢查所有檔案是否存在
        for i, file_path in enumerate(file_paths):
            if not file_path.exists():
                self.logger.error(f"檔案 {i+1} 不存在: {file_path}")
                return False
        
        try:
            # 開啟所有資料集
            datasets = []
            time_ranges = []
            
            self.logger.info("開啟資料集...")
            for i, file_path in enumerate(file_paths):
                self.logger.info(f"開啟檔案 {i+1}: {file_path.name}")
                ds = xr.open_dataset(file_path)
                datasets.append(ds)
                
                # 記錄時間範圍和空間維度
                time_min = ds[time_dimension].min().values
                time_max = ds[time_dimension].max().values
                time_ranges.append((time_min, time_max, file_path.name))
                self.logger.info(f"  時間範圍: {time_min} 到 {time_max}")
                self.logger.info(f"  變數: {list(ds.data_vars.keys())}")
                self.logger.info(f"  空間維度: lat={len(ds.latitude)}, lon={len(ds.longitude)}")
            
            # 按時間順序重新排列資料集
            self.logger.info("按時間順序重新排列資料集...")
            sorted_indices = sorted(range(len(time_ranges)), key=lambda i: time_ranges[i][0])
            datasets = [datasets[i] for i in sorted_indices]
            time_ranges = [time_ranges[i] for i in sorted_indices]
            
            self.logger.info("重新排列後的順序:")
            for i, (time_min, time_max, filename) in enumerate(time_ranges):
                self.logger.info(f"  {i+1}. {filename}: {time_min} 到 {time_max}")
            
            # 檢查空間維度一致性
            self.logger.info("檢查空間維度一致性...")
            lat_sizes = [len(ds.latitude) for ds in datasets]
            lon_sizes = [len(ds.longitude) for ds in datasets]
            
            if len(set(lat_sizes)) > 1 or len(set(lon_sizes)) > 1:
                self.logger.warning(f"發現不同的空間維度大小:")
                self.logger.warning(f"  緯度大小: {lat_sizes}")
                self.logger.warning(f"  經度大小: {lon_sizes}")
                self.logger.warning("嘗試重新投影到統一的空間網格...")
                
                # 找到最大的空間維度作為目標
                target_lat_size = max(lat_sizes)
                target_lon_size = max(lon_sizes)
                
                # 重新投影所有資料集到統一的空間網格
                reprojected_datasets = []
                for i, ds in enumerate(datasets):
                    if len(ds.latitude) != target_lat_size or len(ds.longitude) != target_lon_size:
                        self.logger.info(f"重新投影檔案 {i+1}...")
                        ds_reprojected = self._reproject_to_uniform_grid(ds, target_lat_size, target_lon_size)
                        reprojected_datasets.append(ds_reprojected)
                    else:
                        reprojected_datasets.append(ds)
                
                datasets = reprojected_datasets
            
            # 檢查時間重疊
            if remove_duplicates:
                self.logger.info("檢查時間重疊...")
                cleaned_datasets = []
                for i, ds in enumerate(datasets):
                    if i == 0:
                        cleaned_datasets.append(ds)
                        self.logger.info(f"檔案 {i+1} 作為基準檔案，時間點: {len(ds[time_dimension])}")
                    else:
                        # 檢查與之前所有檔案的累積時間範圍重疊
                        # 計算所有之前檔案的累積最大時間
                        cumulative_max_time = None
                        for j in range(i):
                            if cumulative_max_time is None:
                                cumulative_max_time = cleaned_datasets[j][time_dimension].max()
                            else:
                                current_max = cleaned_datasets[j][time_dimension].max()
                                if current_max > cumulative_max_time:
                                    cumulative_max_time = current_max
                        
                        self.logger.info(f"檔案 {i+1} 時間範圍: {ds[time_dimension].min().values} 到 {ds[time_dimension].max().values}")
                        self.logger.info(f"累積最大時間: {cumulative_max_time.values}")
                        
                        # 移除與累積時間範圍重疊的時間
                        time_mask = ds[time_dimension] > cumulative_max_time
                        if time_mask.any():
                            ds_clean = ds.sel({time_dimension: time_mask})
                            cleaned_datasets.append(ds_clean)
                            self.logger.info(f"檔案 {i+1} 清理後時間點: {len(ds_clean[time_dimension])}")
                        else:
                            self.logger.warning(f"檔案 {i+1} 的所有時間都在重疊範圍內，跳過此檔案")
                
                datasets = cleaned_datasets
            
            # 沿時間維度合併
            self.logger.info("沿時間維度合併...")
            merged_ds = xr.concat(datasets, dim=time_dimension)
            
            # 按時間排序
            merged_ds = merged_ds.sortby(time_dimension)
            
            # 檢查重複時間點
            time_coords = pd.to_datetime(merged_ds[time_dimension].values)
            if len(time_coords) != len(time_coords.unique()):
                self.logger.warning("仍存在重複時間點!")
                if remove_duplicates:
                    merged_ds = merged_ds.drop_duplicates(dim=time_dimension, keep='first')
                    self.logger.info(f"移除重複後時間點數量: {len(merged_ds[time_dimension])}")
            
            # 儲存合併結果
            output_path.parent.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"儲存合併檔案: {output_path}")
            merged_ds.to_netcdf(output_path)
            
            # 關閉所有資料集
            for ds in datasets:
                ds.close()
            merged_ds.close()
            
            self.logger.info("✅ 多檔案時間合併完成!")
            return True
            
        except Exception as e:
            self.logger.error(f"合併失敗: {e}")
            return False
    
    def merge_multiple_variables(self, file_paths: List[Path], output_path: Path,
                                variable_mapping: Optional[Dict[str, str]] = None) -> bool:
        """
        合併同時間不同變數的檔案
        
        Parameters:
        -----------
        file_paths : List[Path]
            包含不同變數的檔案路徑列表
        output_path : Path
            輸出檔案路徑
        variable_mapping : Dict[str, str], optional
            變數名稱映射 {新名稱: 舊名稱}
        """
        self.logger.info("=== 多變數合併 ===")
        self.logger.info(f"檔案數量: {len(file_paths)}")
        
        try:
            # 開啟所有資料集
            datasets = []
            all_variables = set()
            
            for i, file_path in enumerate(file_paths):
                self.logger.info(f"開啟檔案 {i+1}: {file_path.name}")
                ds = xr.open_dataset(file_path)
                datasets.append(ds)
                
                variables = list(ds.data_vars.keys())
                all_variables.update(variables)
                self.logger.info(f"  變數: {variables}")
                self.logger.info(f"  時間範圍: {ds.valid_time.min().values} 到 {ds.valid_time.max().values}")
            
            # 檢查時間維度一致性
            time_dims = [ds.valid_time for ds in datasets]
            for i, time_dim in enumerate(time_dims[1:], 1):
                if not np.array_equal(time_dims[0].values, time_dim.values):
                    self.logger.warning(f"檔案 {i+1} 的時間維度與檔案 1 不一致")
            
            # 合併資料集（沿變數維度）
            self.logger.info("合併變數...")
            merged_ds = xr.merge(datasets)
            
            # 應用變數名稱映射
            if variable_mapping:
                self.logger.info("應用變數名稱映射...")
                merged_ds = merged_ds.rename(variable_mapping)
            
            # 儲存結果
            output_path.parent.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"儲存合併檔案: {output_path}")
            merged_ds.to_netcdf(output_path)
            
            # 關閉資料集
            for ds in datasets:
                ds.close()
            merged_ds.close()
            
            self.logger.info("✅ 多變數合併完成!")
            return True
            
        except Exception as e:
            self.logger.error(f"合併失敗: {e}")
            return False
    
    def merge_multiple_species(self, file_paths: List[Path], output_path: Path,
                              species_mapping: Optional[Dict[str, str]] = None) -> bool:
        """
        合併多個物種/污染物的檔案
        
        Parameters:
        -----------
        file_paths : List[Path]
            包含不同物種的檔案路徑列表
        output_path : Path
            輸出檔案路徑
        species_mapping : Dict[str, str], optional
            物種名稱映射 {新名稱: 舊名稱}
        """
        self.logger.info("=== 多物種合併 ===")
        
        # 這實際上與多變數合併類似，但針對污染物物種進行優化
        return self.merge_multiple_variables(file_paths, output_path, species_mapping)
    
    def merge_hybrid(self, file_groups: Dict[str, List[Path]], output_path: Path,
                     merge_strategy: str = "temporal") -> bool:
        """
        混合合併策略：先按組別合併，再合併不同組別
        
        Parameters:
        -----------
        file_groups : Dict[str, List[Path]]
            檔案分組 {組別名稱: 檔案列表}
        output_path : Path
            輸出檔案路徑
        merge_strategy : str
            合併策略: "temporal", "variable", "species"
        """
        self.logger.info("=== 混合合併策略 ===")
        self.logger.info(f"組別數量: {len(file_groups)}")
        self.logger.info(f"合併策略: {merge_strategy}")
        
        try:
            group_results = []
            
            # 先合併每個組別內的檔案
            for group_name, file_list in file_groups.items():
                self.logger.info(f"處理組別: {group_name} ({len(file_list)} 個檔案)")
                
                # 建立臨時輸出檔案
                temp_output = output_path.parent / f"temp_{group_name}.nc"
                
                if merge_strategy == "temporal":
                    success = self.merge_multiple_temporal(file_list, temp_output)
                elif merge_strategy == "variable":
                    success = self.merge_multiple_variables(file_list, temp_output)
                else:
                    success = self.merge_multiple_species(file_list, temp_output)
                
                if success:
                    group_results.append(temp_output)
                else:
                    self.logger.error(f"組別 {group_name} 合併失敗")
                    return False
            
            # 再合併不同組別的結果
            self.logger.info("合併不同組別...")
            if merge_strategy == "temporal":
                final_success = self.merge_multiple_temporal(group_results, output_path)
            else:
                final_success = self.merge_multiple_variables(group_results, output_path)
            
            # 清理臨時檔案
            for temp_file in group_results:
                if temp_file.exists():
                    temp_file.unlink()
                    self.logger.info(f"清理臨時檔案: {temp_file}")
            
            if final_success:
                self.logger.info("✅ 混合合併完成!")
            
            return final_success
            
        except Exception as e:
            self.logger.error(f"混合合併失敗: {e}")
            return False
    
    def analyze_files(self, file_paths: List[Path]) -> Dict:
        """分析檔案資訊"""
        analysis = {
            'files': [],
            'variables': set(),
            'time_ranges': [],
            'dimensions': set()
        }
        
        for i, file_path in enumerate(file_paths):
            if not file_path.exists():
                continue
                
            try:
                ds = xr.open_dataset(file_path)
                file_info = {
                    'index': i,
                    'path': file_path,
                    'name': file_path.name,
                    'variables': list(ds.data_vars.keys()),
                    'dimensions': dict(ds.dims),
                    'time_min': ds.valid_time.min().values if 'valid_time' in ds else None,
                    'time_max': ds.valid_time.max().values if 'valid_time' in ds else None,
                    'time_points': len(ds.valid_time) if 'valid_time' in ds else 0
                }
                
                analysis['files'].append(file_info)
                analysis['variables'].update(file_info['variables'])
                analysis['dimensions'].update(file_info['dimensions'].keys())
                
                if file_info['time_min']:
                    analysis['time_ranges'].append((file_info['time_min'], file_info['time_max']))
                
                ds.close()
                
            except Exception as e:
                self.logger.error(f"分析檔案 {file_path} 失敗: {e}")
        
        return analysis
    
    def _reproject_to_uniform_grid(self, ds: xr.Dataset, target_lat_size: int, target_lon_size: int) -> xr.Dataset:
        """重新投影資料集到統一的空間網格"""
        try:
            # 獲取目標網格座標
            target_lat = np.linspace(ds.latitude.min(), ds.latitude.max(), target_lat_size)
            target_lon = np.linspace(ds.longitude.min(), ds.longitude.max(), target_lon_size)
            
            # 使用 xarray 的 interp 方法進行插值
            ds_interp = ds.interp(latitude=target_lat, longitude=target_lon, method='linear')
            
            self.logger.info(f"重新投影完成: {len(ds.latitude)}x{len(ds.longitude)} -> {target_lat_size}x{target_lon_size}")
            return ds_interp
            
        except Exception as e:
            self.logger.error(f"重新投影失敗: {e}")
            # 如果插值失敗，嘗試使用最近鄰方法
            try:
                ds_interp = ds.interp(latitude=target_lat, longitude=target_lon, method='nearest')
                self.logger.warning("使用最近鄰插值方法")
                return ds_interp
            except Exception as e2:
                self.logger.error(f"最近鄰插值也失敗: {e2}")
                return ds

def interactive_file_selection() -> Tuple[List[Path], str, Path]:
    """互動式檔案選擇"""
    print("\n🌪️ 進階 ERA5 檔案合併工具")
    print("=" * 60)

    period = '20240101_20241231'

    # 預設檔案範例
    home_sl = Path.home() / "DataCenter/Satellite/ERA5/raw/single_level"
    base_sl = BASE_DIR / "ERA5/raw/single_level"
    default_groups = {
        "時間序列合併": {
            "description": "合併多個時間段的檔案",
            "files": [
                str(home_sl / "era5_sfc_t2m_20231231_20240630.nc"),
                str(home_sl / "era5_sfc_t2m_20240630_20241231.nc"),
            ]
        },
        "多變數ERA5合併": {
            "description": "合併不同ERA5變數的檔案",
            "files": [
                str(base_sl / f"era5_sfc_u10_v10_{period}.nc"),
                str(base_sl / f"era5_sfc_d2m_t2m_{period}.nc"),
                str(base_sl / f"era5_sfc_r2m_{period}.nc"),
                str(base_sl / f"era5_sfc_tcwv_sp_{period}.nc"),
                str(base_sl / f"era5_sfc_blh_{period}.nc"),
            ]
        }
    }
    
    print("\n選擇合併類型:")
    for i, (name, info) in enumerate(default_groups.items(), 1):
        print(f"  {i}. {name}")
        print(f"     {info['description']}")
    print("  0. 自訂檔案路徑")
    
    try:
        choice = input("\n請選擇選項 (0-3): ").strip()
        
        if choice == "0":
            # 自訂檔案路徑
            file_paths = []
            print("\n請輸入檔案路徑 (每行一個，輸入空行結束):")
            while True:
                path = input().strip()
                if not path:
                    break
                file_paths.append(Path(path))
            
            if not file_paths:
                print("未輸入任何檔案路徑!")
                return [], "", Path("")
            
            merge_type = input("選擇合併類型 (temporal/variable): ").strip().lower()
            output_name = input("輸入輸出檔案名稱: ").strip()
            
        elif choice in ["1", "2", "3"]:
            idx = int(choice) - 1
            group_name = list(default_groups.keys())[idx]
            file_paths = [Path(f) for f in default_groups[group_name]["files"]]
            
            if choice == "1":
                merge_type = "temporal"
                output_name = "era5_multi_temporal_merged.nc"
            else:
                merge_type = "variable"
                output_name = "era5_multi_variable_merged.nc"
            
            print(f"已選擇: {group_name}")
        else:
            print("無效選項，使用預設設定")
            file_paths = [Path(f) for f in default_groups["時間序列合併"]["files"]]
            merge_type = "temporal"
            output_name = "era5_default_merged.nc"
        
        # 設定輸出路徑
        output_dir = Path.home() / "Desktop"
        output_path = output_dir / output_name
        
        return file_paths, merge_type, output_path
        
    except Exception as e:
        print(f"輸入錯誤: {e}")
        return [], "", Path("")

def main():
    """主函數"""
    merger = AdvancedERA5Merger()
    
    # 互動式檔案選擇
    file_paths, merge_type, output_path = interactive_file_selection()
    
    if not file_paths or not merge_type:
        print("❌ 檔案選擇失敗!")
        return
    
    print(f"\n📁 選擇的檔案 ({len(file_paths)} 個):")
    for i, path in enumerate(file_paths, 1):
        print(f"  {i}. {path.name}")
    print(f"🔄 合併類型: {merge_type}")
    print(f"💾 輸出檔案: {output_path}")
    
    # 分析檔案
    print("\n🔍 分析檔案...")
    analysis = merger.analyze_files(file_paths)
    
    print(f"📊 檔案分析結果:")
    print(f"  總檔案數: {len(analysis['files'])}")
    print(f"  總變數數: {len(analysis['variables'])}")
    print(f"  變數列表: {list(analysis['variables'])}")
    print(f"  維度: {list(analysis['dimensions'])}")
    
    # 執行合併
    print(f"\n🚀 開始合併...")
    
    if merge_type == "temporal":
        success = merger.merge_multiple_temporal(file_paths, output_path)
    elif merge_type == "variable":
        success = merger.merge_multiple_variables(file_paths, output_path)
    else:
        print(f"❌ 不支援的合併類型: {merge_type}")
        return
    
    if success:
        print(f"\n✅ 合併成功! 輸出檔案: {output_path}")
        
        # 驗證結果
        print("\n🔍 驗證合併結果...")
        try:
            ds = xr.open_dataset(output_path)
            print(f"  時間點數量: {len(ds.valid_time)}")
            print(f"  變數數量: {len(ds.data_vars)}")
            print(f"  時間範圍: {ds.valid_time.min().values} 到 {ds.valid_time.max().values}")
            print(f"  變數列表: {list(ds.data_vars.keys())}")
            ds.close()
        except Exception as e:
            print(f"  驗證失敗: {e}")
    else:
        print(f"\n❌ 合併失敗!")

if __name__ == "__main__":
    main()
