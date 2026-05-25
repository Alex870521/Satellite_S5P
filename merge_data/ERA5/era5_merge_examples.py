#!/usr/bin/env python3
"""
ERA5 合併工具 - 程式碼範例

簡潔的程式碼範例，詳細說明請參考 README.md
"""

from pathlib import Path
from advanced_era5_merger import AdvancedERA5Merger

# 範例1: 時間合併
def temporal_merge_example():
    """合併多個時間段的檔案"""
    merger = AdvancedERA5Merger()
    
    files = [
        Path("era5_sfc_t2m_20231231_20240630.nc"),
        Path("era5_sfc_t2m_20240630_20241231.nc")
    ]
    output = Path("era5_temporal_merged.nc")
    
    success = merger.merge_multiple_temporal(files, output)
    print("✅ 時間合併" if success else "❌ 時間合併失敗")

# 範例2: 變數合併
def variable_merge_example():
    """合併不同變數的檔案"""
    merger = AdvancedERA5Merger()
    
    files = [
        Path("era5_sfc_d2m_20240101_20241231.nc"),
        Path("era5_sfc_t2m_20240101_20241231.nc")
    ]
    output = Path("era5_variables_merged.nc")
    
    success = merger.merge_multiple_variables(files, output)
    print("✅ 變數合併" if success else "❌ 變數合併失敗")

# 範例3: 檔案分析
def analyze_files_example():
    """分析檔案資訊"""
    merger = AdvancedERA5Merger()
    
    files = [
        Path("era5_sfc_t2m_20231231_20240630.nc"),
        Path("era5_sfc_d2m_20240101_20241231.nc")
    ]
    
    analysis = merger.analyze_files(files)
    print(f"檔案數: {len(analysis['files'])}")
    print(f"變數: {list(analysis['variables'])}")

if __name__ == "__main__":
    print("🌪️ ERA5 合併工具 - 程式碼範例")
    print("=" * 40)
    
    # 取消註解來執行範例
    # temporal_merge_example()
    # variable_merge_example() 
    # analyze_files_example()
    
    print("\n💡 取消註解來執行範例")
    print("詳細說明請參考 README.md")