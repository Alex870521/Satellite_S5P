"""
檢查 ERA5 資料完整性
檢查從 2022 年至今的月份資料是否完整
"""
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# BASE_DIR（去除使用者名稱硬編；如需指向他處請自行調整或設環境變數）
BASE_DIR = Path.home() / "DataCenter" / "Satellite"


def check_era5_completeness(start_year=2022):
    """
    檢查 ERA5 CSV 檔案完整性

    Parameters:
        start_year (int): 開始檢查的年份
    """
    csv_dir = BASE_DIR / "ERA5" / "processed" / "csv"

    if not csv_dir.exists():
        print(f"❌ CSV 目錄不存在: {csv_dir}")
        return

    print("=" * 80)
    print(f"檢查 ERA5 資料完整性 (從 {start_year} 年至今)")
    print("=" * 80)

    # 計算應該有的月份範圍
    start_date = datetime(start_year, 1, 1)
    current_date = datetime.now()

    missing_files = []
    existing_files = []

    # 遍歷每個月
    check_date = start_date
    while check_date <= current_date:
        year = check_date.year
        month = check_date.month

        # 計算該月的最後一天
        next_month = check_date + relativedelta(months=1)
        last_day = (next_month - timedelta(days=1)).day

        # 預期的檔案名稱
        expected_filename = f"blh_{check_date.strftime('%Y%m%d')}_{check_date.strftime('%Y%m')}{last_day:02d}.csv"
        expected_path = csv_dir / str(year) / expected_filename

        # 檢查檔案是否存在
        if expected_path.exists():
            file_size = expected_path.stat().st_size / 1024  # KB
            existing_files.append({
                'year_month': check_date.strftime('%Y-%m'),
                'filename': expected_filename,
                'size_kb': f"{file_size:.1f}",
                'path': expected_path
            })
        else:
            missing_files.append({
                'year_month': check_date.strftime('%Y-%m'),
                'expected_filename': expected_filename,
                'expected_path': expected_path
            })

        # 移到下個月
        check_date = next_month

    # 顯示現有檔案
    print(f"\n✅ 現有檔案: {len(existing_files)} 個月份")
    print("-" * 80)
    for file_info in existing_files[-10:]:  # 只顯示最後10個
        print(f"  {file_info['year_month']}: {file_info['filename']} ({file_info['size_kb']} KB)")

    if len(existing_files) > 10:
        print(f"  ... (共 {len(existing_files)} 個檔案，僅顯示最近10個)")

    # 顯示缺失檔案
    if missing_files:
        print(f"\n❌ 缺失檔案: {len(missing_files)} 個月份")
        print("-" * 80)
        for file_info in missing_files:
            print(f"  {file_info['year_month']}: {file_info['expected_filename']}")
    else:
        print("\n✅ 所有月份檔案都完整！")

    # 檢查額外的檔案（非月份檔案）
    print("\n📋 檢查其他檔案...")
    print("-" * 80)
    all_csv_files = list(csv_dir.rglob("*.csv"))
    year_folders = [f for f in csv_dir.iterdir() if f.is_dir() and f.name.isdigit()]

    expected_files_set = {f['path'] for f in existing_files}
    extra_files = [f for f in all_csv_files if f not in expected_files_set]

    if extra_files:
        print(f"發現 {len(extra_files)} 個非標準月份檔案:")
        for extra_file in extra_files:
            file_size = extra_file.stat().st_size / 1024 / 1024  # MB
            print(f"  {extra_file.relative_to(csv_dir)}: {file_size:.2f} MB")

    # 檢查 NetCDF 原始檔
    print("\n📦 檢查 NetCDF 原始檔...")
    print("-" * 80)
    nc_dir = BASE_DIR / "ERA5" / "raw" / "single_level"
    if nc_dir.exists():
        nc_files = list(nc_dir.rglob("*.nc"))
        for nc_file in nc_files:
            file_size = nc_file.stat().st_size / 1024 / 1024  # MB
            print(f"  {nc_file.relative_to(nc_dir.parent)}: {file_size:.2f} MB")
    else:
        print(f"  ⚠️  NetCDF 目錄不存在: {nc_dir}")

    # 統計摘要
    print("\n" + "=" * 80)
    print("統計摘要")
    print("=" * 80)
    total_months = len(existing_files) + len(missing_files)
    completeness = (len(existing_files) / total_months * 100) if total_months > 0 else 0
    print(f"總計月份: {total_months}")
    print(f"已有資料: {len(existing_files)} 個月")
    print(f"缺失資料: {len(missing_files)} 個月")
    print(f"完整度: {completeness:.1f}%")

    if missing_files:
        print("\n⚠️  建議動作:")
        print("  1. 下載缺失月份的 ERA5 資料")
        print("  2. 處理 NetCDF 檔案產生對應的 CSV")
        print("  3. 提高排程頻率為每週或每日檢查")

    return {
        'existing_files': existing_files,
        'missing_files': missing_files,
        'extra_files': extra_files,
        'completeness': completeness
    }


if __name__ == "__main__":
    check_era5_completeness(start_year=2022)
