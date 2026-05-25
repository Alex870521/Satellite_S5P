from dataclasses import dataclass
from typing import Literal, TypeAlias, Optional, Tuple, get_args
from enum import Enum

# =============================================================================
# 產品類型常量定義 (單一數據源，易於維護)
# =============================================================================

# 產品類型常量
PRODUCT_TYPES = (
    'O3____', 'O3_TCL', 'O3__PR', 'CH4___', 'CO____', 
    'NO2___', 'HCHO__', 'SO2___', 'CLOUD_', 'FRESCO', 
    'AER_LH', 'AER_AI'
)

# 產品級別常量
PRODUCT_LEVELS = ('L0__', 'L1B_', 'L2__')

# 產品類別常量
PRODUCT_CLASSES = ('NRTI', 'OFFL', 'RPRO', 'TEST', 'OGCA', 'GSOV', 'OPER')

# 從常量創建 Literal 類型 (提供 IDE 自動完成和類型檢查)
ProductTypeLiteral = Literal[PRODUCT_TYPES]
ProductLevelLiteral = Literal[PRODUCT_LEVELS]
ProductClassLiteral = Literal[PRODUCT_CLASSES]

# 向後兼容的類型別名
ClassInput = ProductClassLiteral
TypeInput = ProductTypeLiteral


@dataclass
class ProductConfig:
    """產品配置"""
    display_name: str      # 顯示名稱（帶下標）
    dataset_name: str      # 數據集名稱
    vmin: float | None     # 最小值
    vmax: float | None     # 最大值
    units: str             # 單位
    title: str             # 標題
    cmap: str = 'viridis'  # 預設色階
    # 新增：網格解析度配置（基於 Sentinel-5P 2019/8/6 後官方規格）
    resolution: Optional[Tuple[float, float]] = None  # (x_km, y_km) 解析度，None 表示使用預設值


class ProductType(str, Enum):
    """
    Sentinel-5P 產品類型枚舉 (基於常量自動生成)
    
    這個枚舉定義了所有可用的 Sentinel-5P 產品類型，用於 API 查詢和數據處理。
    每個產品類型對應特定的氣體或參數測量。
    
    產品分類：
    - 臭氧產品：O₃ 總柱、對流層柱、剖面
    - 氣體產品：NO₂, SO₂, CO, CH₄, HCHO
    - 雲和氣溶膠產品：雲掩膜、氣溶膠指數等
    
    使用範例：
        >>> ProductType.NO2___  # NO₂ 對流層柱
        >>> ProductType.O3____  # O₃ 總柱
        >>> ProductType.CH4___  # CH₄ 混合比
    
    注意：此枚舉基於 PRODUCT_TYPES 常量自動生成，確保與 Literal 類型一致。
    """
    # 從常量自動生成枚舉值
    O3____ = 'O3____'   # O₃ 總垂直柱 (Total Vertical Column)
    O3_TCL = 'O3_TCL'   # O₃ 對流層柱 (Tropospheric Column) - 注意：可能無數據
    O3__PR = 'O3__PR'   # O₃ 剖面 (Profile) - 2022年後有 total_vertical_columns 和 tropospheric_column
    CH4___ = 'CH4___'   # CH₄ 甲烷混合比 (Methane Mixing Ratio)
    CO____ = 'CO____'   # CO 總柱 (Total Column)
    NO2___ = 'NO2___'   # NO₂ 對流層柱 (Tropospheric Column)
    HCHO__ = 'HCHO__'   # HCHO 甲醛對流層垂直柱 (Tropospheric Vertical Column)
    SO2___ = 'SO2___'   # SO₂ 總垂直柱 (Total Vertical Column)
    CLOUD_ = 'CLOUD_'   # 雲掩膜 (Cloud Mask)
    FRESCO = 'FRESCO'   # FRESCO 雲產品
    AER_LH = 'AER_LH'   # 氣溶膠層高 (Aerosol Layer Height)
    AER_AI = 'AER_AI'   # 氣溶膠指數 (Aerosol Index)
    
    @classmethod
    def get_all_values(cls) -> tuple[str, ...]:
        """獲取所有產品類型值"""
        return PRODUCT_TYPES
    
    @classmethod
    def validate(cls, value: str) -> bool:
        """驗證值是否為有效的產品類型"""
        return value in PRODUCT_TYPES


class ProductLevel(str, Enum):
    """
    Sentinel-5P 產品處理級別枚舉 (基於常量自動生成)
    
    定義了 Sentinel-5P 數據的不同處理級別，從原始數據到最終產品。
    
    級別說明：
    - L0__: Level 0 - 原始輻射數據 (Raw radiance data)
    - L1B_: Level 1B - 校準後的輻射數據 (Calibrated radiance data)
    - L2__: Level 2 - 地球物理參數產品 (Geophysical parameters)
    
    通常用於數據處理的是 L2 級別產品，包含反演的大氣成分濃度。
    
    使用範例：
        >>> ProductLevel.Level2  # Level 2 產品
        >>> ProductLevel.Level1B  # Level 1B 產品
    
    注意：此枚舉基於 PRODUCT_LEVELS 常量自動生成，確保與 Literal 類型一致。
    """
    Level0 = 'L0__'   # Level 0 - 原始輻射數據
    Level1B = 'L1B_'  # Level 1B - 校準後的輻射數據
    Level2 = 'L2__'   # Level 2 - 地球物理參數產品
    
    @classmethod
    def get_all_values(cls) -> tuple[str, ...]:
        """獲取所有產品級別值"""
        return PRODUCT_LEVELS
    
    @classmethod
    def validate(cls, value: str) -> bool:
        """驗證值是否為有效的產品級別"""
        return value in PRODUCT_LEVELS


class ProductClass(str, Enum):
    """
    Sentinel-5P 產品處理類別枚舉 (基於常量自動生成)
    
    定義了 Sentinel-5P 數據的不同處理類別，影響數據的可用性、精度和處理時間。
    
    主要處理類別：
    - NRTI: 近實時處理 (Near-real time) - 最快但精度較低，適合監測
    - OFFL: 離線處理 (Offline) - 標準處理，平衡速度和精度
    - RPRO: 重新處理 (Reprocessing) - 最高精度，用於歷史數據
    
    測試類別：
    - TEST: 內部測試
    - OGCA: 地面校準 (On-ground calibration)
    - GSOV: 地面段驗證 (Ground segment validation)
    - OPER: 操作處理 (Operational processing)
    
    建議使用：
    - 科學研究：優先使用 RPRO (最高精度)
    - 實時監測：使用 NRTI (最快可用)
    - 一般應用：使用 OFFL (平衡選擇)
    
    使用範例：
        >>> ProductClass.OFFL  # 標準離線處理
        >>> ProductClass.RPRO  # 重新處理 (最高精度)
    
    注意：此枚舉基於 PRODUCT_CLASSES 常量自動生成，確保與 Literal 類型一致。
    """
    # Main processing types (主要處理類別)
    NRTI = 'NRTI'  # 近實時處理 (Near-real time) - 最快但精度較低
    OFFL = 'OFFL'  # 離線處理 (Offline) - 標準處理，平衡速度和精度
    RPRO = 'RPRO'  # 重新處理 (Reprocessing) - 最高精度，用於歷史數據

    # Testing types (測試類別)
    TEST = 'TEST'  # 內部測試 (Internal testing)
    OGCA = 'OGCA'  # 地面校準 (On-ground calibration)
    GSOV = 'GSOV'  # 地面段驗證 (Ground segment validation)
    OPER = 'OPER'  # 操作處理 (Operational processing)
    
    @classmethod
    def get_all_values(cls) -> tuple[str, ...]:
        """獲取所有產品類別值"""
        return PRODUCT_CLASSES
    
    @classmethod
    def validate(cls, value: str) -> bool:
        """驗證值是否為有效的產品類別"""
        return value in PRODUCT_CLASSES


# =============================================================================
# Sentinel-5P 產品解析度配置表 (基於 2019/8/6 後官方規格)
# =============================================================================
# 
# 產品類型          | 解析度 (km)    | 產品名稱                    | 說明
# ------------------|---------------|----------------------------|------------------
# NO2___           | 5.5 × 3.5     | NO₂ 對流層柱               | 高解析度，適合精細分析
# O3____           | 5.5 × 3.5     | O₃ 總垂直柱               | 高解析度，適合精細分析
# SO2___           | 5.5 × 3.5     | SO₂ 總垂直柱              | 高解析度，適合精細分析
# HCHO__           | 5.5 × 3.5     | HCHO 對流層垂直柱         | 高解析度，適合精細分析
# AER_AI           | 5.5 × 3.5     | 氣溶膠指數                | 高解析度，適合精細分析
# AER_LH           | 5.5 × 3.5     | 氣溶膠層高                | 高解析度，適合精細分析
# O3_TCL           | 5.5 × 3.5     | O₃ 對流層柱              | 高解析度，適合精細分析
# ------------------|---------------|----------------------------|------------------
# CO____           | 5.5 × 7.0     | CO 總柱                  | 中等解析度，經度精細
# CH4___           | 5.5 × 7.0     | CH₄ 甲烷混合比           | 中等解析度，經度精細
# ------------------|---------------|----------------------------|------------------
# O3__PR           | 30.0 × 30.0   | O₃ 剖面                  | 低解析度，大範圍觀測
# 
# 解析度分類：
# - 高解析度 (5.5×3.5 km): 6種產品 - 適合城市級別的精細分析
# - 中等解析度 (5.5×7.0 km): 2種產品 - 平衡精度和覆蓋範圍
# - 低解析度 (30×30 km): 1種產品 - 適合大範圍區域分析
# 
# 使用建議：
# - 城市空氣品質監測：優先使用高解析度產品 (NO₂, O₃, SO₂)
# - 區域性研究：可使用中等解析度產品 (CO, CH₄)
# - 全球性分析：可使用低解析度產品 (O₃ 剖面)
# =============================================================================

PRODUCT_CONFIGS: dict[str, ProductConfig] = {
    'O3____': ProductConfig(
        display_name='O\u2083',
        dataset_name='ozone_total_vertical_column',
        vmin=0.1, vmax=0.11,
        units=f'O$_3$ Total Vertical Column (mol/m$^2$)',
        title=f'O$_3$ Total Vertical Column',
        cmap='RdBu_r',
        resolution=(5.5, 3.5)  # O₃ (total): 5.5km x 3.5km
    ),
    'O3_TCL': ProductConfig(
        display_name='O\u2083_TCL',
        dataset_name='ozone_tropospheric_column',
        vmin=0.1, vmax=0.2,
        units=f'O$_3$ Tropospheric Column (mol/m$^2$)',
        title=f'O$_3$ Tropospheric Column',
        cmap='RdBu_r',
        resolution=(5.5, 3.5)  # O₃ (total): 5.5km x 3.5km
    ),
    'O3__PR': ProductConfig(
        display_name='O\u2083_PR',
        dataset_name='ozone_tropospheric_column',
        vmin=0.01, vmax=0.04,
        units=f'O$_3$ Tropospheric Column (mol/m$^2$)',
        title=f'O$_3$ Tropospheric Column',
        cmap='jet',
        resolution=(30.0, 30.0)  # O₃ (profile): 30km x 30km
    ),
    'CH4___': ProductConfig(
        display_name='CH\u2084',
        dataset_name='methane_mixing_ratio',
        vmin=None, vmax=None,
        units=f'CH$_4$ Methane Mixing Ratio',
        title=f'CH$_4$ Methane Mixing Ratio',
        cmap='RdBu_r',
        resolution=(5.5, 7.0)  # CH₄: 5.5km x 7km
    ),
    'CO____': ProductConfig(
        display_name='CO',
        dataset_name='carbonmonoxide_total_column',
        vmin=None, vmax=None,
        units=f'CO Total Column (mol/m$^2$)',
        title=f'CO Total Column',
        cmap='RdBu_r',
        resolution=(5.5, 7.0)  # CO: 5.5km x 7km
    ),
    'NO2___': ProductConfig(
        display_name='NO\u2082',
        dataset_name='nitrogendioxide_tropospheric_column',
        vmin=-2e-4, vmax=2e-4,
        units=f'NO$_2$ Tropospheric Column (mol/m$^2$)',
        title=f'NO$_2$ Tropospheric Column',
        cmap='RdBu_r',
        resolution=(5.5, 3.5)  # NO₂: 5.5km x 3.5km
    ),
    'HCHO__': ProductConfig(
        display_name='HCHO',
        dataset_name='formaldehyde_tropospheric_vertical_column',
        vmin=-4e-4, vmax=4e-4,
        units=f'HCHO Tropospheric Vertical Column (mol/m$^2$)',
        title=f'HCHO Tropospheric Vertical Column',
        cmap='RdBu_r',
        resolution=(5.5, 3.5)  # HCHO: 5.5km x 3.5km
    ),
    'SO2___': ProductConfig(
        display_name='SO\u2082',
        dataset_name='sulfurdioxide_total_vertical_column',
        vmin=-4e-3, vmax=4e-3,
        units=f'SO$_2$ Total Vertical Column (mol/m$^2$)',
        title=f'SO$_2$ Total Vertical Column',
        cmap='RdBu_r',
        resolution=(5.5, 3.5)  # SO₂: 5.5km x 3.5km
    ),
    'AER_AI': ProductConfig(
        display_name='Aerosol Index',
        dataset_name='aerosol_index_340_380',
        vmin=None, vmax=None,
        units=f'Aerosol Index',
        title=f'Aerosol Index',
        cmap='viridis',
        resolution=(5.5, 3.5)  # AER (AI): 5.5km x 3.5km
    )
}


def get_resolution_for_product(product_type: str) -> Tuple[float, float]:
    """
    從產品配置中獲取解析度
    
    Parameters:
    -----------
    product_type : str
        產品類型，例如 'NO2___', 'O3____' 等
        
    Returns:
    --------
    Tuple[float, float]
        解析度 (x_km, y_km)，如果未找到則返回預設值 (5.5, 3.5)
    """
    if product_type in PRODUCT_CONFIGS:
        config = PRODUCT_CONFIGS[product_type]
        if config.resolution is not None:
            return config.resolution
    
    # 預設解析度
    return (5.5, 3.5)


def get_all_resolution_configs() -> dict[str, Tuple[float, float]]:
    """
    獲取所有產品的解析度配置
    
    Returns:
    --------
    dict[str, Tuple[float, float]]
        產品類型到解析度的映射
    """
    return {
        product_type: get_resolution_for_product(product_type)
        for product_type in PRODUCT_CONFIGS.keys()
    }


def get_product_info(product_type: str) -> dict:
    """
    獲取產品的完整信息
    
    Parameters:
    -----------
    product_type : str
        產品類型，例如 'NO2___', 'O3____' 等
        
    Returns:
    --------
    dict
        包含產品完整信息的字典
    """
    if product_type not in PRODUCT_CONFIGS:
        return {"error": f"未找到產品類型: {product_type}"}
    
    config = PRODUCT_CONFIGS[product_type]
    return {
        "product_type": product_type,
        "display_name": config.display_name,
        "dataset_name": config.dataset_name,
        "units": config.units,
        "title": config.title,
        "vmin": config.vmin,
        "vmax": config.vmax,
        "cmap": config.cmap,
        "resolution": config.resolution or get_resolution_for_product(product_type)
    }


def list_available_products() -> dict[str, list[str]]:
    """
    列出所有可用的產品，按類別分組
    
    Returns:
    --------
    dict[str, list[str]]
        按類別分組的產品列表
    """
    return {
        "臭氧產品": [ProductType.O3____, ProductType.O3_TCL, ProductType.O3__PR],
        "氣體產品": [ProductType.NO2___, ProductType.SO2___, ProductType.CO____, 
                    ProductType.CH4___, ProductType.HCHO__],
        "雲和氣溶膠產品": [ProductType.CLOUD_, ProductType.FRESCO, 
                        ProductType.AER_LH, ProductType.AER_AI]
    }


def get_processing_recommendations() -> dict[str, str]:
    """
    獲取不同使用場景的處理建議
    
    Returns:
    --------
    dict[str, str]
        使用場景到建議的映射
    """
    return {
        "科學研究": f"建議使用 {ProductClass.RPRO} (重新處理) - 最高精度",
        "實時監測": f"建議使用 {ProductClass.NRTI} (近實時) - 最快可用",
        "一般應用": f"建議使用 {ProductClass.OFFL} (離線處理) - 平衡選擇",
        "數據驗證": f"建議使用 {ProductClass.GSOV} (地面段驗證)",
        "校準測試": f"建議使用 {ProductClass.OGCA} (地面校準)"
    }


def get_literal_values() -> dict[str, tuple[str, ...]]:
    """
    獲取所有 Literal 類型的值
    
    Returns:
    --------
    dict[str, tuple[str, ...]]
        類型名稱到值的映射
    """
    return {
        "ProductTypeLiteral": get_args(ProductTypeLiteral),
        "ProductLevelLiteral": get_args(ProductLevelLiteral),
        "ProductClassLiteral": get_args(ProductClassLiteral),
    }


def validate_inputs(product_type: str, product_level: str, product_class: str) -> dict[str, bool]:
    """
    驗證輸入值是否有效
    
    Parameters:
    -----------
    product_type : str
        產品類型
    product_level : str
        產品級別
    product_class : str
        產品類別
        
    Returns:
    --------
    dict[str, bool]
        驗證結果
    """
    return {
        "product_type": ProductType.validate(product_type),
        "product_level": ProductLevel.validate(product_level),
        "product_class": ProductClass.validate(product_class),
    }


# =============================================================================
# 詳細解析度對照表
# =============================================================================
# 
# 高解析度產品 (5.5 × 3.5 km) - 適合城市級別分析
# ┌─────────────┬──────────────┬─────────────────────────┬─────────────────┐
# │ 產品代碼    │ 解析度 (km)  │ 產品名稱                │ 主要應用        │
# ├─────────────┼──────────────┼─────────────────────────┼─────────────────┤
# │ NO2___      │ 5.5 × 3.5    │ NO₂ 對流層柱           │ 城市空氣品質    │
# │ O3____      │ 5.5 × 3.5    │ O₃ 總垂直柱           │ 臭氧監測        │
# │ SO2___      │ 5.5 × 3.5    │ SO₂ 總垂直柱          │ 工業污染監測    │
# │ HCHO__      │ 5.5 × 3.5    │ HCHO 對流層垂直柱     │ 揮發性有機物    │
# │ AER_AI      │ 5.5 × 3.5    │ 氣溶膠指數            │ 沙塵暴監測      │
# │ AER_LH      │ 5.5 × 3.5    │ 氣溶膠層高           │ 大氣層結構      │
# │ O3_TCL      │ 5.5 × 3.5    │ O₃ 對流層柱          │ 對流層臭氧      │
# └─────────────┴──────────────┴─────────────────────────┴─────────────────┘
# 
# 中等解析度產品 (5.5 × 7.0 km) - 平衡精度和覆蓋範圍
# ┌─────────────┬──────────────┬─────────────────────────┬─────────────────┐
# │ 產品代碼    │ 解析度 (km)  │ 產品名稱                │ 主要應用        │
# ├─────────────┼──────────────┼─────────────────────────┼─────────────────┤
# │ CO____      │ 5.5 × 7.0    │ CO 總柱              │ 一氧化碳監測    │
# │ CH4___      │ 5.5 × 7.0    │ CH₄ 甲烷混合比       │ 溫室氣體監測    │
# └─────────────┴──────────────┴─────────────────────────┴─────────────────┘
# 
# 低解析度產品 (30.0 × 30.0 km) - 大範圍區域分析
# ┌─────────────┬──────────────┬─────────────────────────┬─────────────────┐
# │ 產品代碼    │ 解析度 (km)  │ 產品名稱                │ 主要應用        │
# ├─────────────┼──────────────┼─────────────────────────┼─────────────────┤
# │ O3__PR      │ 30.0 × 30.0  │ O₃ 剖面              │ 全球臭氧分析    │
# └─────────────┴──────────────┴─────────────────────────┴─────────────────┘
# 
# 解析度選擇指南：
# ┌─────────────────┬─────────────────┬─────────────────────────────────────┐
# │ 應用場景        │ 推薦解析度      │ 適用產品                              │
# ├─────────────────┼─────────────────┼─────────────────────────────────────┤
# │ 城市空氣品質    │ 高解析度        │ NO₂, O₃, SO₂, HCHO, AER_AI         │
# │ 區域性研究      │ 中等解析度      │ CO, CH₄                            │
# │ 全球性分析      │ 低解析度        │ O₃__PR                             │
# │ 工業污染監測    │ 高解析度        │ SO₂, NO₂                           │
# │ 溫室氣體研究    │ 中等解析度      │ CH₄, CO                            │
# │ 氣象預報        │ 高解析度        │ AER_AI, AER_LH                     │
# └─────────────────┴─────────────────┴─────────────────────────────────────┘
# =============================================================================
