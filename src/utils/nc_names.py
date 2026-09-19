"""NetCDF 變數 / 座標名稱的小工具。

``l3/ingest.py`` 與 ``coverage/reader.py`` 原本各有一份逐字相同的 ``_pick``;
收斂到這裡,兩邊都 ``from src.utils.nc_names import pick_name as _pick``。
"""
from __future__ import annotations

import xarray as xr


def pick_name(ds: xr.Dataset, names: tuple[str, ...]) -> str | None:
    """回傳 ``names`` 中第一個存在於 ``ds``(variables 或 coords)的名字;都沒有回 None。"""
    for n in names:
        if n in ds.variables or n in ds.coords:
            return n
    return None
