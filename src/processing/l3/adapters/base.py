"""L2Adapter 契約:把 source 專屬的原始檔讀成 source 無關的 GranuleL2。

每個 source 只需實作這層(讀檔 + 抽 var/lon/lat/qa);regrid/write/聚合全部共用。
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Iterator, Protocol

from src.processing.l3.granule import GranuleL2


class L2Adapter(Protocol):
    source: str

    def read(self, nc_file: str | Path) -> GranuleL2 | None:
        """讀單一原始檔 → GranuleL2;無法讀/無資料回 None。"""
        ...

    def iter_granules(self, files: Iterable[str | Path]) -> Iterator[GranuleL2]:
        ...
