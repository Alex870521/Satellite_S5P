"""C17:`plot_taiwan_map` 只剩一份實作(src/visualization/plot_taiwan.py)。

合併前兩份同名同簽名的函式,同樣的 map_scale='Taiwan' 一份畫全島、一份畫中北部。
這裡釘住:'Taiwan' 一律全島、中北部改叫 'Central'、舊名仍保留原輸出並發棄用警告。
合併時四種組合已做過逐像素比對(相同);這裡不繪製,只驗範圍,CI 不需要 Natural Earth 資料。
"""
from __future__ import annotations

import warnings

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pytest  # noqa: E402

from src.visualization.plot_taiwan import EXTENTS, plot_taiwan_map  # noqa: E402


def _extent(**kw):
    fig, ax = plot_taiwan_map(dpi=30, **kw)
    try:
        return [round(x, 3) for x in ax.get_extent()]
    finally:
        plt.close(fig)


@pytest.mark.parametrize("scale", ["Taiwan", "Central", "East_Asia"])
def test_map_scale_extents(scale):
    assert _extent(map_scale=scale) == EXTENTS[scale]


def test_taiwan_means_whole_island():
    assert EXTENTS["Taiwan"] == [119, 123, 21, 26]


def test_explicit_extent_overrides_scale():
    assert _extent(map_scale="East_Asia", extent=[121, 122, 24.5, 25.5]) == [121, 122, 24.5, 25.5]


def test_global_no_longer_crashes():
    assert _extent(map_scale="Global") == [-180, 180, -90, 90]


def test_unknown_scale_rejected():
    with pytest.raises(ValueError):
        plot_taiwan_map(map_scale="taiwan")


def test_old_power_plant_name_keeps_central_and_warns():
    from src.visualization import plot_taiwan_power_plant as P
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        fig, ax = P.plot_taiwan_map(map_scale="Taiwan", dpi=30)
    try:
        assert [round(x, 3) for x in ax.get_extent()] == EXTENTS["Central"]
        assert any(issubclass(x.category, DeprecationWarning) for x in w)
    finally:
        plt.close(fig)
