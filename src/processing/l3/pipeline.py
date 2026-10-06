"""L3Pipeline:Adapter → Regridder → Writer 的編排器(level-aware)。

每個檔先自動偵測 level:
  * **L2 swath** → Adapter 讀 → SupersampleBinRegridder 格網化(原本的路徑)。
  * **已是 L3 grid** → 跳過 regrid,改 ``ingest_l3`` 讀+對齊到同一 GridSpec。
兩條路徑都吐 GriddedField,寫 nc/圖、時間聚合(L3Accumulator)完全共用。

階段二(預留):L3Accumulator 在固定網格上做時間聚合(daily/monthly 加權平均)。
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, cast

import numpy as np

from src.processing.l3.granule import GranuleL2, GridSpec, GriddedField
from src.processing.l3.ingest import ingest_l3
from src.processing.l3.level import detect_level
import logging

_log = logging.getLogger(__name__)
from src.processing.l3.writer import L3Writer


class L3Pipeline:
    def __init__(self, adapter, regridder, grid: GridSpec, writer: L3Writer | None = None):
        self.adapter = adapter
        self.regridder = regridder
        self.grid = grid
        self.writer = writer or L3Writer()

    def regrid_granule(self, g: GranuleL2) -> GriddedField:
        return cast(GriddedField, self.regridder.regrid(g, self.grid))

    def build_field(self, nc_file: str | Path,
                    level: str | None = None) -> GriddedField | None:
        """讀一個檔 → GriddedField,依 level 決定 regrid(L2)或對齊(L3)。

        ``level`` 預設 None = 自動偵測;傳 "L2"/"L3" 可手動覆寫。
        """
        nc_file = Path(nc_file)
        lvl = level or detect_level(nc_file)
        if lvl == "L3":
            return ingest_l3(nc_file, self.grid, self.adapter.product,
                             source=getattr(self.adapter, "source", ""))
        g = self.adapter.read(nc_file)
        if g is None:
            return None
        g.time = _overpass_time(g, self.grid)
        return self.regrid_granule(g)

    def process_file(self, nc_file: str | Path,
                     out_nc: str | Path | None = None,
                     fig_path: str | Path | None = None,
                     level: str | None = None) -> GriddedField | None:
        gf = self.build_field(nc_file, level=level)
        if gf is None:
            return None
        if out_nc is not None:
            self.writer.write_nc(gf, out_nc)
            if fig_path is not None:
                self.writer.write_figure(gf, out_nc, fig_path)
        return gf

    def process_files(self, files: Iterable[str | Path], out_dir: str | Path | None = None):
        results = []
        for f in files:
            f = Path(f)
            out_nc = (Path(out_dir) / f.name) if out_dir else None
            results.append(self.process_file(f, out_nc=out_nc))
        return results

    # ------------------------------------------------------------------ #
    # 階段二:時間聚合
    # ------------------------------------------------------------------ #
    def aggregate(self, files: Iterable[str | Path], freq: str = "D",
                  level: str | None = None, on_period=None, progress=None,
                  tz_offset_hours: float = 0):
        """把多個 granule 依時間分窗(freq)聚合成每窗一張場。

        因為每個 granule 經超取樣後就已經在同一張固定網格上,聚合 = 逐格加權平均
        (權重 = 該格的子點 count),由 :class:`L3Accumulator` 累加。

        ``freq``: ``"D"`` 日 / ``"M"`` 月 / ``"Y"`` 年 / ``"granule"`` 逐軌(每個 granule
        自成一窗,期別 = 過境時刻,不合併同日多軌;``tz_offset_hours`` 對它無作用)。
        ``on_period(period, result)``: 每完成一窗就回呼(可邊做邊寫檔,不必全留記憶體)。
        ``tz_offset_hours``: 分窗前把 granule 時間加上這個位移(GEMS 用 8 = 台灣當地日期;
        預設 0 = UTC,S5P / MODIS 的既有年檔不受影響)。
        回傳 ``[(period, {"value","count","std"}), ...]``,依時間排序。

        **串流**:先用檔名日期把檔案排成時間序,再一邊讀一邊累加,換窗即 finalize →
        任何時刻只有一個 accumulator 活著(~1.6MB);若改成「先全部讀進來再分組」,
        整年 daily 會佔約 350MB。三個 source 的檔名都內含日期(S5P `…_YYYYMMDDThhmmss_`、
        MODIS `.AYYYYDDD.`、GEMS `_YYYYMMDD_hhmm_`),故排序不需先開檔。
        """
        ordered = sorted((Path(f) for f in files), key=_name_sort_key)

        out, acc, cur = [], None, None
        seen: set = set()
        for f in ordered:
            gf = self.build_field(f, level=level)
            if progress:
                progress(f, gf)
            if gf is None:
                continue
            period = _period_key(np.datetime64(gf.time, "ns"), freq, tz_offset_hours)
            if _is_granule(freq) and (period == cur or period in seen):
                # 同一時刻(相鄰時會被下面的換窗邏輯默默併成一筆,所以要在這裡就攔)
                raise ValueError(
                    f"逐軌模式下 {f.name} 與另一個檔的過境時刻相同({period}):"
                    f"這個產品的檔案沒有逐軌時間(例如 MCD19A2 是已合併多軌的逐日 tile),"
                    f"不能用 freq='granule'。")
            if cur is None or period != cur:
                if acc is not None:
                    res = acc.finalize()
                    out.append((cur, res))
                    if on_period:
                        on_period(cur, res)
                if period in seen:
                    # 檔名順序與實際時間不一致 → 同一窗被切成兩段,會產生重複期別。
                    # 寧可大聲說,也不要靜靜輸出兩筆同日資料。
                    raise ValueError(
                        f"期別 {period} 重複出現:檔案未依時間排序,聚合會產生重複輸出。"
                        f" 請檢查 {f.name} 的檔名日期是否與檔內時間一致。")
                seen.add(period)
                acc, cur = L3Accumulator(self.grid), period
            assert acc is not None   # 第一個有效 granule 一定先建好 acc
            acc.add(gf)
        if acc is not None:
            res = acc.finalize()
            out.append((cur, res))
            if on_period:
                on_period(cur, res)
        return out


def _period_key(t: np.datetime64, freq: str, tz_offset_hours: float = 0) -> np.datetime64:
    """granule 時間 → 分窗鍵(該窗的起點)。``tz_offset_hours`` 讓分窗依當地時間切。

    例:GEMS 的 UTC 23:45 是台灣隔天 07:45,UTC 分組會把它算進前一天。
    """
    if _is_granule(freq):
        return cast(np.datetime64, np.datetime64(t, "ns"))   # 逐軌:期別就是過境時刻本身
    if tz_offset_hours:
        t = t + np.timedelta64(int(round(tz_offset_hours * 3600)), "s")
    unit = {"D": "D", "M": "M", "Y": "Y"}.get(freq.upper())
    if unit is None:
        raise ValueError(f"freq 只支援 D/M/Y/granule,收到 {freq!r}")
    return cast(np.datetime64, t.astype(f"datetime64[{unit}]"))


def _is_granule(freq: str) -> bool:
    return freq.lower() in ("granule", "g")


def _overpass_time(g: GranuleL2, grid: GridSpec) -> np.datetime64:
    """granule 在目標網格範圍內的平均觀測時刻。

    S5P 一個檔是一整圈軌道(~100 分鐘),掃過台灣框只有一兩分鐘;檔案的 ``time`` 又只是
    當天 00:00 的參考時間。取「有像元落在網格範圍內的掃描線」的平均時間,才是逐軌輸出
    要的過境時刻。沒有 ``scan_time``(GEMS/MODIS 由檔名給時間)或掃描線全在框外時,
    原樣回傳 ``g.time``。
    """
    if g.scan_time is None:
        return g.time
    lat, lon = grid.lat, grid.lon
    inside = ((g.lat >= lat.min()) & (g.lat <= lat.max())
              & (g.lon >= lon.min()) & (g.lon <= lon.max()))
    rows = inside.any(axis=1)
    t = np.asarray(g.scan_time)[rows]
    t = t[~np.isnat(t)]
    if t.size == 0:
        return g.time
    ns = t.astype("datetime64[ns]").astype("int64")
    return np.datetime64(int(round(float(ns.mean()))), "ns")


def _name_sort_key(p: Path):
    """用檔名內的日期排序(避免為了排序先開一輪檔)。取不到就退回檔名字典序。"""
    from src.utils.extract_datetime_from_filename import extract_datetime_from_filename

    exc = None
    try:
        d = extract_datetime_from_filename(p.name, to_local=False)
    except Exception as e:
        d, exc = None, e
    if d is None:
        # 退回字典序不是錯,但要出聲:同批多檔如此時,aggregate 的重複期別保護會 raise,
        # 沒有這行 warning 使用者只會看到一個不知從何而來的 ValueError。
        _log.warning("排序退化:%s 取不到檔名日期(%s),退回字典序", p.name, exc or "無日期樣式")
        return (1, "", p.name)
    return (0, d.isoformat(), p.name)


class L3Accumulator:
    """階段二:固定網格上的時間聚合(running 加權平均)。

    因為每個 granule 經超取樣後已是整張網格(覆蓋外 NaN),聚合 = 跨 granule 的
    逐格加權平均(權重 = 該格 count)。輸出 value / count / std。
    """

    def __init__(self, grid: GridSpec):
        shape = (len(grid.lat), len(grid.lon))
        self.grid = grid
        self._wsum = np.zeros(shape)
        self._sum = np.zeros(shape)
        self._sqsum = np.zeros(shape)
        self._n = np.zeros(shape)

    def add(self, gf: GriddedField) -> None:
        v = gf.value
        w = np.where(np.isfinite(v), gf.count, 0.0)
        v0 = np.where(np.isfinite(v), v, 0.0)
        self._wsum += w
        self._sum += w * v0
        self._sqsum += w * v0 * v0
        self._n += (w > 0)

    def finalize(self) -> dict:
        with np.errstate(invalid="ignore", divide="ignore"):
            mean = self._sum / self._wsum
            var = self._sqsum / self._wsum - mean * mean
        mean[self._wsum == 0] = np.nan
        std = np.sqrt(np.clip(var, 0, None))
        std[self._wsum == 0] = np.nan
        return {"value": mean, "count": self._n, "std": std}
