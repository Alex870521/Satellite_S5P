"""比較兩份格網化結果(新舊路徑、或自建 vs HARP oracle)的標準指標。

**為什麼需要這支**:比較兩種格網化方法時有兩個很容易誤讀的陷阱,實測都踩過:

1. **逐格相關對雜訊大的產品會偏低,但那不代表不一致。**
   SO₂ 的 ``std/|mean| ≈ 1.4``(NO₂ 只有 0.85)。RBF 會平滑、binning 保留雜訊,
   於是逐格 r 只有 0.75 —— 可是同一批資料的**逐日空間平均** r 是 0.9962。
   ⇒ 判定物理一致性要看 :func:`compare_fields` 的 ``daily_r``,不是 ``cell_r``。

2. **用「各自的有效遮罩」算平均會騙人。**
   同一組 SO₂ 檔,各自遮罩下日均差 +19.77%,共同遮罩下只有 −0.45% ——
   差別全在遮罩不同,不在數值。
   ⇒ 所有偏差一律在**共同遮罩**上算;覆蓋率另外單獨報。

另外附一個外插診斷 ``only_a_mean``:在「A 有值、B 無觀測」的格上 A 的均值。
實測舊 RBF 的 SO₂ 在那些格上是 **−4.09e−05**(強負值、非物理),而共同格是 +2.13e−06
—— 那是內插往無觀測區外插出來的假值。這個數字是判斷「誰比較可信」的關鍵證據。
"""
from __future__ import annotations

import numpy as np


def compare_fields(a: np.ndarray, b: np.ndarray) -> dict:
    """比較兩個同形狀的 ``(time, lat, lon)``(或 2D)場。

    ``a`` = 參考(通常是舊路徑 / oracle),``b`` = 待驗(通常是新路徑)。
    所有偏差都在**共同有效遮罩**上計算。回傳指標 dict;無共同有效格則各值為 NaN。
    """
    a = np.asarray(a, dtype="float64")
    b = np.asarray(b, dtype="float64")
    if a.shape != b.shape:
        raise ValueError(f"形狀不一致:{a.shape} vs {b.shape}")

    fa, fb = np.isfinite(a), np.isfinite(b)
    both = fa & fb
    out = {
        "n_common": int(both.sum()),
        "cov_a": float(fa.mean() * 100),
        "cov_b": float(fb.mean() * 100),
        "cell_r": np.nan, "cell_bias_pct": np.nan, "cell_rmse_pct": np.nan,
        "daily_r": np.nan, "daily_bias_pct": np.nan,
        "only_a_frac": float((fa & ~fb).mean() * 100),
        "only_a_mean": np.nan, "common_mean": np.nan,
    }
    if not both.any():
        return out

    x, y = a[both], b[both]
    den = np.abs(x).mean()
    out["cell_r"] = float(np.corrcoef(x, y)[0, 1]) if x.size > 1 else np.nan
    out["cell_bias_pct"] = float((y - x).mean() / den * 100) if den else np.nan
    out["cell_rmse_pct"] = float(np.sqrt(((y - x) ** 2).mean()) / den * 100) if den else np.nan
    out["common_mean"] = float(x.mean())

    only_a = fa & ~fb
    if only_a.any():
        out["only_a_mean"] = float(a[only_a].mean())

    # 逐窗(通常是逐日)空間平均,限共同遮罩 —— 物理一致性的主判準
    if a.ndim == 3:
        da, db = [], []
        for i in range(a.shape[0]):
            m = both[i]
            if m.any():
                da.append(a[i][m].mean())
                db.append(b[i][m].mean())
        if len(da) > 1:
            da, db = np.asarray(da), np.asarray(db)
            k = np.isfinite(da) & np.isfinite(db)
            if k.sum() > 1:
                out["daily_r"] = float(np.corrcoef(da[k], db[k])[0, 1])
                d = np.abs(da[k]).mean()
                out["daily_bias_pct"] = float((db[k] - da[k]).mean() / d * 100) if d else np.nan
    return out


def format_comparison(m: dict, label: str = "") -> str:
    """把 :func:`compare_fields` 的結果印成人看得懂的一段。"""
    lines = [f"比對 {label}".rstrip(), f"  共同有效格 n={m['n_common']:,}"]
    if np.isfinite(m["daily_r"]):
        lines.append(f"  逐日空間平均(共同遮罩):r={m['daily_r']:.4f}  "
                     f"bias={m['daily_bias_pct']:+.2f}%   ← 物理一致性主判準")
    lines.append(f"  逐格:r={m['cell_r']:.4f}  bias={m['cell_bias_pct']:+.2f}%  "
                 f"rmse={m['cell_rmse_pct']:.1f}%")
    lines.append(f"  覆蓋:A {m['cov_a']:.1f}%  →  B {m['cov_b']:.1f}%")
    if np.isfinite(m["only_a_mean"]):
        tail = ""
        if m["only_a_mean"] < 0 <= m["common_mean"]:
            tail = "  ← A 在無觀測區外插出非物理負值"
        lines.append(f"  A有/B無 {m['only_a_frac']:.1f}% 的格:A 均值 {m['only_a_mean']:.3e} "
                     f"vs 共同格 {m['common_mean']:.3e}{tail}")
    return "\n".join(lines)
