"""C18:路徑全由環境變數決定,程式碼裡不得寫死任何掛載點。

settings 在 import 時讀環境變數,所以每個情境開子行程跑,避免污染本行程已 import 的常數。
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PROBE = ("from src.config.settings import BASE_DIR, DATA_ROOTS;"
         "print(BASE_DIR); print('|'.join(str(p) for p in DATA_ROOTS))")


def _probe(**env):
    e = {k: v for k, v in os.environ.items()
         if k not in ("SATELLITE_BASE_DIR", "SATELLITE_DATA_ROOTS")}
    e.update(env)
    out = subprocess.run([sys.executable, "-c", PROBE], cwd=REPO, env=e,
                         capture_output=True, text=True, check=True).stdout.splitlines()
    return Path(out[0]), [Path(x) for x in out[1].split("|")]


def test_unset_data_roots_falls_back_to_base_dir():
    # load_dotenv() 不覆寫既有環境變數 → 明確給空字串即可隔離本機 .env 的設定;
    # 空字串必須等同未設(曾經會得到 [],CLI 誤報「找不到任何外接碟」)。
    base, roots = _probe(SATELLITE_BASE_DIR="/tmp/sat_base", SATELLITE_DATA_ROOTS="")
    assert base == Path("/tmp/sat_base")
    assert roots == [base]


def test_data_roots_split_on_pathsep_and_skip_empty():
    _, roots = _probe(SATELLITE_BASE_DIR="/tmp/b",
                      SATELLITE_DATA_ROOTS=os.pathsep.join(["/tmp/a", "", "/tmp/b"]))
    assert roots == [Path("/tmp/a"), Path("/tmp/b")]


def test_no_hardcoded_mount_points_in_code():
    hits = []
    for d in ("src", "scripts", "tests"):
        for f in (REPO / d).rglob("*.py"):
            for i, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
                code = line.split("#", 1)[0]
                if "/Volumes/" in code and f.name != Path(__file__).name:
                    hits.append(f"{f.relative_to(REPO)}:{i}")
    assert not hits, f"寫死掛載點:{hits}(改用 settings.BASE_DIR / DATA_ROOTS)"
