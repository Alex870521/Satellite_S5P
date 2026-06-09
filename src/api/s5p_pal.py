"""S5P-PAL Level-3 (gridded) STAC client.

Official Copernicus Sentinel-5P only ships L1B/L2; ready-made **gridded L3** comes
from the S5P-PAL service (joined Copernicus Data Space 2025-06). Open/anonymous
STAC API, global NetCDF.

STAC layout (no API key):
    catalog = https://data-portal.s5p-pal.com/api/s5p-l3
    path    = /{product}/{aggregation}/{year}  -> Collection, links rel=item
    item    = ....json -> assets['product'].href = /download/s5p-l3/{uuid} (x-netcdf)
    products: no2, no2-tropospheric, o3, hcho, co, ch4, so2-7km, aod-494nm, aai, alh, ...
    aggregations: day | fortnight | month | season | year  (finest = daily)
    global grid 8192x16384 (~0.022 deg / ~2.4 km), from 2018-04-30.

Used by SentinelHubBase L3 download path (see sentinel_api.py).
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests

L3_API = "https://data-portal.s5p-pal.com/api/s5p-l3"
AGGREGATIONS = ("day", "fortnight", "month", "season", "year")


class S5PPALClient:
    """Minimal STAC client for S5P-PAL L3: list products, find items, resumable download."""

    def __init__(self, api: str = L3_API, logger=None):
        self.api = api.rstrip("/")
        self.logger = logger

    def _log(self, msg: str):
        if self.logger:
            self.logger.info(msg)
        else:
            print(msg)

    # ------------------------------------------------------------------ #
    def list_products(self) -> dict[str, str]:
        """{product_id: title} for every L3 product."""
        r = requests.get(self.api, timeout=60)
        r.raise_for_status()
        return {
            l["href"].rstrip("/").split("/")[-1]: l.get("title", "")
            for l in r.json().get("links", []) if l.get("rel") == "child"
        }

    @staticmethod
    def _parse_iso(s: str) -> datetime:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)

    def find_items(self, product: str, aggregation: str,
                   start: str | datetime, end: str | datetime) -> list[dict]:
        """L3 items for product/aggregation overlapping [start, end].

        Traverses /{product}/{agg}/{year} (the /items and /collections endpoints
        are broken on this server — use catalog child links). Returns dicts with
        id / start / end / href / type.
        """
        if aggregation not in AGGREGATIONS:
            raise ValueError(f"aggregation must be one of {AGGREGATIONS}")
        s = start if isinstance(start, datetime) else datetime.strptime(start, "%Y-%m-%d")
        e = end if isinstance(end, datetime) else datetime.strptime(end, "%Y-%m-%d")

        import re
        date_re = re.compile(r"-(\d{8})-\d{8}\.json$")  # 檔名第一個日期 = 資料日

        items: list[dict] = []
        for year in range(s.year, e.year + 1):
            url = f"{self.api}/{product}/{aggregation}/{year}"
            r = requests.get(url, timeout=60)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            for link in r.json().get("links", []):
                if link.get("rel") != "item":
                    continue
                # 先用檔名內的資料日預篩,避免把整年 item JSON 全抓下來(快很多)
                m = date_re.search(link["href"])
                if m:
                    try:
                        d = datetime.strptime(m.group(1), "%Y%m%d")
                        if d < s.replace(hour=0, minute=0, second=0) - timedelta(days=1) or d > e:
                            continue
                    except ValueError:
                        pass
                it = requests.get(link["href"], timeout=60).json()
                p = it.get("properties", {})
                i_start = self._parse_iso(p.get("start_datetime") or p["datetime"])
                i_end = self._parse_iso(p.get("end_datetime") or p.get("start_datetime") or p["datetime"])
                if i_end < s or i_start > e:
                    continue
                asset = it.get("assets", {}).get("product", {})
                items.append({
                    "id": it.get("id"),
                    "start": i_start.strftime("%Y-%m-%d"),
                    "end": i_end.strftime("%Y-%m-%d"),
                    "href": asset.get("href"),
                    "type": asset.get("type"),
                })
        items.sort(key=lambda x: x["start"])
        return items

    def download(self, href: str, out: Path, max_retries: int = 6) -> Path:
        """Download one global L3 NetCDF with Range-resume + retry.

        S5P-PAL files are large (daily ~480MB, monthly ~1GB) and the signed S3
        URL drops connections mid-stream; it supports Range, so resume from the
        partial .part on each retry.
        """
        out = Path(out)
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(out.suffix + ".part")

        total = None
        try:
            probe = requests.get(href, headers={"Range": "bytes=0-0"}, stream=True,
                                 timeout=120, allow_redirects=True)
            cr = probe.headers.get("Content-Range")
            if cr and "/" in cr:
                total = int(cr.split("/")[-1])
            probe.close()
        except requests.RequestException:
            pass

        for attempt in range(1, max_retries + 1):
            have = tmp.stat().st_size if tmp.exists() else 0
            if total and have >= total:
                break
            headers = {"Range": f"bytes={have}-"} if have else {}
            try:
                with requests.get(href, headers=headers, stream=True, timeout=600,
                                  allow_redirects=True) as r:
                    if have and r.status_code == 200:  # server ignored Range -> restart
                        have = 0
                        tmp.unlink(missing_ok=True)
                    r.raise_for_status()
                    with open(tmp, "ab" if have else "wb") as f:
                        for chunk in r.iter_content(1 << 20):
                            f.write(chunk)
                if not total or tmp.stat().st_size >= total:
                    break
            except (requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as ex:
                got = tmp.stat().st_size if tmp.exists() else 0
                self._log(f"  連線中斷({attempt}/{max_retries}) {type(ex).__name__}，已抓 {got/1e6:.0f}MB，續傳…")
        else:
            raise RuntimeError(f"download failed after {max_retries} retries: {out.name}")

        if total and tmp.stat().st_size != total:
            raise RuntimeError(f"size mismatch {tmp.stat().st_size} != {total}: {out.name}")
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp.rename(out)
        return out
