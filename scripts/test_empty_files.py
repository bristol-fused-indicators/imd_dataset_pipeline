from pathlib import Path

import polars as pl
from project_paths import project_root

directory: Path = project_root / "data/raw/bristol_city_of/police_uk"

for f in sorted(directory.glob("*.parquet")):
    n = pl.scan_parquet(f).select(pl.len()).collect().item()
    if n == 0:
        print(f"EMPTY: {f.name}")
