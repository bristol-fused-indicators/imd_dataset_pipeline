import os
import pathlib

import polars as pl
from project_paths import paths

output_dirs = paths.data_output.glob("*/combined_indicators_2019-09-01.parquet")
dfs = {file.parts[-2:]: pl.read_parquet(str(file)) for file in output_dirs}

print(len(dfs))


for file, df in dfs.items():
    print(str(file))
    print(df.head())


big_df = pl.concat(dfs.values(), how="diagonal_relaxed").fill_null(0)

print((big_df.describe()))

print("BBBB")

big_df.write_parquet("combined_combined_indicators_2019-09-01.parquet")
