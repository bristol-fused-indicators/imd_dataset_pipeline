from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder

from .defs.assets import all_assets


@definitions
def defs():
    defs = Definitions(assets=all_assets)
    return defs
