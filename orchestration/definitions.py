from dagster import Definitions

from .assets import all_assets

definitions = Definitions(assets=all_assets)
