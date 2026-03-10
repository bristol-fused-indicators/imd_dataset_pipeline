# ruff: noqa: F401


class TestFetchImports:
    def test_import_police_uk(self):
        from imd_pipeline.fetch import police_uk

    def test_import_universal_credit(self):
        from imd_pipeline.fetch import universal_credit

    def test_import_land_registry(self):
        from imd_pipeline.fetch import land_registry

    def test_import_connectivity(self):
        from imd_pipeline.fetch import connectivity

    def test_import_open_street_map(self):
        from imd_pipeline.fetch import open_street_map

    def test_import_geography_lookup(self):
        from imd_pipeline.fetch import geography_lookup

    def test_import_postcode_lookup(self):
        from imd_pipeline.fetch import postcode_lookup

    def test_import_lsoa_2011_2021_lookup(self):
        from imd_pipeline.fetch import lsoa_2011_2021_lookup


class TestProcessImports:
    def test_import_police_uk(self):
        from imd_pipeline.process import police_uk

    def test_import_universal_credit(self):
        from imd_pipeline.process import universal_credit

    def test_import_land_registry(self):
        from imd_pipeline.process import land_registry

    def test_import_connectivity(self):
        from imd_pipeline.process import connectivity

    def test_import_open_street_map(self):
        from imd_pipeline.process import open_street_map

    def test_import_geography_lookup(self):
        from imd_pipeline.process import geography_lookup

    def test_import_postcode_lookup(self):
        from imd_pipeline.process import postcode_lookup

    def test_import_lsoa_2011_2021_lookup(self):
        from imd_pipeline.process import lsoa_2011_2021_lookup


class TestUtilImports:
    def test_import_http(self):
        from imd_pipeline.utils import http

    def test_import_lsoas(self):
        from imd_pipeline.utils import lsoas

    def test_import_temporal(self):
        from imd_pipeline.utils import timeframes


class TestCombineImports:
    def test_import_join(self):
        from imd_pipeline.combine import join


class TestTopLevelImports:
    def test_import_fetch_package(self):
        from imd_pipeline import fetch

    def test_import_process_package(self):
        from imd_pipeline import process

    def test_import_combine_package(self):
        from imd_pipeline import combine
