# IMD Dataset Pipeline

A pipeline that fetches, processes, and combines open data sources into a unified LSOA-grain dataset for modelling deprivation in Bristol.

The pipeline produces a single parquet file with one row per LSOA and feature columns derived from five data sources: Universal Credit claimants, Police UK crime data, Land Registry price paid data, DfT connectivity scores, and OpenStreetMap features.

## Get Started

### requirements

You need Python 3.12+ and [uv](https://docs.astral.sh/uv/getting-started/installation/) installed. If you don't have uv:

```bash
# windows (powershell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# mac or linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

You also need a Stat-Xplore API key for the Universal Credit data source. Register at [stat-xplore.dwp.gov.uk](https://stat-xplore.dwp.gov.uk/) — it's free

### Clone and install

```bash
git clone https://github.com/fused-indicators/imd-dataset-pipeline.git
cd imd-dataset-pipeline
uv sync
```

`uv sync` creates the virtual environment and installs all dependencies from the lockfile. You don't need to create a venv manually.

### Set up environment variables

Copy the example file and add your API key:

```bash
cp .env.example .env
```

Edit `.env` and set:

```
STATXPLORE_API_KEY=your_key_here
```

### Run the pipeline

```bash
uv run python main.py
```

This fetches raw data from all sources, processes each into LSOA grain features, and combines them into a single output file at `data/output/combined_indicators.parquet`.

The first run will take several minutes as it downloads data from each source. Subsequent runs use cached raw data unless you pass `force=True` to individual fetch functions.

To run an individual module during development:

```bash
uv run python -m imd_pipeline.fetch.police_uk
uv run python -m imd_pipeline.process.police_uk
```

## Project Structure

```
imd-dataset-pipeline/
├─── config/
│       ├─── pipeline.toml          # pipeline desing parameters (stable across runs)
│       └─── run.toml               # runtime parameters (snapshot date, scope)
├─── data/
│       ├─── config/                # amenity groups, query templates
│       ├─── lookup/                # LSOA boundaries, postcode mappings (committed)
│       ├─── raw/                   # source-specific subdirs (gitignored)
│       ├─── processed/             # one parquet per source (gitignored)
│       └─── output/                # final unified dataset (gitignored)
├─── imd_pipeline/                  # core package
│       ├─── fetch/                 # one module per source — downloads raw data
│       ├─── process/               # one module per source — transforms to LSOA orented feature sets
│       ├─── combine/               # joins processed sources into unified dataset
│       └─── utils/                 # shared functions (HTTP, LSOA mapping)
└─── main.py                        # runner
```


## How the pipeline works

The pipeline has three stages:

**Fetch** downloads raw data from external sources and writes it to `data/raw/{source}/`. Each source has its own fetch module that handles API calls, caching, and format conversion. Raw data is cached so that rerunning fetch skips sources that already have data on disk.

**Process** reads raw data, transforms it to a standard format (one row per LSOA, with feature columns), and returns a Polars LazyFrame. Each source's processing is independent. Temporal windowing is applied here — for example, filtering police crime data to a 12 month window

**Combine** left joins all processed LazyFrames onto the LSOAs, producing the final unified dataset. LSOAs missing from a source get null values for that source's features.

## Adding a new data source

1. Create `imd_pipeline/fetch/{source}.py` with a `fetch()` function that downloads data to `data/raw/{source}/`
2. Create `imd_pipeline/process/{source}.py` with a `process()` function that returns a `pl.LazyFrame` with `lsoa_code` as the join key
3. Add two lines to `main.py`: one to fetch, one to process
4. Pass the resulting LazyFrame to the `join()` call in combine

Look at `fetch/connectivity.py` and `process/connectivity.py` as the simplest example to follow.

## current data sources

| Source | Fetch method | Update frequency | Key features |
|--------|-------------|-----------------|--------------|
| Universal Credit | Stat-Xplore API | Monthly | Claim counts by condition group |
| Police UK | Street level API | Monthly | Crime counts by category, outcome rates |
| Land Registry | S3 bulk download | Monthly | Property prices, transaction volumes |
| Connectivity | GOV.UK static file | Annual | DfT transport connectivity scores |
| OpenStreetMap | Overpass API | On demand | Amenity counts, proximity features, land use |

## Contributing

See the [issues list](https://github.com/fused-indicators/imd-dataset-pipeline/issues) for open tasks. Issues tagged `good-first-issue` are designed for team members getting familiar with the codebase

When working on a new feature or fix, create a branch from `main` and open a pull request when ready. The codebase uses type hints and docstrings — follow the patterns in existing modules.

### creating a branch

```
git checkout -b name_of_new_branch
```

use something descriptive for a branch name, like what type of work it is, what issue youre working on, your name, the name of the task. For example:

```
git checkout -b feature/issue-8-add-new-source

git checkout -b bugfix/issue-11-fix-url
```

### open a pull request

open a pr and we will find out whose free to review it at merge it.

## Troubleshooting

### incompatible hardware

in you see an error message when running `uv sync` because polars doesn't want to run on your cpu, you can try a couple of things:

1. add `POLARS_SKIP_CPU_CHECK=1` to your .env file
2. some specific version of one of the other packages, ill update this when i remember which one


If you have swapped packages to fix this incompatibility, we found it useful to reinstall without letting uv use cached packages

```
uv add {package} --no-cache

uv sync --no-cache

```

