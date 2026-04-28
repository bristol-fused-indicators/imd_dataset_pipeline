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

It will be possible to use the code using pip or conda to manage the project, but instructions will not be provided.

You also need a Stat-Xplore API key for the Universal Credit data source. Register at [stat-xplore.dwp.gov.uk](https://stat-xplore.dwp.gov.uk/) — it's free

### Clone and install

```bash
git clone https://github.com/fused-indicators/imd_dataset_pipeline.git
cd imd_dataset_pipeline 
uv sync
```

`uv sync` creates the virtual environment and installs all dependencies from the lockfile. You don't need to create a venv manually.

### Set up environment variables

Copy the example file and add your API key:

```bash
cp .env.example .env
```

Or just copy and paste it in your IDE, and rename it to `.env` yourself.

Edit `.env` and set:

```
STATXPLORE_API_KEY=your_key_here
```

### Run the pipeline

```bash
uv run python main.py
```

This fetches raw data from all sources, processes each into LSOA grain features, and combines them into a single output file at `data/output/combined_indicators.parquet`.

The first run will take several minutes as it downloads data from each source. Subsequent runs use cached raw data unless you pass `force_refresh=True` to individual fetch functions.

To run an individual module during development:

```bash
uv run python -m imd_pipeline.fetch.police_uk
uv run python -m imd_pipeline.process.police_uk
```


## Running the pipeline

The pipeline can be run in three ways, each suited to different needs:

### Option 1: Direct Python runner (`main.py`)

Runs the pipeline once, reading configuration from TOML files. This is the simplest way to test changes locally.

```bash
uv run python main.py
```

This reads:
- `config/pipeline_parameters.toml` — `window_months` (timespan for aggregation)
- `config/run_parameters.toml` — `snapshot_date` (end date for the window) and `lad_names` (list of districts to process)

Example `config/run_parameters.toml`:
```toml
[snapshot]
date = 2025-12-01

[scope]
lad_names = ["Bristol, City of"]
```

### Option 2: Dagster UI (orchestration demo)

Demonstrates how the pipelines data collection could be automated via scheduled jobs. Uses Dagster's asset DAG,
freshness policies, and schedules. Assets are partitioned per district, with example schedules defined for Bristol.

**Setup:**
```bash
uv sync --dev
```

**Activate the virtual environment:**

On Windows (powershell):
```powershell
.venv\scripts\activate
```

On mac/Linux:
```bash
source .venv/bin/activate
```

**Start Dagster:**
```bash
dagster dev -m orchestration
```

Dagster serves on `http://localhost:3000`. You will see:
- An asset DAG showing the data pipeline structure (fetch -> process -> combine)
- Assets partitioned by district (Bristol, Exeter, Plymouth, Newcastle upon Tyne, Bournemouth/Christchurch/Poole, Sheffield)
- Freshness policies and cron schedules that trigger Bristol-only runs monthly/quarterly/annually
- Ability to manually materialise any asset or partition via the UI

**To materialise a single asset for Bristol:**
1. Navigate to the asset in the DAG
2. Click "Materialise" 
3. Select partition "Bristol, City of"
4. View logs in the Runs tab

This demonstrates how a production deployment would automatically collect and process district data on a schedule.

### Option 3: Historical backfill (`scripts/quarterly_backfill_op.py`)

A standalone script for running the full historical pipeline across multiple dates (2019–2025) and cities.
Useful for generating snapshots at anchor dates (all 6 cities) and quarterly intervals (Bristol only).

**What it does:**
- Generates quarterly snapshot dates from 2019-09-01 to 2025-10-01
- For anchor dates (2019-09-01 and 2025-10-01): processes all cities
- For quarterly dates in between: processes Bristol only
- Skips snapshots that have already been computed
- Renames output files per snapshot date for archival

**Run:**
```bash
uv run python scripts/quarterly_backfill_op.py
```

This overwrites `config/run_parameters.toml` for each run, so use it in isolation or revert the config afterwards.

The output of this method produces all the data needed to replicate the nowcasting research - note that you will have to combine the data of multiple cities for the anchor files using the stitch.py script (updated the date in the filepath to the appropriate one and running twice).


## Project Structure

```
imd-dataset-pipeline/
├─── config/
│       ├─── pipeline_parameters.toml    # pipeline design parameters (stable across runs)
│       └─── run_parameters.toml         # runtime parameters (snapshot date, scope)
├─── data/
│       ├─── config/                # amenity groups, query templates
│       ├─── lookup/                # LSOA boundaries, postcode mappings (committed)
│       ├─── raw/                   # source-specific subdirs (gitignored)
│       ├─── processed/             # one parquet per source (gitignored)
│       └─── output/                # final unified dataset (gitignored)
├─── imd_pipeline/                  # core package
│       ├─── fetch/                 # one module per source — downloads raw data
│       ├─── process/               # one module per source — transforms to LSOA oriented feature sets
│       ├─── combine/               # joins processed sources into unified dataset
│       └─── utils/                 # shared functions (HTTP, LSOA mapping)
├─── orchestration/                 # Dagster definitions for demonstration
│       ├─── assets.py              # asset DAG per data source
│       ├─── jobs.py                # job groupings
│       ├─── schedules.py           # cron schedules (Bristol partition only)
│       ├─── configs.py             # TimeframeConfig for snapshot_date/window_months
│       └─── definitions.py         # Dagster Definitions export
├─── scripts/
│       └─── quarterly_backfill_op.py    # standalone script for historical backfill
├─── tests/
├─── main.py                        # entry point for a single run
└─── pyproject.toml
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

