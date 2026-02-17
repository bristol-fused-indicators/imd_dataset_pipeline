We are implementing the data collection and unification task within the "Replicating and Nowcasting the IoD for Bristol" project - see docs/project_proposal.pdf for full info.

In order to model the Index of Deprivation, we need a dataset with features and target variables. This will need to be created from data sources that can be collected, cleaned, processed, and unified. Then features can be extracted.

The data sources will be selected based on how appropriate they are for answering our research question. The criteria include:
    - The data can be accessed programatically - that is, it doesn't need to be manually requested
    - The data is updated frequently. If we "nowcast" the index every quarter, we want data to be around that fresh. For some features that we deem to be stable, slower refresh schedules will be acceptable
    - How useful the data were in initial exploritory tasks (replicating ONS methods or simple approximations)
    - whether the data are expensive or cheap for the ONS/a local authority to obtain. This means prioritizing open, passively collected datasets over surveyed information.

The data sources that have been selected for the inital phase are:

    1. Universal Credit Claimants API 
    2. Police UK (api)
    3. Land registry price paid
    4. Connectivity data
    5. OSM features

These data are accessed in different ways. This has previously been done in separate apps. This repository will have to write high-quality, reusable code that can access all of them. Then there will be steps for processing these data and extracting features before finally joining all together and writing the unified dataset to file.

In future, this repository should become a production ready data engineering code base that is orchestratable and schedulable, for example by introducing a tool such as dagster that can import the code to create ops, assets, and jobs. This is definitively out of scope for the current task.

An essential requirement is that any team member will be able to pull my code and create the unified dataset on their laptop (all windows users with python installed).

Some data sources will require more preprocessing than others. Eventually, the grain of all data must be LSOA. To get there, some data will require geospatial transformation, for example by aggregating the number of map points inside an LSOA polygon. Others may exist at a post code level and need to be rolled up to an LSOA level. 

Open Questions:

    1. How to handle the temporal nature of these data? We need a dataset that corresponds to the 2025 report. Should this be all data for 2025? For just the three months before 2025? Do we need another for 2019? Should we define a timeframe (12 month sliding window) and produce a dataset that would have been "fresh" in each month from January 2019 until now?
    2. How can we make this program scalable? It should be straight forward to include a new data source
    3. Is it within scope to handle modelling specific processing steps such as feature selection, dimensionality reduction, scaling, impution? My vote right now is no.
    