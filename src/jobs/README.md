# Job entry points

Job definitions live as bundle resources in `databricks/resources/`, not here.

- `databricks/resources/etl_flow.job.yml` - the `etl_flow` job (three chained tasks)
- `src/etl/LandingtoFeed.py` - task 1
- `src/etl/FeedtoSTG.py` - task 2
- `src/etl/STGtoDatamart.py` - task 3

`src/ETLPipeline.dbc` is a workspace export from before the bundle existed. It is
not referenced by any resource and cannot be deployed; port anything still needed
from it into `src/etl/` and delete it.
