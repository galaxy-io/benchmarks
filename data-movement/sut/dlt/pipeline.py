import os

import dlt
from dlt.sources.sql_database import sql_database


def main():
    tables = os.environ["TABLES"].split(",")
    source = sql_database(
        os.environ["SOURCE_DSN"],
        schema="public",
        table_names=tables,
        backend="connectorx",
        chunk_size=100000,
        backend_kwargs={"conn": os.environ["SOURCE_DSN"], "return_type": "arrow_stream"},
    ).parallelize()
    p = dlt.pipeline(
        pipeline_name="bench",
        destination=dlt.destinations.postgres(os.environ["SINK_DSN"]),
        dataset_name="public",
    )
    print(p.run(source, write_disposition="replace", loader_file_format="csv"))


# Guarded: normalize workers use the spawn start method, which re-imports
# this module in every worker.
if __name__ == "__main__":
    main()
