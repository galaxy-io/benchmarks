import os
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import dlt
from dlt.sources.sql_database import sql_database


def sqlalchemy_source_url(source_url):
    """Translate the harness URL without changing ConnectorX's URL.

    go-sql-driver uses ``tls=true`` for MySQL. SQLAlchemy/PyMySQL instead
    enables TLS through its ``ssl_*`` query arguments, so reflection needs a
    dialect-specific URL while ConnectorX retains the original one.
    """
    parsed = urlsplit(source_url)
    scheme = "mysql+pymysql" if parsed.scheme == "mysql" else parsed.scheme
    query = []
    tls = False
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key == "tls":
            tls = value.lower() not in {"", "false", "0"}
        else:
            query.append((key, value))
    if parsed.scheme == "mysql" and tls:
        # Enable PyMySQL hostname verification when the harness DSN requires TLS.
        query.append(("ssl_check_hostname", "true"))
    return urlunsplit((scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))


def main():
    tables = os.environ["TABLES"].split(",")
    src = os.environ["SOURCE_DSN"]
    # SQLAlchemy needs a driver-specific URL; ConnectorX takes the plain URL.
    sa_src = sqlalchemy_source_url(src)
    source = sql_database(
        sa_src,
        schema="bench",
        table_names=tables,
        backend="connectorx",
        backend_kwargs={
            "conn": src,
            "return_type": "arrow_stream",
            "batch_size": int(os.environ["EXTRACT_BATCH_SIZE"]),
        },
    ).parallelize()
    p = dlt.pipeline(
        pipeline_name="bench",
        destination=dlt.destinations.postgres(os.environ["SINK_DSN"]),
        dataset_name="bench",
    )
    print(p.run(source, write_disposition="replace", loader_file_format="csv"))


# Guarded: normalize workers use the spawn start method, which re-imports
# this module in every worker.
if __name__ == "__main__":
    main()
