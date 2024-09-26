from typing import BinaryIO

import duckdb
import tomllib
import yaml

from mdbl.models.cli import ValidFileTypes
from mdbl.models.mappings import DBMappings
from mdbl.utils import generate_dummy_parquets


def read_mapping(file: BinaryIO, file_type: ValidFileTypes) -> DBMappings:
    match file_type:
        case ValidFileTypes.TOML:
            data = tomllib.load(file)
            return DBMappings.model_validate(data)
        case ValidFileTypes.YAML:
            data = yaml.safe_load(file)
            return DBMappings.model_validate(data)


def data_load(db_mappings: DBMappings, folder: str = "parquets"):
    """
    Exmaple:

    ```toml
    [[table]]
    source = "object_parquet"
    to = "object"
    [[table.column]]
    source = "oid_parquet"
    to = "oid"
    [[table.column]]
    source = "firstmjd_parquet"
    to = "firstmjd"
    [[table.column]]
    source = "ndet_parquet"
    to = "ndet"
    ```

    ```sql
    CREATE TABLE postgres.object AS (
        SELECT
            oid_parquet as oid,
            firstmjd_parquet as firstmjd,
            ndet_parquet as ndet
        FROM parquet
    );
    ```
    """
    generate_dummy_parquets(folder=folder)

    with duckdb.connect() as con:
        con.install_extension("postgres")
        con.load_extension("postgres")
        con.sql(
            "ATTACH 'dbname=postgres user=postgres password=postgres host=127.0.0.1' as postgres (TYPE POSTGRES)"
        )

        for table in db_mappings.tables:
            aliases = ", ".join(
                [f"{column.from_} as {column.to}" for column in table.columns]
            )
            query = f"SELECT {aliases} FROM read_parquet('{folder}/{table.from_}/*.parquet')"
            con.sql(query).show()

            con.sql(f"CREATE OR REPLACE TABLE postgres.{table.to} AS {query}")
            con.sql(f"SELECT * FROM postgres.{table.to}")
