import os
from typing import BinaryIO

import duckdb
import tomllib
import yaml

from mdbl.models.cli import ValidFileTypes
from mdbl.models.mappings import DBMappings


def read_mapping(file: BinaryIO, file_type: ValidFileTypes) -> DBMappings:
    match file_type:
        case ValidFileTypes.TOML:
            data = tomllib.load(file)
            return DBMappings.model_validate(data)
        case ValidFileTypes.YAML:
            data = yaml.safe_load(file)
            return DBMappings.model_validate(data)


class MissingColumnError(Exception):
    pass


class MissingTableError(Exception):
    pass


def check_missing_elements(
    con: duckdb.DuckDBPyConnection, db_mappings: DBMappings, folder: str
):
    if not os.path.isdir(folder):
        raise IOError(f"Folder '{folder}' does not exist.")

    db_tables = con.execute("SELECT name FROM (SHOW TABLES);").fetchall()
    db_tables = set(map(lambda row: row[0], db_tables))

    db_columns = {}
    for table in db_tables:
        table_columns = con.execute(
            f"SELECT column_name FROM (SHOW {table})"
        ).fetchall()
        db_columns[table] = set(map(lambda row: row[0], table_columns))

    exceptions: list[MissingColumnError | MissingTableError] = []

    # Check for missing fields on db
    for table_mapping in db_mappings.tables:
        if table_mapping.to not in db_tables:
            exceptions.append(
                MissingTableError(
                    f"Table '{table_mapping.to}' does not exist in database."
                )
            )
            continue
        for column_mapping in table_mapping.columns:
            if column_mapping.to not in db_columns[table_mapping.to]:
                exceptions.append(
                    MissingColumnError(
                        f"Column '{column_mapping.to}' does not exist in table '{table_mapping.to}' in the database"
                    )
                )

    # Check for missing fields on parquets
    for table_mappings in db_mappings.tables:
        parquet_path = os.path.join(folder, table_mappings.from_)
        if not os.path.isdir(parquet_path):
            exceptions.append(
                MissingTableError(
                    f"Parquets '{table_mappings.from_}' not found in '{os.path.join(parquet_path, '*.parquet')}'."
                )
            )
            continue

        columns = duckdb.execute(
            f"""
            SELECT column_name
            FROM (
                SHOW SELECT * FROM read_parquet('{os.path.join(parquet_path, '*.parquet')}')
            );"""
        ).fetchall()
        columns = set(map(lambda row: row[0], columns))
        for column_mapping in table_mappings.columns:
            if column_mapping.from_ not in columns:
                exceptions.append(
                    MissingColumnError(
                        f"Column '{column_mapping.to}' does not exist on parquets '{os.path.join(parquet_path, '*.parquet')}'."
                    )
                )

    if exceptions != []:
        raise ExceptionGroup(
            "While checking the mapping, found missing items",
            exceptions,
        )


def data_load(
    con: duckdb.DuckDBPyConnection,
    db_mappings: DBMappings,
    folder: str = "parquets",
):
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
    INSERT INTO postgres.object (
        SELECT
            oid_parquet as oid,
            firstmjd_parquet as firstmjd,
            ndet_parquet as ndet
        FROM parquet
    );
    ```
    """
    for table in db_mappings.tables:
        aliases = ", ".join(
            [f"{column.from_} as {column.to}" for column in table.columns]
        )
        query = f"SELECT {aliases} FROM read_parquet('{os.path.join(folder, table.from_, '*.parquet')}')"
        con.sql(query).show()

        con.sql(f"INSERT INTO {table.to} BY NAME {query}")
        con.sql(f"SELECT COUNT() as 'Total rows inserted' FROM {table.to}").show()
