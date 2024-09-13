import math
from faker import Faker
import polars as pl
import duckdb
from pathlib import Path

fake = Faker()

db_mappings = {
    "object": {
        "step": 0,
        "parquet": "object_parquet",
        "mapping": {
            "oid_parquet": {"column": "oid"},
            "firstmjd_parquet": {"column": "firstmjd"},
            "ndet_parquet": {"column": "ndet"},
        },
    },
    "detections": {
        "step": 1,
        "parquet": "detections_parquet",
        "mapping": {
            "candid_parquet": {"column": "candid"},
            "oid_parquet": {"column": "oid"},
            "other_parquet": {"column": "mag"},
        },
    },
}


parquets = {
    "object_parquet": [
        "oid_parquet",
        "firstmjd_parquet",
        "lastmjd_parquet",
        "ndet_parquet",
    ],
    "detections_parquet": [
        "candid_parquet",
        "oid_parquet",
        "mag_parquet",
        "other_parquet",
        "another_parquet",
    ],
}


def generate_dummy_parquets(
    folder: str = "parquets",
    n_parquets: int = 30,
    n_rows_per_parquet: int = 1000,
):
    for table, columns in parquets.items():
        table_folder = Path(folder) / table
        table_folder.mkdir(parents=True, exist_ok=True)
        for i in range(1, n_parquets + 1):
            dummy_data = {
                column: [f"{column}: {fake.md5()}" for _ in range(n_rows_per_parquet)]
                for column in columns
            }
            df = pl.DataFrame(dummy_data)
            df.write_parquet(
                f"{folder}/{table}/{str(i).zfill(math.ceil(math.log10(n_parquets)))}.parquet"
            )


def data_load(folder: str = "parquets"):
    generate_dummy_parquets(folder=folder)

    with duckdb.connect() as con:
        con.install_extension("postgres")
        con.load_extension("postgres")
        con.sql(
            "ATTACH 'dbname=postgres user=postgres password=postgres host=127.0.0.1' as postgres (TYPE POSTGRES)"
        )

        for table in sorted(db_mappings, key=lambda table: db_mappings[table]["step"]):
            parquet = db_mappings[table]["parquet"]
            mapping = db_mappings[table]["mapping"]

            aliases = ", ".join(
                [
                    f"{parquet_col} as {col_data["column"]}"
                    for parquet_col, col_data in mapping.items()
                ]
            )
            query = (
                f"SELECT {aliases} FROM read_parquet('{folder}/{parquet}/*.parquet')"
            )
            con.sql(query).show()

            con.sql(f"CREATE TABLE postgres.{table} AS {query}")
            con.sql(f"SELECT * FROM postgres.{table}")
