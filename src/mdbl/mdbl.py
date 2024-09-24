from enum import Enum
import math
from typing import BinaryIO, override
from faker import Faker
import polars as pl
import duckdb
from pathlib import Path
from pydantic import BaseModel, RootModel
import tomllib
import yaml

fake = Faker()


class Mapping(BaseModel):
    column: str


class TableMappings(BaseModel):
    step: int
    parquet: str
    mapping: dict[str, Mapping]


class DBMappings(RootModel[dict[str, TableMappings]]):
    @override
    def __iter__(self):  # pyright: ignore[reportIncompatibleMethodOverride]
        return iter(self.root)

    def __getitem__(self, item: str):
        return self.root[item]


db_mappings = DBMappings.model_validate(
    {
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
)


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


class ValidFileTypes(Enum):
    TOML = "TOML"
    YAML = "YAML"

    @classmethod
    def possible_values(cls):
        return [variant.value for variant in cls.__members__.values()]


def read_mapping(file: BinaryIO, file_type: ValidFileTypes) -> DBMappings:
    match file_type:
        case ValidFileTypes.TOML:
            data = tomllib.load(file)
            return DBMappings(data)
        case ValidFileTypes.YAML:
            data: object = yaml.safe_load(file)
            return DBMappings(data.__dict__)


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


def data_load(db_mappings: DBMappings = db_mappings, folder: str = "parquets"):
    """
    Exmaple:

    ```python
    mapping = {
        "object": {
            "step": 0,
            "parquet": "object_parquet",
            "mapping": {
                "oid_parquet": {"column": "oid"},
                "firstmjd_parquet": {"column": "firstmjd"},
                "ndet_parquet": {"column": "ndet"},
            },
        }
    }
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

    with duckdb.connect() as con:  # pyright: ignore[reportUnknownMemberType]
        con.install_extension("postgres")
        con.load_extension("postgres")
        _ = con.sql(
            "ATTACH 'dbname=postgres user=postgres password=postgres host=127.0.0.1' as postgres (TYPE POSTGRES)"
        )

        sorted_tables = sorted(db_mappings, key=lambda table: db_mappings[table].step)
        for table in sorted_tables:
            parquet = db_mappings[table].parquet
            mapping = db_mappings[table].mapping

            aliases = ", ".join(
                [
                    f"{parquet_col} as {col_data.column}"
                    for parquet_col, col_data in mapping.items()
                ]
            )
            query = (
                f"SELECT {aliases} FROM read_parquet('{folder}/{parquet}/*.parquet')"
            )
            print(query)
            con.sql(query).show()

            _ = con.sql(f"CREATE OR REPLACE TABLE postgres.{table} AS {query}")
            _ = con.sql(f"SELECT * FROM postgres.{table}")
