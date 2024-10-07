import math
from pathlib import Path

import polars as pl
from faker import Faker

fake = Faker()

parquets = {
    "objects_parquet": [
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
) -> None:
    oids = [f"{fake.md5()}" for _ in range(n_rows_per_parquet * n_parquets)]
    for table, columns in parquets.items():
        table_folder = Path(folder) / table
        table_folder.mkdir(parents=True, exist_ok=True)
        for i in range(1, n_parquets + 1):
            dummy_data = {
                column: [f"{column}: {fake.md5()}" for _ in range(n_rows_per_parquet)]
                for column in columns
                if column != "oid_parquet"
            }

            dummy_data["oid_parquet"] = oids[
                (i - 1) * n_rows_per_parquet : i * n_rows_per_parquet
            ]

            df = pl.DataFrame(dummy_data)
            df.write_parquet(
                f"{folder}/{table}/{str(i).zfill(math.ceil(math.log10(n_parquets)))}.parquet"
            )
