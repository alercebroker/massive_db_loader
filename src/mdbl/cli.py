import json
from typing import Any, BinaryIO, TextIO

import click
import duckdb

import mdbl.mdbl as mdbl
from mdbl.models.cli import ValidFileTypes
from mdbl.models.mappings import DBMappings


@click.group
@click.option("--db-name", required=True, type=str, envvar="DB_NAME")
@click.option("--db-user", required=True, type=str, envvar="DB_USER")
@click.option("--db-host", required=True, type=str, envvar="DB_HOST")
@click.option("--db-port", required=True, type=int, envvar="DB_PORT")
@click.option("--db-pass", required=True, type=str, envvar="DB_PASS")
@click.pass_context
def main(
    ctx: click.Context,
    db_name: str,
    db_user: str,
    db_host: str,
    db_port: int,
    db_pass: str,
):
    """
    MDBL:
    Application to load large parquet files into a DB.
    """
    con = duckdb.connect()
    con.install_extension("postgres")
    con.load_extension("postgres")
    con.sql(
        f"CREATE SECRET (TYPE POSTGRES, HOST '{db_host}', PORT {db_port}, DATABASE {db_name}, USER '{db_user}', PASSWORD '{db_pass}');"
    )
    con.sql("ATTACH '' AS pg (TYPE POSTGRES);")
    con.sql("USE pg;")

    ctx.ensure_object(dict)
    ctx.obj["con"] = con


@main.command()
@click.option("-f", "--file", type=click.File("w"))
@click.option("-i", "--indent", type=int, default=4)
def generate_mapping_schema(file: TextIO | None, indent: int):
    """
    Generates a JSON schema describing the format of the mapping file
    to give better IDE support.
    """
    json_shema = DBMappings.model_json_schema()
    if file:
        file.write(json.dumps(json_shema))
    else:
        click.echo(json.dumps(json_shema, indent=indent))


@main.command()
@click.pass_obj
@click.option("-p", "--parquets", default="parquets", type=str)
@click.option("-m", "--mapping", required=True, type=click.File("rb"))
@click.option(
    "-t",
    "--file_type",
    required=True,
    default="TOML",
    type=click.Choice(ValidFileTypes.possible_values(), case_sensitive=False),
)
def data_load(
    obj: dict[str, Any],
    parquets: str,
    mapping: BinaryIO,
    file_type: str,
):
    """
    Loads the parquets into the database applying the especified mappings.
    """
    con: duckdb.DuckDBPyConnection = obj["con"]
    db_mappings = mdbl.read_mapping(mapping, ValidFileTypes(file_type))
    try:
        mdbl.check_missing_elements(con, db_mappings, parquets)
        mdbl.data_load(con, db_mappings, parquets)
    except IOError as e:
        click.echo(e)
    except ExceptionGroup as eg:
        click.echo(eg.message)
        for e in eg.exceptions:
            click.echo(f"\t> {e}")
    finally:
        con.close()
