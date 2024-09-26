import json
from typing import BinaryIO, TextIO

import click
import duckdb

import mdbl.mdbl as mdbl
from mdbl.models.cli import ValidFileTypes
from mdbl.models.mappings import DBMappings


@click.group
def main():
    """
    Main Group
    """
    pass


@main.command()
@click.option("-f", "--file", type=click.File("w"))
@click.option("-i", "--indent", type=int, default=4)
def schema(file: TextIO | None, indent: int):
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
def sql():
    with duckdb.connect() as con:
        con.install_extension("postgres")
        con.load_extension("postgres")
        con.sql(
            "ATTACH 'dbname=postgres user=postgres password=postgres host=127.0.0.1' as postgres (TYPE POSTGRES)"
        )
        while True:
            query: str = click.prompt("SQL")
            try:
                con.sql(query).show()
            except click.ClickException as e:
                click.echo(e)
                return
            except Exception as e:
                click.secho(e, fg="yellow")


@main.command()
@click.option("-f", "--file", required=True, type=click.File("rb"))
@click.option(
    "-t",
    "--file_type",
    required=True,
    type=click.Choice(ValidFileTypes.possible_values(), case_sensitive=False),
)
def data_load(file: BinaryIO, file_type: str):
    db_mappings = mdbl.read_mapping(file, ValidFileTypes(file_type))
    mdbl.data_load(db_mappings)
