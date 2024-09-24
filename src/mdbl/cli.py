from typing import BinaryIO
import click
import mdbl.mdbl as mdbl
import duckdb


@click.group
def main():
    """
    Main Group
    """
    pass


@main.command()
def hello_world():
    """
    Prints 'Hello, World!' to the console.
    """
    click.echo("Hello, World!")


@main.command()
def sql():
    with duckdb.connect() as con:
        con.install_extension("postgres")
        con.load_extension("postgres")
        _ = con.sql(
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
@click.option("--file", "-f", required=True, type=click.File("rb"))
@click.option(
    "--file-type",
    "-t",
    required=True,
    type=click.Choice(mdbl.ValidFileTypes.possible_values(), case_sensitive=False),
)
def data_load(file: BinaryIO, file_type: str):
    db_mappings = mdbl.read_mapping(file, mdbl.ValidFileTypes(file_type))
    click.echo(db_mappings)
    mdbl.data_load(db_mappings)
