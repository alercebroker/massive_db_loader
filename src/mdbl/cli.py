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
        con.sql(
            "ATTACH 'dbname=postgres user=postgres password=postgres host=127.0.0.1' as postgres (TYPE POSTGRES)"
        )
        while True:
            query = click.prompt("SQL")
            try:
                con.sql(query).show()
            except click.ClickException as e:
                click.echo(e)
                return
            except Exception as e:
                click.secho(e, fg="yellow")


@main.command()
def data_load():
    mdbl.data_load()
