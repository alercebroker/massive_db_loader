import click
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
        while True:
            query = click.prompt("SQL")
            try:
                con.sql(query).show()
            except click.ClickException as e:
                click.echo(e)
                return
            except Exception as e:
                click.secho(e, fg="yellow")

