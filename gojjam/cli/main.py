import click
from gojjam.cli.commands.init import init
from gojjam.cli.commands.run import run

@click.group()
def cli():
    """🌊 Gojjam: The Lightweight SQL-First Data Orchestrator."""
    pass

cli.add_command(init)
cli.add_command(run)

if __name__ == "__main__":
    cli()