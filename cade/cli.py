"""CLI for cade."""
import json
import sys
from datetime import date
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from cade.backends.parquet import ParquetBackend
from cade.exceptions import InvalidRangeError, SnapshotNotFound

app = typer.Typer(help="Counterparty Agreement Data Engine")
console = Console()

import os
_DATA_DIR = os.environ.get("CADE_DATA_DIR", "./data")


def _repo() -> ParquetBackend:
    return ParquetBackend(_DATA_DIR)


@app.command("get")
def cmd_get(
    counterparty_id: str = typer.Argument(..., help="Counterparty ID"),
    netting_set_id: str = typer.Argument(..., help="Netting set ID"),
    date_: date = typer.Option(..., "--date", help="COB date (YYYY-MM-DD)"),
    output: str = typer.Option("json", "--output", "-o", help="Output format: json"),
):
    """Retrieve a snapshot for a counterparty netting set on a COB date."""
    try:
        snapshot = _repo().get_snapshot(counterparty_id, netting_set_id, date_)
        typer.echo(snapshot.model_dump_json(indent=2))
    except SnapshotNotFound as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command("diff")
def cmd_diff(
    counterparty_id: str = typer.Argument(...),
    netting_set_id: str = typer.Argument(...),
    from_date: date = typer.Option(..., "--from"),
    to_date: date = typer.Option(..., "--to"),
):
    """Show what changed between two COB dates for a netting set."""
    try:
        diff = _repo().get_diff(counterparty_id, netting_set_id, from_date, to_date)
        typer.echo(diff.model_dump_json(indent=2))
    except (SnapshotNotFound, InvalidRangeError) as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command("who-matters")
def cmd_who_matters(
    date_: date = typer.Option(..., "--date", help="COB date (YYYY-MM-DD)"),
    threshold: Optional[float] = typer.Option(None, "--threshold", "-t", help="Minimum exposure"),
    top_n: Optional[int] = typer.Option(20, "--top", "-n", help="Maximum rows to return"),
):
    """Show counterparties ranked by exposure on a COB date."""
    summaries, stale = _repo().get_portfolio(date_, threshold=threshold, top_n=top_n)
    if stale:
        console.print("[yellow]Warning: portfolio index may be stale[/yellow]")
    table = Table(title=f"Portfolio Exposure — {date_}")
    table.add_column("Counterparty", style="cyan")
    table.add_column("Netting Set", style="cyan")
    table.add_column("Exposure", justify="right", style="green")
    for s in summaries:
        table.add_row(s.counterparty_id, s.netting_set_id, f"{s.exposure_total:,.0f}")
    console.print(table)


@app.command("list-dates")
def cmd_list_dates(
    counterparty_id: str = typer.Argument(...),
    netting_set_id: str = typer.Argument(...),
):
    """List all COB dates with snapshots for a netting set."""
    dates = _repo().list_cob_dates(counterparty_id, netting_set_id)
    for d in dates:
        typer.echo(str(d))


@app.command("export")
def cmd_export(
    counterparty_id: str = typer.Argument(...),
    netting_set_id: str = typer.Argument(...),
    date_: date = typer.Option(..., "--date"),
    fmt: str = typer.Option("json", "--format", "-f", help="json"),
):
    """Export a snapshot to stdout."""
    try:
        snapshot = _repo().get_snapshot(counterparty_id, netting_set_id, date_)
        if fmt == "json":
            typer.echo(snapshot.model_dump_json(indent=2))
        else:
            typer.echo(f"Unsupported format: {fmt}", err=True)
            raise typer.Exit(1)
    except SnapshotNotFound as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
