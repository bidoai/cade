"""CLI entry point for the cade ingestion pipeline.

Commands:
  cade-ingest run --date YYYY-MM-DD [--dry-run] [--workers N]
      Ingest all counterparties for a single COB date.

  cade-ingest backfill --from YYYY-MM-DD --to YYYY-MM-DD [--workers N]
      Ingest all COB dates in a date range (inclusive).

  cade-ingest status --date YYYY-MM-DD
      Show what is in staging vs what is already in cade for a date.

  cade-ingest check-config
      Validate all required environment variables and exit.
"""
from __future__ import annotations

import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import httpx
import psycopg2
import typer
from rich.console import Console
from rich.table import Table

from cade.ingest import config as cfg_module
from cade.ingest.assembler import assemble
from cade.ingest.exceptions import ConfigError, MatrixHashMismatch
from cade.ingest.matrix_sync import download_matrix, matrix_dest_path
from cade.ingest import staging
from cade.models import MatrixRef

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

app = typer.Typer(help="cade ingestion pipeline — raw staging data → cade")
console = Console()


def _list_s3_matrices(s3_client, bucket: str, cob_date: date) -> list[dict]:
    """List matrix objects in S3 for a COB date.

    Returns list of dicts: {s3_key, counterparty_id, netting_set_id, instrument, expected_hash}
    Expected hash is read from S3 object metadata key 'sha256'.
    """
    prefix = f"matrices/{cob_date}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Key format: matrices/{cob_date}/{counterparty_id}/{netting_set_id}/{instrument}.ext
            # or:          matrices/{cob_date}/{counterparty_id}/{instrument}.ext
            parts = key.split("/")
            if len(parts) < 4:
                continue
            # Fetch metadata to get expected hash
            head = s3_client.head_object(Bucket=bucket, Key=key)
            expected_hash = head.get("Metadata", {}).get("sha256", "")
            objects.append({
                "s3_key": key,
                "parts": parts,
                "expected_hash": expected_hash,
                "size": obj["Size"],
            })
    return objects


def _ingest_one(
    snap,
    api_url: str,
    dry_run: bool,
) -> str:
    """POST one COBSnapshot to the cade API. Returns 'posted', 'skipped', or 'error:{msg}'."""
    if dry_run:
        return "dry_run"

    payload = {
        "snapshot": snap.model_dump(mode="json"),
        "exposure_total": 0.0,
    }
    try:
        resp = httpx.post(f"{api_url}/ingest", json=payload, timeout=30)
        if resp.status_code == 201:
            return "posted"
        elif resp.status_code == 409:
            return "skipped"
        else:
            return f"error:{resp.status_code}:{resp.text[:200]}"
    except Exception as e:
        return f"error:{e}"


def _run_date(
    cob_date: date,
    conf,
    dry_run: bool,
    workers: int,
) -> dict:
    """Run ingestion for a single COB date. Returns result counts."""
    import boto3

    results = {"posted": 0, "skipped": 0, "incomplete": 0, "errors": 0}

    conn = psycopg2.connect(conf.staging_db_url)
    s3 = boto3.client("s3", region_name=conf.aws_region)

    try:
        agreements = staging.get_agreements(conn, cob_date)
        if not agreements:
            logger.info("No staging_agreements rows for %s — nothing to ingest", cob_date)
            return results

        trades = staging.get_trades(conn, cob_date)
        market_data = staging.get_market_data(conn, cob_date)

        # Download matrices from S3
        s3_objects = _list_s3_matrices(s3, conf.s3_bucket, cob_date)
        matrix_refs: dict[str, dict[str, MatrixRef]] = {}  # {cp/ns: {instrument: MatrixRef}}

        for obj in s3_objects:
            parts = obj["parts"]
            # Key: matrices/{date}/{cp_id}/{ns_id}/{instrument}.ext  (4+ parts after "matrices/")
            # or:  matrices/{date}/{cp_id}/{instrument}.ext            (3 parts after "matrices/")
            if len(parts) >= 5:
                cp_id, ns_id = parts[2], parts[3]
                instrument = ".".join(parts[4:]).rsplit(".", 1)[0]  # strip extension
            else:
                logger.warning("Unexpected S3 key format: %s — skipping", obj["s3_key"])
                continue

            ns_key = f"{cp_id}/{ns_id}"
            dest = matrix_dest_path(conf.data_dir, cob_date, cp_id, obj["s3_key"])

            if dest.exists():
                # Already downloaded; reuse
                from cade.ingest.matrix_sync import compute_file_hash
                actual = compute_file_hash(dest)
                ref = MatrixRef(path=str(dest.relative_to(conf.data_dir)), hash=actual)
            else:
                if not obj["expected_hash"]:
                    logger.warning(
                        "No expected_hash in S3 metadata for %s — skipping matrix", obj["s3_key"]
                    )
                    continue
                try:
                    download_matrix(s3, conf.s3_bucket, obj["s3_key"], dest, obj["expected_hash"])
                    ref = MatrixRef(
                        path=str(dest.relative_to(conf.data_dir)),
                        hash=obj["expected_hash"],
                    )
                except MatrixHashMismatch as e:
                    logger.error("Matrix hash mismatch: %s", e)
                    results["errors"] += 1
                    continue

            matrix_refs.setdefault(ns_key, {})[instrument] = ref

        # Assemble snapshots
        snapshots, skipped = assemble(agreements, trades, market_data, matrix_refs, cob_date)
        results["incomplete"] += len(skipped)
        for ns_key in skipped:
            logger.warning("Skipping incomplete counterparty/ns: %s for %s", ns_key, cob_date)

        # POST to cade (with concurrency)
        def post_one(snap):
            result = _ingest_one(snap, conf.api_url, dry_run)
            return snap.counterparty_id, snap.netting_set_id, result

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(post_one, snap) for snap in snapshots]
            for future in as_completed(futures):
                cp_id, ns_id, result = future.result()
                if result == "posted" or result == "dry_run":
                    results["posted"] += 1
                    logger.info("✓ %s/%s [%s]", cp_id, ns_id, result)
                elif result == "skipped":
                    results["skipped"] += 1
                    logger.info("~ %s/%s already ingested", cp_id, ns_id)
                else:
                    results["errors"] += 1
                    logger.error("✗ %s/%s %s", cp_id, ns_id, result)

    finally:
        conn.close()

    return results


@app.command("run")
def cmd_run(
    date_: date = typer.Option(..., "--date", help="COB date (YYYY-MM-DD)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Assemble but do not POST"),
    workers: int = typer.Option(4, "--workers", "-w", help="Parallel POST workers", min=1),
):
    """Ingest all counterparties for a single COB date."""
    try:
        conf = cfg_module.load()
    except ConfigError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)

    mode = "DRY RUN — " if dry_run else ""
    console.print(f"[bold]{mode}Ingesting COB date {date_}[/bold]")
    results = _run_date(date_, conf, dry_run=dry_run, workers=workers)
    console.print(
        f"  posted={results['posted']}  skipped={results['skipped']}  "
        f"incomplete={results['incomplete']}  errors={results['errors']}"
    )
    if results["errors"]:
        raise typer.Exit(1)


@app.command("backfill")
def cmd_backfill(
    from_date: date = typer.Option(..., "--from", help="Start COB date (inclusive)"),
    to_date: date = typer.Option(..., "--to", help="End COB date (inclusive)"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    workers: int = typer.Option(4, "--workers", "-w", min=1),
):
    """Ingest all COB dates in a date range (inclusive)."""
    if from_date > to_date:
        typer.echo("--from must be before --to", err=True)
        raise typer.Exit(1)

    try:
        conf = cfg_module.load()
    except ConfigError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)

    dates = []
    d = from_date
    while d <= to_date:
        dates.append(d)
        d += timedelta(days=1)

    console.print(f"[bold]Backfill: {from_date} → {to_date} ({len(dates)} dates)[/bold]")
    total = {"posted": 0, "skipped": 0, "incomplete": 0, "errors": 0}

    for d in dates:
        results = _run_date(d, conf, dry_run=dry_run, workers=workers)
        for k in total:
            total[k] += results[k]
        console.print(
            f"  {d}  posted={results['posted']} skipped={results['skipped']} "
            f"incomplete={results['incomplete']} errors={results['errors']}"
        )

    console.print(f"\n[bold]Total:[/bold] {total}")
    if total["errors"]:
        raise typer.Exit(1)


@app.command("status")
def cmd_status(
    date_: date = typer.Option(..., "--date", help="COB date (YYYY-MM-DD)"),
):
    """Show what is in staging vs what is already in cade for a date."""
    try:
        conf = cfg_module.load()
    except ConfigError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)

    conn = psycopg2.connect(conf.staging_db_url)
    try:
        agreements = staging.get_agreements(conn, date_)
    finally:
        conn.close()

    staging_ns = {(a["counterparty_id"], a["netting_set_id"]) for a in agreements}

    # Check cade for each
    table = Table(title=f"Ingest Status — {date_}")
    table.add_column("Counterparty", style="cyan")
    table.add_column("Netting Set", style="cyan")
    table.add_column("In Staging", justify="center")
    table.add_column("In cade", justify="center")

    for cp_id, ns_id in sorted(staging_ns):
        in_staging = "✓"
        try:
            resp = httpx.get(
                f"{conf.api_url}/counterparties/{cp_id}/agreements/{ns_id}/{date_}",
                timeout=10,
            )
            in_cade = "✓" if resp.status_code == 200 else "—"
        except Exception:
            in_cade = "?"
        table.add_row(cp_id, ns_id, in_staging, in_cade)

    console.print(table)


@app.command("check-config")
def cmd_check_config():
    """Validate configuration and exit."""
    try:
        conf = cfg_module.load()
        console.print("[green]Configuration OK[/green]")
        console.print(f"  STAGING_DB_URL: {conf.staging_db_url[:30]}...")
        console.print(f"  S3_BUCKET:      {conf.s3_bucket}")
        console.print(f"  CADE_DATA_DIR:  {conf.data_dir}")
        console.print(f"  CADE_API_URL:   {conf.api_url}")
    except ConfigError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
