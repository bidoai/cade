#!/usr/bin/env bash
# push_to_cade.sh — run on each Linux dump server after the daily COB data dump.
#
# Usage:
#   ./push_to_cade.sh <cob_date> <counterparty_id>
#   ./push_to_cade.sh 2024-03-15 ACME-CORP
#
# Required environment variables:
#   S3_BUCKET        — S3 bucket for matrix files
#   STAGING_DB_URL   — PostgreSQL DSN for staging tables
#   DUMP_DIR         — root directory of daily dumps (default: /data/dumps)
#
# Directory layout expected under DUMP_DIR:
#   {cob_date}/{counterparty_id}/{netting_set_id}/
#     agreement.csv  — single row: agreement terms
#     trades.csv     — one row per trade
#     market_data.csv — rows: data_type,key,value
#     matrices/      — *.npy or other matrix files

set -euo pipefail

COB=${1:?Usage: $0 <cob_date> <counterparty_id>}
CP=${2:?Usage: $0 <cob_date> <counterparty_id>}
DUMP_DIR=${DUMP_DIR:-/data/dumps}
S3_BUCKET=${S3_BUCKET:?S3_BUCKET is not set}
STAGING_DB_URL=${STAGING_DB_URL:?STAGING_DB_URL is not set}

echo "[push_to_cade] COB=$COB CP=$CP"

CP_DIR="$DUMP_DIR/$COB/$CP"
if [ ! -d "$CP_DIR" ]; then
  echo "[push_to_cade] ERROR: $CP_DIR does not exist" >&2
  exit 1
fi

# Upload matrix files to S3 with sha256 metadata
for matrix_file in "$CP_DIR"/*/matrices/*; do
  [ -f "$matrix_file" ] || continue
  NS=$(basename "$(dirname "$(dirname "$matrix_file")")")
  FILENAME=$(basename "$matrix_file")
  S3_KEY="matrices/$COB/$CP/$NS/$FILENAME"
  SHA256="sha256-v1:$(sha256sum "$matrix_file" | awk '{print $1}')"
  echo "[push_to_cade] Uploading $S3_KEY (hash=$SHA256)"
  aws s3 cp "$matrix_file" "s3://$S3_BUCKET/$S3_KEY" \
    --metadata "sha256=$SHA256" --quiet
done

# Load agreement data into Postgres staging
for NS_DIR in "$CP_DIR"/*/; do
  NS=$(basename "$NS_DIR")

  if [ -f "$NS_DIR/agreement.csv" ]; then
    echo "[push_to_cade] Loading agreement: $CP/$NS"
    psql "$STAGING_DB_URL" -c \
      "\copy staging_agreements (counterparty_id, netting_set_id, cob_date, agreement_id, threshold_amount, minimum_transfer_amount, currency, eligible_collateral, valuation_agent) FROM '$NS_DIR/agreement.csv' CSV HEADER ON CONFLICT (counterparty_id, netting_set_id, cob_date) DO NOTHING"
  fi

  if [ -f "$NS_DIR/trades.csv" ]; then
    echo "[push_to_cade] Loading trades: $CP/$NS"
    psql "$STAGING_DB_URL" -c \
      "\copy staging_trades (counterparty_id, netting_set_id, cob_date, trade_id, product_type, notional, currency, direction, maturity_date) FROM '$NS_DIR/trades.csv' CSV HEADER ON CONFLICT (counterparty_id, netting_set_id, cob_date, trade_id) DO NOTHING"
  fi

  if [ -f "$NS_DIR/market_data.csv" ]; then
    echo "[push_to_cade] Loading market data: $CP/$NS"
    psql "$STAGING_DB_URL" -c \
      "\copy staging_market_data (cob_date, data_type, key, value) FROM '$NS_DIR/market_data.csv' CSV HEADER ON CONFLICT (cob_date, data_type, key) DO NOTHING"
  fi
done

echo "[push_to_cade] Done: COB=$COB CP=$CP"
