"""Initial staging schema for cade ingestion pipeline.

Revision ID: 001
Revises:
Create Date: 2026-03-21
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, ARRAY

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "staging_agreements",
        sa.Column("counterparty_id", sa.Text, nullable=False),
        sa.Column("netting_set_id", sa.Text, nullable=False),
        sa.Column("cob_date", sa.Date, nullable=False),
        sa.Column("agreement_id", sa.Text, nullable=False),
        sa.Column("threshold_amount", sa.Numeric, nullable=True),
        sa.Column("minimum_transfer_amount", sa.Numeric, nullable=True),
        sa.Column("independent_amount", sa.Numeric, server_default="0"),
        sa.Column("currency", sa.Text, nullable=True),
        sa.Column("eligible_collateral", ARRAY(sa.Text), nullable=True),
        sa.Column("rounding_amount", sa.Numeric, server_default="0"),
        sa.Column("valuation_agent", sa.Text, nullable=True),
        sa.Column("extra", JSONB, server_default="{}"),
        sa.Column(
            "ingested_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("counterparty_id", "netting_set_id", "cob_date"),
    )

    op.create_table(
        "staging_trades",
        sa.Column("counterparty_id", sa.Text, nullable=False),
        sa.Column("netting_set_id", sa.Text, nullable=False),
        sa.Column("cob_date", sa.Date, nullable=False),
        sa.Column("trade_id", sa.Text, nullable=False),
        sa.Column("product_type", sa.Text, nullable=True),
        sa.Column("notional", sa.Numeric, nullable=True),
        sa.Column("currency", sa.Text, nullable=True),
        sa.Column("direction", sa.Text, nullable=True),
        sa.Column("maturity_date", sa.Date, nullable=True),
        sa.Column("extra", JSONB, server_default="{}"),
        sa.Column(
            "ingested_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint(
            "counterparty_id", "netting_set_id", "cob_date", "trade_id"
        ),
    )

    op.create_index(
        "ix_staging_trades_cob_date", "staging_trades", ["cob_date"]
    )
    op.create_index(
        "ix_staging_agreements_cob_date", "staging_agreements", ["cob_date"]
    )

    op.create_table(
        "staging_market_data",
        sa.Column("cob_date", sa.Date, nullable=False),
        sa.Column("data_type", sa.Text, nullable=False),
        sa.Column("key", sa.Text, nullable=False),
        sa.Column("value", sa.Numeric, nullable=False),
        sa.Column(
            "ingested_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("cob_date", "data_type", "key"),
    )


def downgrade() -> None:
    op.drop_table("staging_market_data")
    op.drop_index("ix_staging_trades_cob_date")
    op.drop_index("ix_staging_agreements_cob_date")
    op.drop_table("staging_trades")
    op.drop_table("staging_agreements")
