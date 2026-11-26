import argparse, sqlite3, json, os
from datetime import datetime, timezone
import polars as pl
import duckdb
from pydantic import BaseModel, EmailStr, Field, ValidationError
from quarantine_fixer import add_suggestions, replay

WATERMARK_FILE = "data/lakehouse/watermark.json"
WAREHOUSE = "data/lakehouse/warehouse.duckdb"
ORDER_COLS = ["order_id", "customer_email", "product_sku", "amount", "status", "updated_at"]


class OrderContract(BaseModel):
    order_id: int
    customer_email: EmailStr
    product_sku: str
    amount: float = Field(gt=0)
    status: str = Field(pattern="^(PLACED|SHIPPED|DELIVERED)$")
    updated_at: str


def get_watermark():
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, "r") as f:
            return json.load(f).get("last_updated_at", "1970-01-01T00:00:00+00:00")
    return "1970-01-01T00:00:00+00:00"


def save_watermark(ts):
    with open(WATERMARK_FILE, "w") as f:
        json.dump({"last_updated_at": ts, "updated_run": datetime.now(timezone.utc).isoformat()}, f, indent=2)


def validate_rows(rows, run_ts):
    valid_rows, quarantine_rows = [], []
    for row in rows:
        try:
            OrderContract(**row)
            valid_rows.append(row)
        except ValidationError as e:
            err = e.errors()[0]
            row["error_field"] = str(err["loc"][0])
            row["error_reason"] = err["msg"]
            row["quarantined_at"] = run_ts
            quarantine_rows.append(row)
    return valid_rows, quarantine_rows


def init_warehouse(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_orders_scd2 (
            order_id INTEGER,
            customer_email VARCHAR,
            product_sku VARCHAR,
            amount DOUBLE,
            status VARCHAR,
            valid_from VARCHAR,
            valid_to VARCHAR,
            is_current BOOLEAN
        );
    """)


def load_scd2(con, valid_rows):
    stage_df = pl.DataFrame(valid_rows)
    con.register("staging_orders", stage_df)
    con.execute("""
        UPDATE dim_orders_scd2
        SET valid_to = staging_orders.updated_at, is_current = FALSE
        FROM staging_orders
        WHERE dim_orders_scd2.order_id = staging_orders.order_id
          AND dim_orders_scd2.is_current = TRUE;
    """)
    con.execute("""
        INSERT INTO dim_orders_scd2
        SELECT order_id, customer_email, product_sku, amount, status,
               updated_at AS valid_from, NULL AS valid_to, TRUE AS is_current
        FROM staging_orders;
    """)
    con.unregister("staging_orders")


def run_pipeline():
    last_wm = get_watermark()
    conn = sqlite3.connect("data/raw/ecommerce.db")
    df = pl.read_database(
        f"SELECT * FROM orders WHERE updated_at > '{last_wm}' ORDER BY updated_at ASC",
        connection=conn
    )
    conn.close()

    if df.is_empty():
        print("No new CDC records found since last watermark.")
        return

    run_ts = datetime.now(timezone.utc).isoformat()
    valid_rows, quarantine_rows = validate_rows(df.to_dicts(), run_ts)

    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    if valid_rows:
        pl.DataFrame(valid_rows).write_parquet(f"data/lakehouse/clean_{batch_id}.parquet")
    if quarantine_rows:
        bad_df = add_suggestions(pl.DataFrame(quarantine_rows, infer_schema_length=None))
        bad_df.write_parquet(f"data/quarantine/bad_{batch_id}.parquet")

    con = duckdb.connect(WAREHOUSE)
    init_warehouse(con)
    if valid_rows:
        load_scd2(con, valid_rows)

    save_watermark(df["updated_at"].max())

    summary = con.execute("""
        SELECT status, COUNT(*) AS active_orders, ROUND(SUM(amount), 2) AS total_revenue
        FROM dim_orders_scd2 WHERE is_current = TRUE GROUP BY status
    """).fetchall()
    history_count = con.execute("SELECT COUNT(*) FROM dim_orders_scd2 WHERE is_current = FALSE").fetchone()[0]
    con.close()

    print(f"CDC Processed: {len(df)} | Clean: {len(valid_rows)} | Quarantined: {len(quarantine_rows)}")
    print(f"SCD2 Historical Versions Archived: {history_count}")
    print("Active Warehouse Metrics:", summary)


def ingest_replayed(fixed_df):
    """Replay goes through the SAME contract + SCD2 load. Never touches the watermark
    and never writes new quarantine files. Returns {order_id: label}."""
    rows = fixed_df.select(ORDER_COLS).to_dicts()
    run_ts = datetime.now(timezone.utc).isoformat()
    con = duckdb.connect(WAREHOUSE)
    try:
        init_warehouse(con)
        latest = dict(con.execute(
            "SELECT order_id, MAX(valid_from) FROM dim_orders_scd2 GROUP BY order_id").fetchall())
        result, candidates = {}, []
        for r in rows:
            # a newer version already exists in the warehouse -> this old fix would overwrite fresh data
            if latest.get(r["order_id"], "") > r["updated_at"]:
                result[r["order_id"]] = "SUPERSEDED"
            else:
                candidates.append(r)
        valid, _still_bad = validate_rows(candidates, run_ts)
        if valid:
            load_scd2(con, valid)
        for r in valid:
            result[r["order_id"]] = "REPLAYED"
        return result
    finally:
        con.close()


def run_replay(auto_approve_min=None):
    stats = replay(ingest_replayed, "data/quarantine", auto_approve_min)
    print("Replay result:", stats or "nothing approved to replay")
    return stats


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--replay", action="store_true", help="re-ingest APPROVED quarantined rows")
    p.add_argument("--auto-approve", type=float, default=None,
                   help="auto-approve PENDING fixes with confidence >= this value (e.g. 0.9)")
    args = p.parse_args()
    if args.replay:
        run_replay(args.auto_approve)
    else:
        run_pipeline()
