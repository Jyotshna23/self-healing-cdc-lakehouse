import duckdb, glob
import polars as pl
from pipeline import run_replay


def test_replay_keeps_warehouse_clean():
    """Runs on the data CI just generated. Replay must not corrupt the warehouse or break SCD2."""
    stats = run_replay(auto_approve_min=0.5)
    con = duckdb.connect("data/lakehouse/warehouse.duckdb")
    assert con.execute("SELECT COUNT(*) FROM dim_orders_scd2 WHERE amount <= 0").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM dim_orders_scd2 WHERE customer_email IS NULL").fetchone()[0] == 0
    assert con.execute("""SELECT COUNT(*) FROM (SELECT order_id FROM dim_orders_scd2
                          WHERE is_current = TRUE GROUP BY order_id HAVING COUNT(*) > 1)""").fetchone()[0] == 0
    assert stats.get("REPLAY_FAILED", 0) == 0
    done = sum(pl.read_parquet(f).filter(pl.col("review_status") == "REPLAYED").height
               for f in glob.glob("data/quarantine/bad_*.parquet"))
    assert done > 0   # holds on re-runs too
