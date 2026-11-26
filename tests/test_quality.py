import duckdb, glob

def test_lakehouse_files_created():
    assert len(glob.glob("data/lakehouse/clean_*.parquet")) >= 1
    assert len(glob.glob("data/quarantine/bad_*.parquet")) >= 1

def test_no_corrupt_data_in_warehouse():
    con = duckdb.connect("data/lakehouse/warehouse.duckdb")
    bad_amounts = con.execute("SELECT COUNT(*) FROM dim_orders_scd2 WHERE amount <= 0").fetchone()[0]
    null_emails = con.execute("SELECT COUNT(*) FROM dim_orders_scd2 WHERE customer_email IS NULL").fetchone()[0]
    assert bad_amounts == 0
    assert null_emails == 0

def test_scd2_uniqueness_and_history():
    con = duckdb.connect("data/lakehouse/warehouse.duckdb")
    duplicate_active = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT order_id FROM dim_orders_scd2 WHERE is_current = TRUE GROUP BY order_id HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    archived = con.execute("SELECT COUNT(*) FROM dim_orders_scd2 WHERE is_current = FALSE").fetchone()[0]
    assert duplicate_active == 0
    assert archived > 0
