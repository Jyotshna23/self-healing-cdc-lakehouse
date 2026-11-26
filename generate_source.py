import sqlite3, random, sys
from datetime import datetime, timedelta, timezone

def generate_data(mode="initial"):
    conn = sqlite3.connect("data/raw/ecommerce.db")
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        customer_email TEXT,
        product_sku TEXT,
        amount REAL,
        status TEXT,
        updated_at TEXT
    )
    """)
    statuses = ["PLACED", "SHIPPED", "DELIVERED"]
    now = datetime.now(timezone.utc)
    rows = []

    if mode == "initial":
        for i in range(1, 5001):
            email = f"user{i}@example.com" if i % 25 != 0 else None
            amount = round(random.uniform(15.0, 500.0), 2) if i % 40 != 0 else -99.0
            status = random.choice(statuses) if i % 50 != 0 else "CORRUPT_STATUS"
            ts = (now - timedelta(minutes=random.randint(10, 1440))).isoformat()
            rows.append((i, email, f"SKU-{random.randint(100,150)}", amount, status, ts))
        cur.executemany("INSERT OR REPLACE INTO orders VALUES (?,?,?,?,?,?)", rows)
        print("Batch 1: Generated 5,000 initial orders with injected schema/data anomalies.")
    else:
        for i in range(1, 51):
            ts = datetime.now(timezone.utc).isoformat()
            rows.append((i, f"user{i}@example.com", f"SKU-{random.randint(100,150)}", round(random.uniform(25.0, 450.0), 2), "DELIVERED", ts))
        for i in range(5001, 5101):
            ts = datetime.now(timezone.utc).isoformat()
            rows.append((i, f"user{i}@example.com", f"SKU-{random.randint(100,150)}", round(random.uniform(25.0, 450.0), 2), "PLACED", ts))
        cur.executemany("INSERT OR REPLACE INTO orders VALUES (?,?,?,?,?,?)", rows)
        print("Batch 2 (CDC Delta): Updated 50 existing orders + inserted 100 new orders.")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "initial"
    generate_data(mode)
