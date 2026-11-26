from pathlib import Path
import duckdb
import polars as pl
import streamlit as st
from quarantine_fixer import add_suggestions

DB_PATH = "data/lakehouse/warehouse.duckdb"
QDIR = "data/quarantine"
TABLE = "dim_orders_scd2"

st.set_page_config(page_title="CDC Lakehouse Observability", layout="wide")


def mask_email(e):
    if not e or "@" not in str(e):
        return e
    local, dom = str(e).split("@", 1)
    return f"{local[:1]}***@{dom}"


@st.cache_data(ttl=30)
def load_quarantine() -> pl.DataFrame:
    frames = []
    for f in sorted(Path(QDIR).glob("bad_*.parquet")):
        df = pl.read_parquet(f)
        if "suggested_fix" not in df.columns:      # files from before the upgrade
            df = add_suggestions(df)
        frames.append(df.with_row_index("_row").with_columns(pl.lit(str(f)).alias("_file")))
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


@st.cache_data(ttl=30)
def warehouse_stats():
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        versions = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        revenue = con.execute(f"SELECT COALESCE(SUM(amount),0) FROM {TABLE} WHERE is_current").fetchone()[0]
        latest = dict(con.execute(f"SELECT order_id, MAX(valid_from) FROM {TABLE} GROUP BY order_id").fetchall())
        return versions, float(revenue), latest
    finally:
        con.close()


def lifecycle(order_id: str):
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        return con.execute(f"SELECT * FROM {TABLE} WHERE CAST(order_id AS VARCHAR) = ? ORDER BY valid_from",
                           [order_id]).df()
    finally:
        con.close()


def save_status(changes):
    by_file = {}
    for f, r, s in changes:
        by_file.setdefault(f, {})[r] = s
    for f, m in by_file.items():
        df = pl.read_parquet(f)
        if "review_status" not in df.columns:
            df = add_suggestions(df)
        status = df["review_status"].to_list()
        for r, s in m.items():
            status[r] = s
        tmp = Path(f).with_suffix(".tmp")
        df.with_columns(pl.Series("review_status", status)).write_parquet(tmp)
        tmp.replace(f)


st.title("Self-Healing CDC Lakehouse - Observability")
if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
show_pii = st.sidebar.checkbox("Show raw customer emails (PII)", value=False)

try:
    versions, revenue, latest = warehouse_stats()
except Exception as e:
    st.error(f"Cannot read {DB_PATH}. Run the pipeline first, or wait if it is writing right now.\n\n{e}")
    st.stop()

q = load_quarantine()
replayed = superseded_n = 0
open_q = q
if q.height:
    newer = [latest.get(o, "") > (u or "") for o, u in zip(q["order_id"].to_list(), q["updated_at"].to_list())]
    q = q.with_columns(pl.Series("superseded", newer))
    is_final = pl.col("review_status").is_in(["REPLAYED", "SUPERSEDED"])
    replayed = q.filter(pl.col("review_status") == "REPLAYED").height
    superseded_n = q.filter((pl.col("superseded") | (pl.col("review_status") == "SUPERSEDED"))
                            & (pl.col("review_status") != "REPLAYED")).height
    open_q = q.filter(~is_final & ~pl.col("superseded"))

n_open = open_q.height
processed = versions + q.height - replayed          # replayed rows already sit in the warehouse
clean_rate = 100 * versions / processed if processed else 0
fixable = open_q.filter(pl.col("fix_json").is_not_null()).height if n_open else 0
approved_waiting = open_q.filter(pl.col("review_status") == "APPROVED").height if n_open else 0

a = st.columns(4)
a[0].metric("Total Processed", f"{processed:,}")
a[1].metric("Clean Rate", f"{clean_rate:.1f}%")
a[2].metric("Quarantined (open)", f"{n_open:,}")
a[3].metric("Active Revenue", f"{revenue:,.2f}")
b = st.columns(4)
b[0].metric("Fix Coverage (open)", f"{(100 * fixable / n_open) if n_open else 0:.0f}%")
b[1].metric("Self-healed by replay", f"{replayed:,}")
b[2].metric("Fixed upstream (superseded)", f"{superseded_n:,}")
b[3].metric("Approved, waiting for replay", f"{approved_waiting:,}")
st.caption("Clean Rate = rows that reached the warehouse / all rows processed. "
           "Superseded = a newer valid version of that order already exists, so the old bad row needs no fix.")

tab1, tab2 = st.tabs(["Quarantine", "Order history (SCD Type 2)"])

with tab1:
    if not n_open:
        st.success("No open quarantined rows.")
    else:
        c1, c2 = st.columns(2)
        errs = (open_q.select(pl.col("error_types").str.split(",")).explode("error_types")
                .group_by("error_types").len().sort("len", descending=True).to_pandas()
                .set_index("error_types"))
        c1.subheader("Errors by type")
        c1.bar_chart(errs)
        c1.caption("A row with several problems counts once per problem. "
                   "(The pipeline's own error_field keeps only the first.)")
        c2.subheader("Review status")
        c2.bar_chart(open_q.group_by("review_status").len().to_pandas().set_index("review_status"))

        types = sorted(set(errs.index))
        pick = st.multiselect("Filter by error type", types, default=types)
        view = open_q.filter(pl.col("error_types").str.contains("|".join(pick))) if pick else open_q.clear()
        if not show_pii and "customer_email" in view.columns:
            view = view.with_columns(pl.col("customer_email").map_elements(mask_email, return_dtype=pl.Utf8))
        pdf = view.to_pandas()
        show = [c for c in ["order_id", "customer_email", "amount", "status", "error_types",
                            "suggested_fix", "fix_confidence", "review_status", "error_reason"] if c in pdf.columns]
        edited = st.data_editor(
            pdf, column_order=show, key="qeditor",
            disabled=[c for c in pdf.columns if c != "review_status"],
            column_config={"review_status": st.column_config.SelectboxColumn(
                "review_status", options=["PENDING", "APPROVED", "REJECTED"])})
        if st.button("Save approvals"):
            diff = edited[edited["review_status"] != pdf["review_status"]]
            save_status([(r["_file"], int(r["_row"]), r["review_status"]) for _, r in diff.iterrows()])
            st.cache_data.clear()
            st.success(f"Saved {len(diff)} change(s). Now run: python pipeline.py --replay")

with tab2:
    oid = st.text_input("Order ID", placeholder="e.g. 7")
    if oid:
        h = lifecycle(oid.strip())
        if h.empty:
            st.warning("No versions found for this order_id.")
        else:
            skip = {"valid_from", "valid_to", "is_current"}
            changes = [""]
            for i in range(1, len(h)):
                ch = [f"{c}: {h.iloc[i-1][c]} -> {h.iloc[i][c]}" for c in h.columns
                      if c not in skip and h.iloc[i-1][c] != h.iloc[i][c]]
                changes.append("; ".join(ch))
            h["what_changed"] = changes
            if not show_pii:
                h["customer_email"] = h["customer_email"].map(mask_email)
            st.caption(f"{len(h)} version(s), oldest first")
            st.dataframe(h)
