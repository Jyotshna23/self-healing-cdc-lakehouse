"""Suggests fixes for quarantined orders and replays approved ones (dead-letter replay)."""
from __future__ import annotations
import difflib, json, re
from pathlib import Path
import polars as pl

VALID_STATUS = ["PLACED", "SHIPPED", "DELIVERED"]
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
DOMAIN_TYPOS = {"gmial.com": "gmail.com", "gmal.com": "gmail.com", "gamil.com": "gmail.com",
                "yahooo.com": "yahoo.com", "hotmial.com": "hotmail.com", "outlok.com": "outlook.com"}
KNOWN = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"]
NEW_COLS = ["error_types", "suggested_fix", "fix_json", "fix_confidence", "review_status"]


def _issue(field, typ, value, conf, text):
    return {"field": field, "type": typ, "value": value, "confidence": conf, "text": text}


def check_amount(v):
    if v is None or str(v).strip() == "":
        return _issue("amount", "MISSING_AMOUNT", None, 0.0, "Amount missing - get it from the source system")
    try:
        x = float(str(v).replace("$", "").replace(",", "").strip())
    except ValueError:
        return _issue("amount", "INVALID_AMOUNT", None, 0.0, "Amount is not a number - manual check")
    if x < 0:
        return _issue("amount", "NEGATIVE_AMOUNT", round(abs(x), 2), 0.6,
                      f"Set amount to {abs(x):.2f} (abs). Could be a refund or placeholder - review first")
    if x == 0:
        return _issue("amount", "ZERO_AMOUNT", None, 0.0, "Amount is zero - verify with source system")
    return None


def check_email(v):
    if v is None or str(v).strip() == "":
        return _issue("customer_email", "MISSING_EMAIL", None, 0.0, "Email missing - flag for CRM lookup")
    e = str(v).strip().lower().replace(" ", "")
    if "@" in e:
        local, dom = e.rsplit("@", 1)
        if dom in DOMAIN_TYPOS:
            return _issue("customer_email", "INVALID_EMAIL", f"{local}@{DOMAIN_TYPOS[dom]}", 0.9,
                          f"Fix domain typo {dom} -> {DOMAIN_TYPOS[dom]}")
        if EMAIL_RE.match(e):
            if e == v:
                return None
            return _issue("customer_email", "INVALID_EMAIL", e, 0.95, "Trim spaces / lowercase")
        if "." not in dom and f"{dom}.com" in KNOWN:
            return _issue("customer_email", "INVALID_EMAIL", f"{local}@{dom}.com", 0.7,
                          f"Domain looks incomplete - add .com ({dom}.com)")
    else:
        for d in KNOWN:
            if e.endswith("." + d):
                return _issue("customer_email", "INVALID_EMAIL", f"{e[:-len(d) - 1]}@{d}", 0.7,
                              f"Missing @ before {d}")
    return _issue("customer_email", "INVALID_EMAIL", None, 0.0, "Invalid email, no safe fix - flag for CRM lookup")


def check_status(v):
    s = str(v).strip().upper() if v is not None else ""
    if s in VALID_STATUS:
        return None if s == v else _issue("status", "INVALID_STATUS", s, 0.95, "Fix case/spaces")
    m = difflib.get_close_matches(s, VALID_STATUS, n=1, cutoff=0.6)
    if m:
        conf = round(difflib.SequenceMatcher(None, s, m[0]).ratio(), 2)
        return _issue("status", "INVALID_STATUS", m[0], conf, f"Closest valid status: {m[0]}")
    return _issue("status", "INVALID_STATUS", None, 0.0, "No close valid status - manual review")


def suggest(row: dict) -> dict:
    """Re-checks every field (the pipeline only records the FIRST error per row)."""
    issues = [i for i in (check_amount(row.get("amount")),
                          check_email(row.get("customer_email")),
                          check_status(row.get("status"))) if i]
    if not issues:
        return {"error_types": "OTHER", "suggested_fix": "No auto-fix available - manual review",
                "fix_json": None, "fix_confidence": 0.0}
    fixable = all(i["value"] is not None for i in issues)
    return {
        "error_types": ",".join(i["type"] for i in issues),
        "suggested_fix": " | ".join(i["text"] for i in issues),
        "fix_json": json.dumps({i["field"]: i["value"] for i in issues}) if fixable else None,
        "fix_confidence": min(i["confidence"] for i in issues),
    }


def add_suggestions(df: pl.DataFrame) -> pl.DataFrame:
    """Call on bad rows right before writing bad_*.parquet."""
    df = df.drop([c for c in NEW_COLS if c in df.columns])
    out = [suggest(r) for r in df.to_dicts()]
    return df.with_columns(
        pl.Series("error_types", [o["error_types"] for o in out], dtype=pl.Utf8),
        pl.Series("suggested_fix", [o["suggested_fix"] for o in out], dtype=pl.Utf8),
        pl.Series("fix_json", [o["fix_json"] for o in out], dtype=pl.Utf8),
        pl.Series("fix_confidence", [o["fix_confidence"] for o in out], dtype=pl.Float64),
        pl.Series("review_status", ["PENDING"] * len(out), dtype=pl.Utf8),
    )


def apply_fixes(df: pl.DataFrame) -> pl.DataFrame:
    rows = df.to_dicts()
    for r in rows:
        r.update(json.loads(r["fix_json"]))
        for c in NEW_COLS:
            r.pop(c, None)
    return pl.DataFrame(rows, infer_schema_length=None)


def replay(ingest_fn, quarantine_dir="data/quarantine", auto_approve_min=None) -> dict:
    """Re-ingest APPROVED rows.
    ingest_fn(fixed_df) -> {order_id: "REPLAYED" | "SUPERSEDED" | ...}; missing ids = REPLAY_FAILED."""
    stats: dict = {}
    for f in sorted(Path(quarantine_dir).glob("bad_*.parquet")):
        df = pl.read_parquet(f)
        if "review_status" not in df.columns:
            continue
        if auto_approve_min is not None:
            df = df.with_columns(
                pl.when((pl.col("review_status") == "PENDING") & pl.col("fix_json").is_not_null()
                        & (pl.col("fix_confidence") >= auto_approve_min))
                .then(pl.lit("APPROVED")).otherwise(pl.col("review_status")).alias("review_status"))
        df = df.with_row_index("_i")
        todo = df.filter((pl.col("review_status") == "APPROVED") & pl.col("fix_json").is_not_null())
        if todo.is_empty():
            df.drop("_i").write_parquet(f.with_suffix(".tmp")); f.with_suffix(".tmp").replace(f)
            continue
        result = ingest_fn(apply_fixes(todo.drop("_i")))
        status = df["review_status"].to_list()
        for i, oid in zip(todo["_i"].to_list(), todo["order_id"].to_list()):
            label = result.get(oid, "REPLAY_FAILED")
            status[i] = label
            stats[label] = stats.get(label, 0) + 1
        tmp = f.with_suffix(".tmp")
        df.drop("_i").with_columns(pl.Series("review_status", status)).write_parquet(tmp)
        tmp.replace(f)
    return stats
