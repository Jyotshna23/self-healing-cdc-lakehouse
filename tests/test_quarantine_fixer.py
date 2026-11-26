import json
import polars as pl
from quarantine_fixer import suggest, add_suggestions, replay


def row(**k):
    return {"order_id": 1, "customer_email": "a@b.com", "amount": 10.0, "status": "PLACED", **k}


def test_negative_amount():
    assert json.loads(suggest(row(amount=-50))["fix_json"]) == {"amount": 50.0}


def test_status_fuzzy():
    assert json.loads(suggest(row(status="SHIPED"))["fix_json"]) == {"status": "SHIPPED"}


def test_garbage_status_not_guessed():
    assert suggest(row(status="CORRUPT_STATUS"))["fix_json"] is None


def test_missing_email_flagged_for_crm():
    r = suggest(row(customer_email=None))
    assert r["fix_json"] is None and "CRM" in r["suggested_fix"]


def test_domain_typo():
    assert json.loads(suggest(row(customer_email="x@gmial.com"))["fix_json"]) == {"customer_email": "x@gmail.com"}


def test_multi_error_row_is_not_auto_fixable():
    r = suggest(row(amount=-1, customer_email=None))
    assert "NEGATIVE_AMOUNT" in r["error_types"] and "MISSING_EMAIL" in r["error_types"]
    assert r["fix_json"] is None


def test_replay_roundtrip(tmp_path):
    df = add_suggestions(pl.DataFrame([row(amount=-5.0)])).with_columns(pl.lit("APPROVED").alias("review_status"))
    df.write_parquet(tmp_path / "bad_1.parquet")
    stats = replay(lambda d: {i: "REPLAYED" for i in d["order_id"].to_list()}, str(tmp_path))
    assert stats == {"REPLAYED": 1}
    assert pl.read_parquet(tmp_path / "bad_1.parquet")["review_status"][0] == "REPLAYED"
