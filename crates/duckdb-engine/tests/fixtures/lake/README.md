# Lake fixtures

Two small tables holding the twelve orders in `samples/data/orders.csv`, written by each
format's own reference library so that `src.lake.delta` and `src.lake.iceberg` are tested
against tables they did not make themselves. Each was written in two commits, seven rows
and then five, so a reader that saw only the first or the last would be caught.

| Directory | Written by | Holds |
|---|---|---|
| `orders_delta/` | `deltalake` 1.6.5 (delta-rs) | two Parquet files, `_delta_log/` versions 0 and 1 |
| `orders_iceberg/` | `pyiceberg` 0.12.0, SQLite catalog | `data/` two Parquet files; `metadata/` three versions, two snapshots |

Columns: `order_id BIGINT`, `customer_id VARCHAR`, `order_ts TIMESTAMP`, `amount DECIMAL(10,2)`,
`status VARCHAR`. Twelve rows totalling 2264.46.

**The Iceberg table has been moved.** Its metadata records the absolute paths where it was
written, a scratch directory on the machine that made it. So it can only be read with
`allow_moved_paths = true`, with `path` set to `orders_iceberg/` itself, and `version` set to a
metadata file's name without `.metadata.json`. That is deliberate: it is how a copied table
arrives, and it is the case `src.lake.iceberg` got wrong until Phase 10c (see the tracker).

Made on 2026-09-23 with this script, run in a throwaway virtual environment
(`python -m venv v; v\Scripts\pip install deltalake "pyiceberg[pyarrow,sql-sqlite]"`):

```python
import csv, pathlib, shutil
from datetime import datetime
from decimal import Decimal
import pyarrow as pa
from deltalake import write_deltalake
from pyiceberg.catalog.sql import SqlCatalog

OUT = pathlib.Path("out"); shutil.rmtree(OUT, ignore_errors=True); OUT.mkdir()
schema = pa.schema([
    pa.field("order_id", pa.int64(), nullable=False), pa.field("customer_id", pa.string()),
    pa.field("order_ts", pa.timestamp("us")), pa.field("amount", pa.decimal128(10, 2)),
    pa.field("status", pa.string()),
])
rows = list(csv.DictReader(open("samples/data/orders.csv", newline="")))
def batch(part):
    return pa.Table.from_pylist([{
        "order_id": int(r["order_id"]), "customer_id": r["customer_id"],
        "order_ts": datetime.fromisoformat(r["order_ts"]), "amount": Decimal(r["amount"]),
        "status": r["status"]} for r in part], schema=schema)
first, second = batch(rows[:7]), batch(rows[7:])

write_deltalake(str(OUT / "orders_delta"), first)
write_deltalake(str(OUT / "orders_delta"), second, mode="append")

(OUT / "wh").mkdir()
# A plain path, not a file:// URI: pyiceberg on Windows turns the URI into "/C:/...".
catalog = SqlCatalog("fixtures", uri=f"sqlite:///{(OUT / 'catalog.db').as_posix()}",
                     warehouse=(OUT / "wh").as_posix())
catalog.create_namespace("sales")
table = catalog.create_table("sales.orders", schema=schema)
table.append(first); table.append(second)
# then copy out/orders_delta and out/wh/sales/orders here
```
