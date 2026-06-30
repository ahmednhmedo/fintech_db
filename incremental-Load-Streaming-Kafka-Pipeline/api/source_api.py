"""
FastAPI — simulated SOURCE SYSTEM for the incremental pipeline.

Real banks do not let downstream jobs read CSV files directly; they expose a
transaction feed through an API. This service plays that role: it sits in front
of `data/raw/fact_transactions.csv` and hands out ONE transaction per request,
exactly like a core-banking / payment-switch REST endpoint would.

Endpoints
---------
GET /transactions/next   -> the next transaction row as JSON (wraps to the
                            first row again after the last one)
GET /health              -> liveness probe + how many rows served so far

Design notes
------------
* The CSV is *streamed* row-by-row with a persistent file handle (not loaded
  fully into memory) — the file has ~1,000,000 rows, so this keeps the source
  light and behaves like a real continuous feed.
* A threading.Lock guards the cursor so concurrent requests never read a torn
  row (uvicorn may serve requests from a worker thread pool).
* Empty CSV fields ("") are returned as JSON `null`. Type-casting is NOT done
  here — that is the Silver layer's job. Bronze stays raw.

Run:
    uvicorn source_api:app --host 0.0.0.0 --port 8000
    (run from this `incremental/api/` folder, or use the module path shown in
     the README)
"""
import csv
import os
import threading

from fastapi import FastAPI, HTTPException

# Resolve the CSV path relative to the PROJECT ROOT (two levels up from this
# file: incremental/api/ -> incremental/ -> project root), so the API works no
# matter which directory uvicorn is launched from. Overridable via env var.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "..", ".."))
CSV_PATH = os.environ.get(
    "FACT_CSV_PATH",
    os.path.join(_PROJECT_ROOT, "data", "raw", "fact_transactions.csv"),
)


class TransactionSource:
    """Hands out one CSV row at a time, looping forever (cyclic feed)."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self.served = 0
        self._fh = None
        self._reader = None
        self._open()

    def _open(self) -> None:
        """(Re)open the CSV and build a fresh DictReader positioned at row 1."""
        if self._fh is not None:
            self._fh.close()
        self._fh = open(self.path, newline="", encoding="utf-8")
        self._reader = csv.DictReader(self._fh)

    def next_row(self) -> dict:
        """Return the next row as a dict; restart from the top after the last."""
        with self._lock:
            row = next(self._reader, None)
            if row is None:                 # reached end of file -> wrap around
                self._open()
                row = next(self._reader, None)
                if row is None:             # file truly has no data rows
                    raise RuntimeError("fact_transactions.csv contains no data rows")
            self.served += 1
            # Empty string -> null; everything else stays a raw string (Bronze).
            return {k: (v if v != "" else None) for k, v in row.items()}


app = FastAPI(title="Fintech Transaction Source (simulated)", version="1.0.0")

# Build the source at import time so a missing CSV fails fast and loudly.
if not os.path.exists(CSV_PATH):
    raise FileNotFoundError(
        f"Source CSV not found: {CSV_PATH}\n"
        f"Set FACT_CSV_PATH or run from the project so the default path resolves."
    )
source = TransactionSource(CSV_PATH)


@app.get("/transactions/next")
def next_transaction() -> dict:
    """Return the next single transaction, advancing the cursor."""
    try:
        return source.next_row()
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/health")
def health() -> dict:
    """Liveness probe + simple served-rows counter."""
    return {"status": "ok", "csv": CSV_PATH, "rows_served": source.served}
