from __future__ import annotations

import os
from zoneinfo import ZoneInfo

import pandas as pd
from ib_insync import IB, Stock, util

from tools.dropbox_store import get_dbx, download_if_exists, upload_guarded


DROPBOX_PATH_DEFAULT = "/finance_data_platform/demo/spy_5m.csv"


def fetch_latest_spy_5m_bar(host: str, port: int, client_id: int) -> dict:
    ib = IB()
    ib.connect(host, port, clientId=client_id, timeout=10)

    try:
        contract = Stock("SPY", "ARCA", "USD")
        ib.qualifyContracts(contract)

        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="1800 S",
            barSizeSetting="5 mins",
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
            keepUpToDate=False,
        )

        if not bars:
            raise RuntimeError("No bars returned (check market data permissions, RTH, and connection).")

        df = util.df(bars)
        last = df.iloc[-1]

        ny = ZoneInfo("America/New_York")
        ts = pd.to_datetime(last["date"]).to_pydatetime()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=ZoneInfo("UTC"))
        ts_ny = ts.astimezone(ny)

        return {
            "ts_ny": ts_ny.isoformat(),
            "open": float(last["open"]),
            "high": float(last["high"]),
            "low": float(last["low"]),
            "close": float(last["close"]),
            "volume": int(last.get("volume", 0) or 0),
        }
    finally:
        ib.disconnect()


def append_row_to_csv_bytes(existing: bytes | None, row: dict) -> bytes:
    header = "ts_ny,open,high,low,close,volume\n"
    line = f'{row["ts_ny"]},{row["open"]},{row["high"]},{row["low"]},{row["close"]},{row["volume"]}\n'

    if not existing:
        return (header + line).encode("utf-8")

    text = existing.decode("utf-8", errors="replace")
    if not text.strip():
        text = header
    if not text.endswith("\n"):
        text += "\n"
    if not text.splitlines()[0].strip().startswith("ts_ny,open,high,low,close,volume"):
        text = header + text
    return (text + line).encode("utf-8")


def main() -> int:
    host = os.environ.get("IBKR_HOST", "127.0.0.1")
    port = int(os.environ.get("IBKR_PORT", "4001"))
    client_id = int(os.environ.get("IBKR_CLIENT_ID", "7"))
    dropbox_path = os.environ.get("DROPBOX_SPY_5M_PATH", DROPBOX_PATH_DEFAULT)

    row = fetch_latest_spy_5m_bar(host, port, client_id)
    print("[OK] Fetched bar:", row)

    dbx = get_dbx()
    dl = download_if_exists(dbx, dropbox_path)
    new_bytes = append_row_to_csv_bytes(dl.content, row)

    try:
        new_rev = upload_guarded(dbx, dropbox_path, new_bytes, dl.rev)
        print(f"[OK] Uploaded to Dropbox: {dropbox_path} (rev={new_rev})")
        return 0
    except Exception as e:
        print("[WARN] Upload failed, retrying once:", e)
        dl2 = download_if_exists(dbx, dropbox_path)
        new_bytes2 = append_row_to_csv_bytes(dl2.content, row)
        new_rev2 = upload_guarded(dbx, dropbox_path, new_bytes2, dl2.rev)
        print(f"[OK] Uploaded to Dropbox (retry): {dropbox_path} (rev={new_rev2})")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
