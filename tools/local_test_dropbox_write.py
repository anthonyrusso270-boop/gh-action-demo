from __future__ import annotations
import os
from datetime import datetime, timezone

from tools.dropbox_store import get_dbx, download_if_exists, upload_guarded

PATH = os.environ.get("DROPBOX_TEST_PATH", "/finance_data_platform/_smoke/hello.csv")

def main():
    dbx = get_dbx()
    acct = dbx.users_get_current_account()
    print("[OK] Dropbox auth as:", acct.name.display_name)

    dl = download_if_exists(dbx, PATH)
    now = datetime.now(timezone.utc).isoformat()
    header = "ts_utc,note\n"
    line = f"{now},local_dropbox_test\n"

    if not dl.content:
        new_data = (header + line).encode("utf-8")
    else:
        txt = dl.content.decode("utf-8", errors="replace")
        if not txt.startswith("ts_utc,note"):
            txt = header + txt
        if not txt.endswith("\n"):
            txt += "\n"
        new_data = (txt + line).encode("utf-8")

    rev = upload_guarded(dbx, PATH, new_data, dl.rev)
    print("[OK] Wrote Dropbox file:", PATH, "rev=", rev)

if __name__ == "__main__":
    main()
