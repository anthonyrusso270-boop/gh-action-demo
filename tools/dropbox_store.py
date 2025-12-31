from __future__ import annotations

import os
from dataclasses import dataclass

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import WriteMode


@dataclass
class Downloaded:
    content: bytes | None
    rev: str | None


def get_dbx() -> dropbox.Dropbox:
    return dropbox.Dropbox(
        oauth2_refresh_token=os.environ["DROPBOX_REFRESH_TOKEN"],
        app_key=os.environ["DROPBOX_APP_KEY"],
        app_secret=os.environ["DROPBOX_APP_SECRET"],
    )


def download_if_exists(dbx: dropbox.Dropbox, path: str) -> Downloaded:
    try:
        md, res = dbx.files_download(path)
        return Downloaded(content=res.content, rev=md.rev)
    except ApiError as e:
        if e.error.is_path() and e.error.get_path().is_not_found():
            return Downloaded(content=None, rev=None)
        raise


def upload_guarded(dbx: dropbox.Dropbox, path: str, data: bytes, rev: str | None) -> str:
    mode = WriteMode.update(rev) if rev else WriteMode.overwrite
    md = dbx.files_upload(data, path, mode=mode, mute=True)
    return md.rev
