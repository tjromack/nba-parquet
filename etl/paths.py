from __future__ import annotations

import os
from pathlib import Path

LOCAL_OUTPUT_ENV = "LOCAL_OUTPUT_DIR"


def is_local_mode() -> bool:
    return bool(os.environ.get(LOCAL_OUTPUT_ENV))


def resolve_output_uri(s3_bucket: str | None, suffix: str) -> str:
    """Return a Spark-friendly URI for either local disk or S3A.

    If LOCAL_OUTPUT_DIR is set, writes go to ``{LOCAL_OUTPUT_DIR}/{suffix}``
    using a ``file://`` URI (works on Windows + Linux). Otherwise an S3A URI
    is built from ``s3_bucket``.
    """
    suffix = suffix.strip("/")
    local = os.environ.get(LOCAL_OUTPUT_ENV)
    if local:
        base = Path(local).expanduser().resolve()
        target = base / suffix
        target.mkdir(parents=True, exist_ok=True)
        # Build file URI manually so partition tokens like ``season=2025`` are
        # preserved (Path.as_uri percent-encodes the ``=`` character).
        normalized = str(target).replace("\\", "/")
        if not normalized.startswith("/"):
            normalized = "/" + normalized
        return f"file://{normalized}/"
    if not s3_bucket:
        raise ValueError("S3_BUCKET is required when LOCAL_OUTPUT_DIR is not set")
    return f"s3a://{s3_bucket}/{suffix}/"
