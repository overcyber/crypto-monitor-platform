from __future__ import annotations

from functools import lru_cache

from .config import settings


@lru_cache(maxsize=1)
def catalog():
    from pyiceberg.catalog import load_catalog
    s = settings()
    return load_catalog(
        "lakekeeper",
        **{
            "type": "rest",
            "uri": s.lakekeeper_uri,
            "warehouse": s.iceberg_warehouse,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": s.s3_endpoint,
            "s3.access-key-id": s.s3_access_key,
            "s3.secret-access-key": s.s3_secret_key,
            "s3.region": s.s3_region,
            "s3.force-virtual-addressing": "false",
        },
    )


def raw_table():
    return catalog().load_table("market.raw_events")
