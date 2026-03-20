import hashlib
import json
import math
from typing import Any


def _normalize(obj: Any) -> Any:
    """Recursively normalize for canonical JSON.

    - dicts: sort keys
    - floats: round to 10dp (deterministic across Python versions)
    - NaN/Inf: raise ValueError
    - lists: normalize each element
    """
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            raise ValueError(f"Non-finite float in snapshot: {obj}")
        return round(obj, 10)
    if isinstance(obj, dict):
        return {k: _normalize(v) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        return [_normalize(v) for v in obj]
    return obj


def compute_hash(snapshot_dict: dict) -> str:
    """Compute canonical sha256 hash of a COBSnapshot dict.

    Excludes the 'data_hash' field from computation. Returns a string
    prefixed with 'sha256-v1:' to allow future algorithm migration.
    """
    d = {k: v for k, v in snapshot_dict.items() if k != "data_hash"}
    canonical = json.dumps(_normalize(d), separators=(',', ':'))
    return "sha256-v1:" + hashlib.sha256(canonical.encode()).hexdigest()
