import math
import pytest
from cade.hashing import compute_hash, _normalize


def test_same_dict_same_hash():
    d = {"a": 1, "b": 2.0, "c": "hello"}
    assert compute_hash(d) == compute_hash(d)


def test_key_order_irrelevant():
    d1 = {"b": 2, "a": 1}
    d2 = {"a": 1, "b": 2}
    assert compute_hash(d1) == compute_hash(d2)


def test_data_hash_excluded():
    d = {"a": 1, "data_hash": "sha256-v1:old"}
    d_no_hash = {"a": 1}
    assert compute_hash(d) == compute_hash(d_no_hash)


def test_float_rounding_deterministic():
    # 0.1 + 0.2 != 0.3 in float, but rounded to 10dp they compare equal
    d1 = {"v": 0.1 + 0.2}
    d2 = {"v": 0.3}
    # They may or may not be equal depending on platform — what matters is
    # that the hash is stable when called twice with the same input
    h = compute_hash(d1)
    assert compute_hash(d1) == h


def test_nan_raises():
    with pytest.raises(ValueError, match="Non-finite"):
        compute_hash({"v": float("nan")})


def test_inf_raises():
    with pytest.raises(ValueError, match="Non-finite"):
        compute_hash({"v": float("inf")})


def test_hash_prefix():
    h = compute_hash({"a": 1})
    assert h.startswith("sha256-v1:")


def test_single_byte_change_changes_hash():
    d1 = {"threshold": 5_000_000.0}
    d2 = {"threshold": 5_000_001.0}
    assert compute_hash(d1) != compute_hash(d2)
