"""
Turn flat PLC symbol names into nested data.

Copyright (c) 2024 Loupe, https://loupe.team
Part of Omni-Utils, licensed under the MIT License.

This is the parser the Beckhoff and B&R bridges each carried a copy of. The only
difference between the copies was the separator set, which is now a parameter.
"""

import re
from functools import lru_cache
from typing import Any, Mapping


@lru_cache(maxsize=8)
def _splitter(separators: str):
    if not separators:
        raise ValueError("symbol_separators must not be empty")
    return re.compile("[" + re.escape(separators) + "]")


def _ensure_list_with_index(list_name: str, target: dict, index: int) -> list:
    """
    Ensure target[list_name] is a list long enough to hold index, replacing
    whatever else was stored under that name.
    """
    existing = target.get(list_name)
    if not isinstance(existing, list):
        existing = target[list_name] = []
    if index >= len(existing):
        existing.extend([None] * (index - len(existing) + 1))
    return existing


def _split_index(part: str, symbol: str):
    """
    'Axes[3]' -> ('Axes', 3); 'Axes' -> ('Axes', None).

    Raises ValueError, naming the symbol, for anything else: a multi-dimensional
    index ('a[0,1]'), an array of arrays ('a[1][2]'), a stray bracket. The
    caller can then report which symbol it cannot represent.
    """
    if "[" not in part:
        return part, None
    name, _, index = part.partition("[")
    if part.count("[") != 1 or not index.endswith("]"):
        raise ValueError(f"cannot index symbol '{symbol}': '{part}'")
    try:
        return name, int(index[:-1])
    except ValueError:
        raise ValueError(f"cannot index symbol '{symbol}': '{part}'") from None


def nest_symbol(target: dict, symbol: str, value: Any, separators: str = ".") -> dict:
    """
    Write one flat symbol into a nested dict, in place.

    "a.b.c" becomes nested dicts, "arr[2]" a list padded with None,
    "arr[2].x" a dict inside that list. A value of a different kind already
    stored at a position is replaced.

    This is done on every read rather than cached, so a symbol that is no longer
    read does not linger at its last value.

    Args:
        target: the dict to write into.
        symbol: flat name, e.g. "GVL.Axes[0].Pos" or "Program:struct.member".
        value: the value to store.
        separators: the characters that separate parts of the name.

    Returns:
        target.

    Raises:
        ValueError: for an index the parser cannot represent, e.g. "a[0,1]".
    """
    parts = _splitter(separators).split(symbol)
    node = target
    for part in parts[:-1]:
        name, index = _split_index(part, symbol)
        if index is None:
            child = node.get(name)
            if not isinstance(child, dict):
                child = node[name] = {}
        else:
            items = _ensure_list_with_index(name, node, index)
            child = items[index]
            if not isinstance(child, dict):
                child = items[index] = {}
        node = child

    name, index = _split_index(parts[-1], symbol)
    if index is None:
        node[name] = value
    else:
        _ensure_list_with_index(name, node, index)[index] = value
    return target


def nest(flat: Mapping[str, Any], separators: str = ".") -> dict:
    """Nest every symbol of a flat symbol -> value mapping into one new dict."""
    nested = {}
    for symbol, value in flat.items():
        nest_symbol(nested, symbol, value, separators)
    return nested
