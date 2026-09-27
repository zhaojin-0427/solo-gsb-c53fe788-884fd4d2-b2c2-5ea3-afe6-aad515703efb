"""Canonicalisation and content fingerprinting.

The canonical form is semantics-preserving: anything that does not change
the meaning of a contract produces the same canonical document and hence
the same fingerprint. That includes

* object key order (JSON objects are serialised with sorted keys),
* record field order      -> fields are emitted sorted by stable id,
* enum symbol order       -> symbols are emitted sorted,
* union branch order      -> branches are emitted sorted by tag,
* shorthand forms         -> ``"int"`` vs ``{"type": "int"}`` and
  ``"User"`` vs ``{"type": "ref", "name": "User"}`` normalise identically.

Named types are inlined at their first occurrence in a deterministic
traversal; later occurrences become ``ref`` nodes, which is what keeps
recursive schemas finite. The fingerprint is SHA-256 over the canonical
UTF-8 document and is used as the content-addressed storage (CAS) key.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from .types import (
    ArrayType,
    EnumType,
    MapType,
    PrimType,
    RecordType,
    RefType,
    Schema,
    TypeNode,
    UnionType,
)


def canonical_document(schema: Schema) -> str:
    emitted: set[str] = set()

    def enc(node: TypeNode) -> Any:
        if isinstance(node, PrimType):
            return node.name
        if isinstance(node, RefType):
            return {"name": node.name, "type": "ref"}
        if isinstance(node, RecordType):
            if node.name in emitted:
                return {"name": node.name, "type": "ref"}
            emitted.add(node.name)
            fields = []
            for f in sorted(node.fields, key=lambda f: f.id):
                entry: dict[str, Any] = {
                    "id": f.id,
                    "name": f.name,
                    "type": enc(f.type),
                }
                if f.has_default:
                    entry["default"] = f.default
                fields.append(entry)
            return {"fields": fields, "name": node.name, "type": "record"}
        if isinstance(node, EnumType):
            if node.name in emitted:
                return {"name": node.name, "type": "ref"}
            emitted.add(node.name)
            return {
                "name": node.name,
                "symbols": sorted(node.symbols),
                "type": "enum",
            }
        if isinstance(node, UnionType):
            return {
                "branches": [
                    {"tag": b.tag, "type": enc(b.type)}
                    for b in sorted(node.branches, key=lambda b: b.tag)
                ],
                "type": "union",
            }
        if isinstance(node, ArrayType):
            return {"items": enc(node.items), "type": "array"}
        if isinstance(node, MapType):
            return {"type": "map", "values": enc(node.values)}
        raise TypeError(f"cannot canonicalise {node!r}")  # pragma: no cover

    doc = enc(schema.root)
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def fingerprint(canonical: str) -> str:
    """SHA-256 hex digest of a canonical document — the CAS key."""
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
