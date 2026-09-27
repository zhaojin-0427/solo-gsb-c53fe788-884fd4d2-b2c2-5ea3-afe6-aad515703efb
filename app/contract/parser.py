"""Parsing and validation of contract schema documents.

Two passes over the raw JSON document:

1. ``collect`` registers every inline named type (record/enum) so that
   forward references and recursive references resolve.
2. ``build`` constructs the type-node graph. Named nodes are memoised by
   name *before* their children are built, which is what makes local
   recursion (a record referring to itself) representable.

Defaults are validated after the whole graph exists, so a default on a
recursive record type does not trip over a half-built node.
"""
from __future__ import annotations

import re
from typing import Any

from .types import (
    PRIMITIVES,
    ArrayType,
    Branch,
    EnumType,
    Field,
    MapType,
    PrimType,
    RecordType,
    RefType,
    Schema,
    TypeNode,
    UnionType,
)

NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SchemaError(ValueError):
    """Raised when a contract schema document is invalid."""


def parse_schema(content: Any) -> Schema:
    """Validate a raw JSON document and return the parsed :class:`Schema`."""
    parser = _Parser()
    parser.collect(content, "$")
    root = parser.build(content, "$")
    schema = Schema(root=root, names=parser.nodes)
    for fld, path in parser.pending_defaults:
        _validate_default(fld.type, fld.default, schema.names, path, frozenset())
    return schema


class _Parser:
    def __init__(self) -> None:
        self.raw_names: dict[str, Any] = {}
        self.nodes: dict[str, TypeNode] = {}
        self.pending_defaults: list[tuple[Field, str]] = []

    # -- pass 1: register named types -------------------------------------

    def collect(self, raw: Any, path: str) -> None:
        if not isinstance(raw, dict):
            return
        t = raw.get("type")
        if t in ("record", "enum"):
            name = raw.get("name")
            if isinstance(name, str) and NAME_RE.match(name):
                if name in self.raw_names:
                    raise SchemaError(f"{path}: duplicate type name '{name}'")
                self.raw_names[name] = raw
            # malformed names are reported by build()
        children: list[Any] = []
        if t == "record":
            fields = raw.get("fields")
            if isinstance(fields, list):
                children = [f.get("type") for f in fields if isinstance(f, dict)]
        elif t == "array":
            children = [raw.get("items")]
        elif t == "map":
            children = [raw.get("values")]
        elif t == "union":
            branches = raw.get("branches")
            if isinstance(branches, list):
                children = [b.get("type") for b in branches if isinstance(b, dict)]
        for child in children:
            if child is not None:
                self.collect(child, path)

    # -- pass 2: build the node graph -------------------------------------

    def build(self, raw: Any, path: str) -> TypeNode:
        if isinstance(raw, str):
            if raw in PRIMITIVES:
                return PrimType(raw)
            if raw in self.raw_names:
                return RefType(raw)
            raise SchemaError(
                f"{path}: unknown type '{raw}' "
                "(not one of int/long/string/bytes, not a named type)"
            )
        if not isinstance(raw, dict):
            raise SchemaError(
                f"{path}: type must be an object or a type-name string, "
                f"got {type(raw).__name__}"
            )
        t = raw.get("type")
        if t is None:
            raise SchemaError(f"{path}: missing 'type'")
        if isinstance(t, str) and t in PRIMITIVES:
            return PrimType(t)
        if t == "record":
            return self._build_record(raw, path)
        if t == "enum":
            return self._build_enum(raw, path)
        if t == "union":
            return self._build_union(raw, path)
        if t == "array":
            if "items" not in raw:
                raise SchemaError(f"{path}: array type requires 'items'")
            return ArrayType(self.build(raw["items"], f"{path}.items"))
        if t == "map":
            if "values" not in raw:
                raise SchemaError(f"{path}: map type requires 'values'")
            return MapType(self.build(raw["values"], f"{path}.values"))
        if t == "ref":
            name = raw.get("name")
            if not isinstance(name, str) or name not in self.raw_names:
                raise SchemaError(f"{path}: ref to unknown named type {name!r}")
            return RefType(name)
        raise SchemaError(f"{path}: unknown type {t!r}")

    def _named(self, raw: dict, path: str, kind: str) -> str:
        name = raw.get("name")
        if not isinstance(name, str) or not NAME_RE.match(name):
            raise SchemaError(
                f"{path}: {kind} requires a 'name' matching {NAME_RE.pattern}"
            )
        return name

    def _build_record(self, raw: dict, path: str) -> RecordType:
        name = self._named(raw, path, "record")
        if name in self.nodes:  # recursion: return the partially built node
            return self.nodes[name]  # type: ignore[return-value]
        node = RecordType(name=name, fields=[])
        self.nodes[name] = node
        fields = raw.get("fields")
        if not isinstance(fields, list):
            raise SchemaError(f"{path}: record '{name}' requires a 'fields' list")
        seen_ids: set[int] = set()
        seen_names: set[str] = set()
        for i, fraw in enumerate(fields):
            fpath = f"{path}.fields[{i}]"
            fld = self._build_field(fraw, fpath)
            if fld.id in seen_ids:
                raise SchemaError(
                    f"{fpath}: duplicate field id {fld.id} in record '{name}'"
                )
            if fld.name in seen_names:
                raise SchemaError(
                    f"{fpath}: duplicate field name '{fld.name}' in record '{name}'"
                )
            seen_ids.add(fld.id)
            seen_names.add(fld.name)
            node.fields.append(fld)
        return node

    def _build_field(self, raw: Any, path: str) -> Field:
        if not isinstance(raw, dict):
            raise SchemaError(f"{path}: field must be an object")
        fid = raw.get("id")
        if isinstance(fid, bool) or not isinstance(fid, int) or fid < 0:
            raise SchemaError(f"{path}: field 'id' must be a non-negative integer")
        fname = raw.get("name")
        if not isinstance(fname, str) or not NAME_RE.match(fname):
            raise SchemaError(f"{path}: field 'name' must match {NAME_RE.pattern}")
        if "type" not in raw:
            raise SchemaError(f"{path}: field '{fname}' is missing 'type'")
        ftype = self.build(raw["type"], f"{path}({fname})")
        fld = Field(
            id=fid,
            name=fname,
            type=ftype,
            has_default="default" in raw,
            default=raw.get("default"),
        )
        if fld.has_default:
            self.pending_defaults.append((fld, f"{path}({fname}).default"))
        return fld

    def _build_enum(self, raw: dict, path: str) -> EnumType:
        name = self._named(raw, path, "enum")
        if name in self.nodes:
            return self.nodes[name]  # type: ignore[return-value]
        symbols = raw.get("symbols")
        if (
            not isinstance(symbols, list)
            or not symbols
            or not all(isinstance(s, str) and s for s in symbols)
        ):
            raise SchemaError(
                f"{path}: enum '{name}' requires a non-empty 'symbols' list "
                "of non-empty strings"
            )
        if len(set(symbols)) != len(symbols):
            raise SchemaError(f"{path}: enum '{name}' has duplicate symbols")
        node = EnumType(name=name, symbols=list(symbols))
        self.nodes[name] = node
        return node

    def _build_union(self, raw: dict, path: str) -> UnionType:
        branches = raw.get("branches")
        if not isinstance(branches, list) or not branches:
            raise SchemaError(f"{path}: union requires a non-empty 'branches' list")
        seen_tags: set[str] = set()
        out: list[Branch] = []
        for i, braw in enumerate(branches):
            bpath = f"{path}.branches[{i}]"
            if not isinstance(braw, dict):
                raise SchemaError(f"{bpath}: union branch must be an object")
            tag = braw.get("tag")
            if not isinstance(tag, str) or not tag:
                raise SchemaError(
                    f"{bpath}: union branch requires a non-empty string 'tag'"
                )
            if tag in seen_tags:
                raise SchemaError(f"{bpath}: duplicate union branch tag '{tag}'")
            seen_tags.add(tag)
            if "type" not in braw:
                raise SchemaError(f"{bpath}: union branch '{tag}' is missing 'type'")
            out.append(Branch(tag=tag, type=self.build(braw["type"], bpath)))
        return UnionType(branches=out)


def _validate_default(
    node: TypeNode,
    value: Any,
    names: dict[str, TypeNode],
    path: str,
    seen: frozenset,
) -> None:
    """Light-weight validation of a field default against its type.

    Record defaults are objects keyed by field name; union defaults are
    single-key objects ``{<tag>: <value>}``.
    """
    while isinstance(node, RefType):
        if node.name in seen:  # recursive type: stop descending
            return
        seen = seen | {node.name}
        node = names[node.name]
    if isinstance(node, PrimType):
        ok = (
            (type(value) is int)
            if node.name in ("int", "long")
            else isinstance(value, str)
        )
        if not ok:
            raise SchemaError(
                f"{path}: default {value!r} is not a valid {node.name}"
            )
        return
    if isinstance(node, EnumType):
        if value not in node.symbols:
            raise SchemaError(
                f"{path}: default {value!r} is not a symbol of enum '{node.name}'"
            )
        return
    if isinstance(node, ArrayType):
        if not isinstance(value, list):
            raise SchemaError(f"{path}: array default must be a list")
        for i, item in enumerate(value):
            _validate_default(node.items, item, names, f"{path}[{i}]", seen)
        return
    if isinstance(node, MapType):
        if not isinstance(value, dict):
            raise SchemaError(f"{path}: map default must be an object")
        for k, v in value.items():
            _validate_default(node.values, v, names, f"{path}.{k}", seen)
        return
    if isinstance(node, RecordType):
        if not isinstance(value, dict):
            raise SchemaError(
                f"{path}: record default must be an object keyed by field name"
            )
        by_name = {f.name: f for f in node.fields}
        for k, v in value.items():
            fld = by_name.get(k)
            if fld is None:
                raise SchemaError(
                    f"{path}: unknown field '{k}' in default for record "
                    f"'{node.name}'"
                )
            _validate_default(fld.type, v, names, f"{path}.{k}", seen)
        return
    if isinstance(node, UnionType):
        if not isinstance(value, dict) or len(value) != 1:
            raise SchemaError(
                f"{path}: union default must be an object with exactly one "
                "key: the branch tag"
            )
        (tag, inner), = value.items()
        branch = next((b for b in node.branches if b.tag == tag), None)
        if branch is None:
            raise SchemaError(f"{path}: no union branch tagged '{tag}'")
        _validate_default(branch.type, inner, names, f"{path}<{tag}>", seen)
        return
    raise SchemaError(f"{path}: cannot validate default")  # pragma: no cover
