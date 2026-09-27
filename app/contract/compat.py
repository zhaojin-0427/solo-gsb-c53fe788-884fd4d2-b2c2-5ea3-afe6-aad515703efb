"""Reader/writer compatibility checking.

``check_schema(reader, writer)`` answers: *can data written with the
``writer`` contract be read with the ``reader`` contract?*  The rules:

* ``int`` promotes to ``long`` (writer int -> reader long); every other
  primitive change is incompatible;
* records align fields by **stable id**: the reader may ignore writer
  fields it does not know, but every reader field that is absent from the
  writer must declare a default (i.e. a new *required* field needs a
  default);
* the reader enum must cover every writer symbol;
* every writer union branch maps — by tag, hence uniquely — to a reader
  branch whose type must be compatible;
* arrays and maps recurse into their element/value types.

Recursion terminates because named-type pairs already being checked are
tracked in ``visited``: reaching the same (reader, writer) pair again
means the cycle is compatible by coinduction.
"""
from __future__ import annotations

from dataclasses import dataclass

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


@dataclass(frozen=True)
class Violation:
    rule: str
    path: tuple[str, ...]
    message: str


def render_path(path: tuple[str, ...]) -> str:
    """Render a violation path JSONPath-style: ``$.a.b[].c``."""
    out = "$"
    for seg in path:
        out += seg if seg in ("[]", "{}") else f".{seg}"
    return out


def select_violation(violations: list[Violation]) -> Violation:
    """Shortest path wins; ties break lexicographically."""
    return min(violations, key=lambda v: (len(v.path), v.path))


def check_schema(reader: Schema, writer: Schema) -> list[Violation]:
    return _check(reader.root, writer.root, (), frozenset(), reader.names, writer.names)


def _resolve(node: TypeNode, names: dict[str, TypeNode]) -> TypeNode:
    while isinstance(node, RefType):
        node = names[node.name]
    return node


def _kind(node: TypeNode) -> str:
    if isinstance(node, PrimType):
        return node.name
    if isinstance(node, RecordType):
        return "record"
    if isinstance(node, EnumType):
        return "enum"
    if isinstance(node, UnionType):
        return "union"
    if isinstance(node, ArrayType):
        return "array"
    return "map"


def _check(
    r: TypeNode,
    w: TypeNode,
    path: tuple[str, ...],
    visited: frozenset,
    rn: dict[str, TypeNode],
    wn: dict[str, TypeNode],
) -> list[Violation]:
    r = _resolve(r, rn)
    w = _resolve(w, wn)

    if isinstance(r, RecordType) and isinstance(w, RecordType):
        key = (r.name, w.name)
        if key in visited:  # cycle: compatible by coinduction
            return []
        visited = visited | {key}
        return _check_records(r, w, path, visited, rn, wn)

    if isinstance(r, PrimType) and isinstance(w, PrimType):
        if r.name == w.name or (w.name == "int" and r.name == "long"):
            return []
        return [
            Violation(
                "primitive_type_mismatch",
                path,
                f"writer type '{w.name}' cannot be read as reader type "
                f"'{r.name}' (only int -> long promotion is allowed)",
            )
        ]

    if isinstance(r, EnumType) and isinstance(w, EnumType):
        missing = sorted(set(w.symbols) - set(r.symbols))
        if missing:
            return [
                Violation(
                    "enum_symbols_not_covered",
                    path,
                    f"reader enum '{r.name}' does not cover writer symbol(s): "
                    + ", ".join(missing),
                )
            ]
        return []

    if isinstance(r, UnionType) and isinstance(w, UnionType):
        violations: list[Violation] = []
        r_by_tag = {b.tag: b for b in r.branches}
        for wb in sorted(w.branches, key=lambda b: b.tag):
            rb = r_by_tag.get(wb.tag)
            if rb is None:
                violations.append(
                    Violation(
                        "union_branch_missing",
                        path + (wb.tag,),
                        f"writer union branch '{wb.tag}' has no matching "
                        "reader branch",
                    )
                )
            else:
                violations += _check(rb.type, wb.type, path + (wb.tag,), visited, rn, wn)
        return violations

    if isinstance(r, ArrayType) and isinstance(w, ArrayType):
        return _check(r.items, w.items, path + ("[]",), visited, rn, wn)

    if isinstance(r, MapType) and isinstance(w, MapType):
        return _check(r.values, w.values, path + ("{}",), visited, rn, wn)

    return [
        Violation(
            "type_kind_mismatch",
            path,
            f"reader kind '{_kind(r)}' is not compatible with writer kind "
            f"'{_kind(w)}'",
        )
    ]


def _check_records(
    r: RecordType,
    w: RecordType,
    path: tuple[str, ...],
    visited: frozenset,
    rn: dict[str, TypeNode],
    wn: dict[str, TypeNode],
) -> list[Violation]:
    violations: list[Violation] = []
    r_by_id = {f.id: f for f in r.fields}
    w_by_id = {f.id: f for f in w.fields}
    # fields present on both sides: recurse (reader name locates the path)
    for fid in sorted(set(r_by_id) & set(w_by_id)):
        rf, wf = r_by_id[fid], w_by_id[fid]
        violations += _check(rf.type, wf.type, path + (rf.name,), visited, rn, wn)
    # reader fields the writer never wrote: need a default to be readable
    for fid in sorted(set(r_by_id) - set(w_by_id)):
        rf = r_by_id[fid]
        if not rf.has_default:
            violations.append(
                Violation(
                    "required_field_missing_default",
                    path + (rf.name,),
                    f"reader field '{rf.name}' (id {rf.id}) is required but "
                    "absent from the writer contract and declares no default",
                )
            )
    # writer-only fields are simply ignored by the reader
    return violations
