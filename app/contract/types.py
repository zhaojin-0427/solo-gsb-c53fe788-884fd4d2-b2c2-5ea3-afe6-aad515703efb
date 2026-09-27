"""Type nodes for the contract schema language.

A schema document describes one root type. Named types (record / enum) are
collected into a symbol table so that local recursive references
(``{"type": "ref", "name": "..."}`` or the shorthand string form) resolve.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Union

PRIMITIVES = ("int", "long", "string", "bytes")


@dataclass
class PrimType:
    name: str  # one of PRIMITIVES


@dataclass
class RefType:
    """Reference to a named record/enum defined elsewhere in the document."""

    name: str


@dataclass
class Field:
    id: int  # stable id; alignment across versions happens by id, never by name
    name: str
    type: "TypeNode"
    has_default: bool
    default: Any = None


@dataclass
class RecordType:
    name: str
    fields: list[Field] = field(default_factory=list)


@dataclass
class EnumType:
    name: str
    symbols: list[str]


@dataclass
class Branch:
    tag: str
    type: "TypeNode"


@dataclass
class UnionType:
    branches: list[Branch]


@dataclass
class ArrayType:
    items: "TypeNode"


@dataclass
class MapType:
    values: "TypeNode"


TypeNode = Union[PrimType, RefType, RecordType, EnumType, UnionType, ArrayType, MapType]


@dataclass
class Schema:
    root: TypeNode
    names: dict[str, TypeNode]  # named records/enums, for ref resolution
