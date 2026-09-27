import pytest

from app.contract import SchemaError, parse_schema
from app.contract.types import PrimType, RecordType, RefType, UnionType


def test_primitive_root():
    schema = parse_schema("int")
    assert isinstance(schema.root, PrimType)
    assert schema.root.name == "int"


def test_record_with_all_primitive_kinds():
    schema = parse_schema(
        {
            "type": "record",
            "name": "All",
            "fields": [
                {"id": 1, "name": "a", "type": "int"},
                {"id": 2, "name": "b", "type": "long"},
                {"id": 3, "name": "c", "type": "string"},
                {"id": 4, "name": "d", "type": "bytes"},
            ],
        }
    )
    assert isinstance(schema.root, RecordType)
    assert [f.type.name for f in schema.root.fields] == [
        "int",
        "long",
        "string",
        "bytes",
    ]


def test_recursive_record_resolves():
    schema = parse_schema(
        {
            "type": "record",
            "name": "Node",
            "fields": [
                {"id": 1, "name": "value", "type": "int"},
                {"id": 2, "name": "next", "type": {"type": "ref", "name": "Node"}},
            ],
        }
    )
    nxt = schema.root.fields[1]
    assert isinstance(nxt.type, RefType)
    assert schema.names["Node"] is schema.root  # cycle closes on itself


def test_string_shorthand_ref():
    schema = parse_schema(
        {
            "type": "record",
            "name": "Node",
            "fields": [
                {"id": 1, "name": "next", "type": "Node"},
            ],
        }
    )
    assert isinstance(schema.root.fields[0].type, RefType)


def test_forward_reference_to_later_definition():
    schema = parse_schema(
        {
            "type": "record",
            "name": "A",
            "fields": [
                {"id": 1, "name": "b", "type": {"type": "ref", "name": "B"}},
                {
                    "id": 2,
                    "name": "defn",
                    "type": {
                        "type": "record",
                        "name": "B",
                        "fields": [{"id": 1, "name": "x", "type": "int"}],
                    },
                },
            ],
        }
    )
    assert "B" in schema.names


def test_union_parses_tags():
    schema = parse_schema(
        {
            "type": "union",
            "branches": [
                {"tag": "card", "type": "string"},
                {"tag": "cash", "type": "long"},
            ],
        }
    )
    assert isinstance(schema.root, UnionType)
    assert [b.tag for b in schema.root.branches] == ["card", "cash"]


@pytest.mark.parametrize(
    "content,fragment",
    [
        ({"type": "record", "name": "X", "fields": "nope"}, "fields"),
        (
            {
                "type": "record",
                "name": "X",
                "fields": [
                    {"id": 1, "name": "a", "type": "int"},
                    {"id": 1, "name": "b", "type": "int"},
                ],
            },
            "duplicate field id",
        ),
        (
            {
                "type": "record",
                "name": "X",
                "fields": [{"id": -1, "name": "a", "type": "int"}],
            },
            "non-negative",
        ),
        (
            {
                "type": "record",
                "name": "X",
                "fields": [{"id": 1, "name": "a", "type": "wat"}],
            },
            "unknown type",
        ),
        ({"type": "ref", "name": "Ghost"}, "unknown named type"),
        (
            {
                "type": "record",
                "name": "Dup",
                "fields": [
                    {
                        "id": 1,
                        "name": "inner",
                        "type": {"type": "record", "name": "Dup", "fields": []},
                    }
                ],
            },
            "duplicate type name",
        ),
        (
            {"type": "enum", "name": "E", "symbols": ["A", "A"]},
            "duplicate symbols",
        ),
        ({"type": "enum", "name": "E", "symbols": []}, "non-empty"),
        (
            {
                "type": "union",
                "branches": [
                    {"tag": "t", "type": "int"},
                    {"tag": "t", "type": "long"},
                ],
            },
            "duplicate union branch tag",
        ),
        ({"type": "array"}, "items"),
        ({"type": "map"}, "values"),
        ({"type": "record", "name": "1bad", "fields": []}, "name"),
    ],
)
def test_invalid_schemas(content, fragment):
    with pytest.raises(SchemaError, match=fragment):
        parse_schema(content)


def test_default_validation():
    with pytest.raises(SchemaError, match="not a valid int"):
        parse_schema(
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {"id": 1, "name": "a", "type": "int", "default": "not-an-int"}
                ],
            }
        )


def test_bool_is_not_an_int_default():
    with pytest.raises(SchemaError):
        parse_schema(
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {"id": 1, "name": "a", "type": "int", "default": True}
                ],
            }
        )


def test_enum_default_must_be_a_symbol():
    with pytest.raises(SchemaError, match="not a symbol"):
        parse_schema(
            {
                "type": "record",
                "name": "R",
                "fields": [
                    {
                        "id": 1,
                        "name": "color",
                        "type": {"type": "enum", "name": "C", "symbols": ["RED"]},
                        "default": "BLUE",
                    }
                ],
            }
        )


def test_recursive_record_default_terminates():
    schema = parse_schema(
        {
            "type": "record",
            "name": "Node",
            "fields": [
                {"id": 1, "name": "value", "type": "int"},
                {
                    "id": 2,
                    "name": "next",
                    "type": "Node",
                    "default": {"value": 0},
                },
            ],
        }
    )
    assert schema.root.fields[1].has_default
