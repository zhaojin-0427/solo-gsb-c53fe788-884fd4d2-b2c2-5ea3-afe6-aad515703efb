from app.contract import canonical_document, fingerprint, parse_schema


def fp(content):
    return fingerprint(canonical_document(parse_schema(content)))


def test_field_order_does_not_change_fingerprint():
    a = {
        "type": "record",
        "name": "R",
        "fields": [
            {"id": 1, "name": "a", "type": "int"},
            {"id": 2, "name": "b", "type": "string"},
        ],
    }
    b = {
        "type": "record",
        "name": "R",
        "fields": [
            {"id": 2, "name": "b", "type": "string"},
            {"id": 1, "name": "a", "type": "int"},
        ],
    }
    assert fp(a) == fp(b)


def test_enum_symbol_order_normalised():
    a = {"type": "enum", "name": "E", "symbols": ["B", "A", "C"]}
    b = {"type": "enum", "name": "E", "symbols": ["C", "B", "A"]}
    assert fp(a) == fp(b)


def test_union_branch_order_normalised():
    a = {
        "type": "union",
        "branches": [
            {"tag": "x", "type": "int"},
            {"tag": "y", "type": "string"},
        ],
    }
    b = {
        "type": "union",
        "branches": [
            {"tag": "y", "type": "string"},
            {"tag": "x", "type": "int"},
        ],
    }
    assert fp(a) == fp(b)


def test_shorthand_forms_normalise():
    assert fp("int") == fp({"type": "int"})
    a = {
        "type": "record",
        "name": "Node",
        "fields": [{"id": 1, "name": "next", "type": "Node"}],
    }
    b = {
        "type": "record",
        "name": "Node",
        "fields": [
            {"id": 1, "name": "next", "type": {"type": "ref", "name": "Node"}}
        ],
    }
    assert fp(a) == fp(b)


def test_recursive_schema_canonicalises_finitely():
    canonical = canonical_document(
        parse_schema(
            {
                "type": "record",
                "name": "Node",
                "fields": [
                    {"id": 1, "name": "value", "type": "int"},
                    {"id": 2, "name": "next", "type": "Node"},
                ],
            }
        )
    )
    assert canonical.count('"record"') == 1  # inlined once, then referenced
    assert '"ref"' in canonical


def test_different_content_different_fingerprint():
    a = {"type": "record", "name": "R", "fields": [{"id": 1, "name": "a", "type": "int"}]}
    b = {"type": "record", "name": "R", "fields": [{"id": 1, "name": "a", "type": "long"}]}
    assert fp(a) != fp(b)


def test_default_participates_in_fingerprint():
    a = {"type": "record", "name": "R", "fields": [{"id": 1, "name": "a", "type": "int"}]}
    b = {
        "type": "record",
        "name": "R",
        "fields": [{"id": 1, "name": "a", "type": "int", "default": 0}],
    }
    assert fp(a) != fp(b)
