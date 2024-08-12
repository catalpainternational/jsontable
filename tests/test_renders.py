from src.jsontable import (
    ColumnList,
    JsonTable,
    NestedPath,
    Passing,
    PassingList,
    PathExpression,
    ContextItem,
    OrdinalityColumn,
    Column,
    ColumnExists,
    Rendered,
)

from tests.fixtures import cursor, products, transaction, connection  # noqa: F401


def test_OrdinalityColumn(transaction: cursor):  # noqa: F811
    assert (
        transaction.mogrify(OrdinalityColumn(name="id").as_sql())
        == b"id FOR ORDINALITY"
    )


def test_ContextItem(transaction: cursor):  # noqa: F811
    context_item = ContextItem("js")
    assert transaction.mogrify(context_item.as_sql()) == b"js"


def test_Column(transaction: cursor):  # noqa: F811
    col = Column(name="kind", type="text", path_expression=PathExpression("$.kind"))
    assert transaction.mogrify(col.as_sql()) == b"kind text PATH '$.kind'"


def test_WithWrapper(transaction: cursor):  # noqa: F811
    col = Column(
        name="title",
        type="text",
        path_expression=PathExpression("$.films[*].title"),
        format_json=True,
        with_wrapper=True,
    )
    assert (
        transaction.mogrify(col.as_sql())
        == b"title text FORMAT JSON PATH '$.films[*].title' WITH WRAPPER"
    )


def test_ColumnExists(transaction: cursor):  # noqa: F811
    col = ColumnExists(
        name="director", path_expression=PathExpression("$.films[*].director")
    )
    assert (
        transaction.mogrify(col.as_sql())
        == b"director EXISTS PATH '$.films[*].director'"
    )


def test_passing(transaction: cursor):  # noqa: F811
    filter_1 = Passing(value="Alfred Hitchcock", as_="filter")
    filter_2 = Passing(value="Vertigo", as_="filter2")

    passing_list = PassingList([filter_1, filter_2])

    assert transaction.mogrify(filter_1.as_sql()) == b"'Alfred Hitchcock' AS filter"
    assert (
        transaction.mogrify(passing_list.as_sql())
        == b"PASSING 'Alfred Hitchcock' AS filter, 'Vertigo' AS filter2"
    )


def test_collist(transaction: cursor):  # noqa: F811
    col1 = Column(name="kind", type="text", path_expression=PathExpression("$.kind"))
    col2 = Column(
        name="title",
        type="text",
        path_expression=PathExpression("$.films[*].title"),
        format_json=False,
        with_wrapper=True,
    )
    col3 = ColumnExists(
        name="director", path_expression=PathExpression("$.films[*].director")
    )
    col_list = ColumnList([col1, col2, col3])
    assert (
        transaction.mogrify(col_list.as_sql())
        == b"COLUMNS (kind text PATH '$.kind', title text PATH '$.films[*].title' WITH WRAPPER, director EXISTS PATH '$.films[*].director')"
    )


def test_nestedpath(transaction: cursor):  # noqa: F811
    col1 = Column(name="kind", type="text", path_expression=PathExpression("$.kind"))
    col2 = Column(
        name="title",
        type="text",
        path_expression=PathExpression("$.films[*].title"),
        with_wrapper=True,
    )
    col3 = ColumnExists(
        name="director", path_expression=PathExpression("$.films[*].director")
    )

    nested = NestedPath(
        path_expression=PathExpression("$.films[*]"),
        columns=ColumnList([col1, col2, col3]),
    )

    expectation = b"NESTED PATH '$.films[*]' COLUMNS (kind text PATH '$.kind', title text PATH '$.films[*].title' WITH WRAPPER, director EXISTS PATH '$.films[*].director')"
    assert transaction.mogrify(nested.as_sql()) == expectation


def test_jsontable(transaction: cursor):  # noqa: F811
    def render(thing: Rendered) -> bytes:
        return transaction.mogrify(thing.as_sql())

    qs = """
        JSON_TABLE (js, '$.favorites[*]' 
        COLUMNS (id FOR ORDINALITY, kind text PATH '$.kind',
        NESTED PATH '$.authors[*]' COLUMNS (author_id FOR ORDINALITY, author_name text PATH '$.name')))
    """

    passing = Passing("Alfred Hitchcock", "filter")
    passing_list = PassingList([passing])
    assert render(passing_list) == b"PASSING 'Alfred Hitchcock' AS filter"

    author_id = OrdinalityColumn("author_id")
    author_name = Column("author_name", "text", PathExpression("$.name"))

    assert render(author_id) == b"author_id FOR ORDINALITY"
    assert render(author_name) == b"author_name text PATH '$.name'"

    nested_path_columns = ColumnList([author_id, author_name])
    assert (
        render(nested_path_columns)
        == b"COLUMNS (author_id FOR ORDINALITY, author_name text PATH '$.name')"
    )

    nested_path = NestedPath(
        path_expression=PathExpression("$.authors[*]"), columns=nested_path_columns
    )

    assert (
        render(nested_path)
        == b"NESTED PATH '$.authors[*]' COLUMNS (author_id FOR ORDINALITY, author_name text PATH '$.name')"
    )

    columns = ColumnList(
        [
            OrdinalityColumn("id"),
            Column("kind", "text", PathExpression("$.kind")),
            nested_path,
        ]
    )

    assert (
        render(columns)
        == b"COLUMNS (id FOR ORDINALITY, kind text PATH '$.kind', NESTED PATH '$.authors[*]' COLUMNS (author_id FOR ORDINALITY, author_name text PATH '$.name'))"
    )

    context_item = ContextItem("js")

    json_table = JsonTable(
        context_item=context_item,
        path_expression=PathExpression("$.favorites[*]"),
        columns=columns,
    )
    assert transaction.mogrify(json_table.as_sql()).decode() == (""+
        "JSON_TABLE (js, '$.favorites[*]' "+
        "COLUMNS (id FOR ORDINALITY, kind text PATH '$.kind', "+
        "NESTED PATH '$.authors[*]' COLUMNS (author_id FOR ORDINALITY, author_name text PATH '$.name')))"
    )
