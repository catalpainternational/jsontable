from src.jsontable import (
    Column,
    ColumnExists,
    ColumnList,
    ContextItem,
    JsonQuery,
    JsonTable,
    NestedPath,
    OrdinalityColumn,
    PathExpression,
)
from .fixtures import families_table_cursor, cursor, transaction, connection  # noqa: F401


def test_families(families_table_cursor: cursor):  # noqa: F811
    """
    We want to convert this document to a table with one row for each child:
    """

    #  We can use the following JSON_TABLE call to extract this information from the nested JSON arrays:
    families_table_cursor.execute("SELECT * FROM families")

    query = """
    JSON_TABLE (
        families.data, '$[*]' COLUMNS (
            id FOR ORDINALITY,
            father text PATH '$.father',
            married INTEGER EXISTS PATH '$.marriage_date',
            NESTED PATH '$.children[*]' COLUMNS (
              child_id FOR ORDINALITY,
              child text PATH '$.name',
              age INTEGER PATH '$.age'))
        ) AS jt
    """

    families_table_cursor.execute(f"SELECT jt.* FROM families, {query}")
    families_table_cursor.fetchall()

    # We're going to be referencing the "data" column in the "families table"

    context = ContextItem("families.data")
    columns = ColumnList(
        [
            OrdinalityColumn("id"),
            Column("father", "TEXT", PathExpression("$.father")),
            ColumnExists(
                "married",
                type="INTEGER",
                path_expression=PathExpression("$.marriage_date"),
            ),
            # We're going to be referencing the "children" column in the "families table"
            # and these will effectively be "left joined" to the "father" and "married" columns
            # above
            NestedPath(
                PathExpression("$.children[*]"),
                ColumnList(
                    [
                        OrdinalityColumn("child_id"),
                        Column("child", "TEXT", PathExpression("$.name")),
                        Column("age", "INTEGER", PathExpression("$.age")),
                    ]
                ),
            ),
        ],
    )

    jt = JsonTable(
        context_item=context,
        # We're starting by unpacking the array of "Families" in the table
        path_expression=PathExpression("$[*]"),
        columns=columns,
    )

    # The Query itself needs to reference the "families" table
    jq = JsonQuery(
        json_table=jt,
        table_name="families",
    )

    families_table_cursor.execute(jq.as_sql())
    data = families_table_cursor.fetchall()
    assert data == [
        (1, "John", 1, 1, "Eric", 12),
        (1, "John", 1, 2, "Beth", 10),
        (2, "Paul", 0, 1, "Sarah", 9),
        (2, "Paul", 0, 2, "Noah", 3),
        (2, "Paul", 0, 3, "Peter", 1),
    ]
