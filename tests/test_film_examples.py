import pytest
from src.jsontable import (
    ColumnList,
    NestedPath,
    PathExpression,
    Column,
    JsonTable,
    ContextItem,
    JsonQuery,
    Passing,
    PassingList,
    OrdinalityColumn,
    FormatJson,
)

from tests.fixtures import transaction, connection  # noqa: F401
from psycopg2._psycopg import cursor
import json

"""
This module uses the examples found at https://www.postgresql.org/docs/17/functions-json.html#FUNCTIONS-SQLJSON-TABLE
"""

TABLE_NAME = "my_films"
data = {
    "favorites": [
        {
            "kind": "comedy",
            "films": [
                {"title": "Bananas", "director": "Woody Allen"},
                {"title": "The Dinner Game", "director": "Francis Veber"},
            ],
        },
        {
            "kind": "horror",
            "films": [{"title": "Psycho", "director": "Alfred Hitchcock"}],
        },
        {
            "kind": "thriller",
            "films": [{"title": "Vertigo", "director": "Alfred Hitchcock"}],
        },
        {
            "kind": "drama",
            "films": [{"title": "Yojimbo", "director": "Akira Kurosawa"}],
        },
    ]
}


@pytest.fixture
def my_films(transaction: cursor):  # noqa: F811
    """
    Create table. This is dropped + recreated automatically at the end
    of each test.
    """
    transaction.execute(
        f"""
    CREATE TABLE {TABLE_NAME} ( js jsonb );
    INSERT INTO {TABLE_NAME} VALUES (
    %s);
    """,
        (json.dumps(data),),
    )
    yield transaction


def test_my_films(my_films: cursor):  # noqa: F811
    """
    The following query shows how to use JSON_TABLE to turn the
    JSON objects in the my_films table to a view containing
    columns for the keys kind, title, and director contained
    in the original JSON along with an ordinality column:

    SELECT jt.* FROM
    my_films,
    JSON_TABLE (js, '$.favorites[*]' COLUMNS (
      id FOR ORDINALITY,
      kind text PATH '$.kind',
      title text PATH '$.films[*].title' WITH WRAPPER,
      director text PATH '$.films[*].director' WITH WRAPPER)) AS jt
    """

    columns = ColumnList(
        [
            OrdinalityColumn("id"),
            Column("kind", "text", PathExpression("$.kind")),
            Column(
                "title", "text", PathExpression("$.films[*].title"), with_wrapper=True
            ),
            Column(
                "director",
                "text",
                PathExpression("$.films[*].director"),
                with_wrapper=True,
            ),
        ]
    )

    jt = JsonTable(
        context_item=ContextItem("js"),
        path_expression=PathExpression("$.favorites[*]"),
        columns=columns,
    )

    jq = JsonQuery(jt, table_name=TABLE_NAME)
    my_films.execute(jq.as_sql())
    data = my_films.fetchall()
    assert isinstance(data, list)


def test_table_with_passing(my_films: cursor):  # noqa: F811
    """
    The following is a modified version of the above query to show the usage
    of PASSING arguments in the filter specified in the top-level JSON path
    expression and the various options for the individual columns:

        SELECT jt.* FROM
        my_films,
        JSON_TABLE (js, '$.favorites[*] ? (@.films[*].director == $filter)'
        PASSING 'Alfred Hitchcock' AS filter, 'Vertigo' AS filter2
            COLUMNS (
            id FOR ORDINALITY,
            kind text PATH '$.kind',
            title text FORMAT JSON PATH '$.films[*].title' OMIT QUOTES,
            director text PATH '$.films[*].director' KEEP QUOTES)) AS jt
        """
    jsonquery = JsonQuery(
        JsonTable(
            context_item=ContextItem("js"),
            path_expression=PathExpression(
                "$.favorites[*] ? (@.films[*].director == $filter)"
            ),
            passing=PassingList(
                [Passing("Alfred Hitchcock", "filter"), Passing("Vertigo", "filter2")]
            ),
            columns=ColumnList(
                [
                    OrdinalityColumn("id"),
                    Column("kind", "text", PathExpression("$.kind")),
                    Column(
                        "title",
                        "text",
                        PathExpression("$.films[*].title"),
                        format_json=FormatJson(True),
                        quotes="OMIT",
                    ),
                    Column(
                        "director",
                        "text",
                        PathExpression("$.films[*].director"),
                        quotes="KEEP",
                    ),
                ]
            ),
        ),
        table_name=TABLE_NAME,
    )

    my_films.execute(jsonquery.as_sql())
    data = my_films.fetchall()
    assert isinstance(data, list)
    assert data == [
        (1, "horror", "Psycho", '"Alfred Hitchcock"'),
        (2, "thriller", "Vertigo", '"Alfred Hitchcock"'),
    ]


def test_table_with_nested_path(my_films: cursor):
    """
    The following is a modified version of the above query to show the usage of NESTED PATH
    for populating title and director columns, illustrating how they are joined to the parent columns id and kind:

    SELECT jt.* FROM
    my_films,
    JSON_TABLE ( js, '$.favorites[*] ? (@.films[*].director == $filter)'
    PASSING 'Alfred Hitchcock' AS filter
    COLUMNS (
        id FOR ORDINALITY,
        kind text PATH '$.kind',
        NESTED PATH '$.films[*]' COLUMNS (
        title text FORMAT JSON PATH '$.title' OMIT QUOTES,
        director text PATH '$.director' KEEP QUOTES))) AS jt
    """

    jsonquery = JsonQuery(
        JsonTable(
            context_item=ContextItem("js"),
            path_expression=PathExpression(
                "$.favorites[*] ? (@.films[*].director == $filter)"
            ),
            passing=PassingList([Passing("Alfred Hitchcock", "filter")]),
            columns=ColumnList(
                [
                    OrdinalityColumn("id"),
                    Column("kind", "text", PathExpression("$.kind")),
                    NestedPath(
                        PathExpression("$.films[*]"),
                        ColumnList(
                            [
                                Column(
                                    "title",
                                    "text",
                                    PathExpression("$.title"),
                                    format_json=FormatJson(True),
                                    quotes="OMIT",
                                ),
                                Column(
                                    "director",
                                    "text",
                                    PathExpression("$.director"),
                                    quotes="KEEP",
                                ),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        table_name=TABLE_NAME,
    )

    my_films.execute(jsonquery.as_sql())
    data = my_films.fetchall()
    assert data == [
        (1, "horror", "Psycho", '"Alfred Hitchcock"'),
        (2, "thriller", "Vertigo", '"Alfred Hitchcock"'),
    ]


def test_table_without_root_path_filter(my_films: cursor):
    """

    The following is the same query but without the filter in the root path:

    SELECT jt.* FROM
    my_films,
    JSON_TABLE ( js, '$.favorites[*]'
    COLUMNS (
        id FOR ORDINALITY,
        kind text PATH '$.kind',
        NESTED PATH '$.films[*]' COLUMNS (
        title text FORMAT JSON PATH '$.title' OMIT QUOTES,
        director text PATH '$.director' KEEP QUOTES))) AS jt"""

    jsonquery = JsonQuery(
        JsonTable(
            context_item=ContextItem("js"),
            path_expression=PathExpression("$.favorites[*]"),
            columns=ColumnList(
                [
                    OrdinalityColumn("id"),
                    Column("kind", "text", PathExpression("$.kind")),
                    NestedPath(
                        PathExpression("$.films[*]"),
                        ColumnList(
                            [
                                Column(
                                    "title",
                                    "text",
                                    PathExpression("$.title"),
                                    format_json=FormatJson(True),
                                    quotes="OMIT",
                                ),
                                Column(
                                    "director",
                                    "text",
                                    PathExpression("$.director"),
                                    quotes="KEEP",
                                ),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        table_name=TABLE_NAME,
    )

    my_films.execute(jsonquery.as_sql())
    data = my_films.fetchall()

    # id |   kind   |      title      |      director
    # ----+----------+-----------------+--------------------
    #  1 | comedy   | Bananas         | "Woody Allen"
    #  1 | comedy   | The Dinner Game | "Francis Veber"
    #  2 | horror   | Psycho          | "Alfred Hitchcock"
    #  3 | thriller | Vertigo         | "Alfred Hitchcock"
    #  4 | drama    | Yojimbo         | "Akira Kurosawa"
    assert data == [
        (1, "comedy", "Bananas", '"Woody Allen"'),
        (1, "comedy", "The Dinner Game", '"Francis Veber"'),
        (2, "horror", "Psycho", '"Alfred Hitchcock"'),
        (3, "thriller", "Vertigo", '"Alfred Hitchcock"'),
        (4, "drama", "Yojimbo", '"Akira Kurosawa"'),
    ]


def test_json_object_as_input(transaction: cursor):  # noqa: F811
    """
    The following shows another query using a different JSON object as input.
    It shows the UNION "sibling join" between NESTED paths $.movies[*] and $.books[*]
    and also the usage of FOR ORDINALITY column at NESTED levels (columns movie_id, book_id, and author_id):
    """

    context_item = ContextItem("""'{"favorites":
            {"movies":
            [{"name": "One", "director": "John Doe"},
            {"name": "Two", "director": "Don Joe"}],
            "books":
            [{"name": "Mystery", "authors": [{"name": "Brown Dan"}]},
            {"name": "Wonder", "authors": [{"name": "Jun Murakami"}, {"name":"Craig Doe"}]}]
        }}'::json""")
    path_expression = PathExpression("$.favorites[*]")
    columns = ColumnList(
        [
            OrdinalityColumn("user_id"),
            NestedPath(
                path_expression=PathExpression("$.movies[*]"),
                columns=ColumnList(
                    [
                        OrdinalityColumn("movie_id"),
                        Column(
                            "mname",
                            "text",
                            PathExpression("$.name"),
                        ),
                        Column("director", "text"),
                    ]
                ),
            ),
            NestedPath(
                path_expression=PathExpression("$.books[*]"),
                columns=ColumnList(
                    [
                        OrdinalityColumn("book_id"),
                        Column(
                            "bname",
                            "text",
                            PathExpression("$.name"),
                        ),
                        NestedPath(
                            path_expression=PathExpression("$.authors[*]"),
                            columns=ColumnList(
                                [
                                    OrdinalityColumn("author_id"),
                                    Column(
                                        "author_name",
                                        "text",
                                        PathExpression("$.name"),
                                    ),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
        ]
    )

    table = JsonTable(
        context_item=context_item,
        path_expression=path_expression,
        columns=columns,
    )

    query = JsonQuery(table)

    transaction.execute(query.as_sql())
    data = transaction.fetchall()
    assert isinstance(data, list)

    # user_id | movie_id | mname | director | book_id |  bname  | author_id | author_name
    # ---------+----------+-------+----------+---------+---------+-----------+--------------
    #   1 |        1 | One   | John Doe |         |         |           |
    #   1 |        2 | Two   | Don Joe  |         |         |           |
    #   1 |          |       |          |       1 | Mystery |         1 | Brown Dan
    #   1 |          |       |          |       2 | Wonder  |         1 | Jun Murakami
    #   1 |          |       |          |       2 | Wonder  |         2 | Craig Doe

    assert data == [
        (1, 1, "One", "John Doe", None, None, None, None),
        (1, 2, "Two", "Don Joe", None, None, None, None),
        (1, None, None, None, 1, "Mystery", 1, "Brown Dan"),
        (1, None, None, None, 2, "Wonder", 1, "Jun Murakami"),
        (1, None, None, None, 2, "Wonder", 2, "Craig Doe"),
    ]
