"""
These examples were derived from https://suhas.org/postgres/json-table/
"""

from psycopg2 import sql
from psycopg2._psycopg import cursor

from src.jsontable import Column, ColumnList, ContextItem, JsonTable, PathExpression
from tests.fixtures import connection  # noqa: F401
from tests.fixtures import orders  # noqa: F401
from tests.fixtures import products  # noqa: F401
from tests.fixtures import transaction  # noqa: F401


def test_products(products: cursor):  # noqa: F811
    products.execute("""
        SELECT
        p.name,
        jt.size
        FROM
        products p,
        JSON_TABLE(p.attributes, '$.sizes[*]' COLUMNS (
            size TEXT PATH '$'
        )) AS jt;           
    """)
    data = products.fetchall()

    jt = JsonTable(
        context_item=ContextItem("p.attributes"),
        path_expression=PathExpression("$.sizes[*]"),
        columns=ColumnList([Column("size", "TEXT", PathExpression("$"))]),
    )

    query = sql.SQL("SELECT p.name, jt.size FROM products p, {} AS jt;").format(
        jt.as_sql()
    )

    products.execute(query)
    data2 = products.fetchall()
    assert data == data2


def test_order(orders: cursor):  # noqa: F811
    orders.execute(
        """
        SELECT
        o.id AS order_id,
        p.name AS product_name,
        jt.size AS product_size,
        o.quantity
        FROM
        orders o
        JOIN
        products p ON o.product_id = p.id,
        JSON_TABLE(p.attributes, '$.sizes[*]' COLUMNS (
            size TEXT PATH '$'
        )) AS jt;"""
    )
    data = orders.fetchall()

    jt = JsonTable(
        context_item=ContextItem("p.attributes"),
        path_expression=PathExpression("$.sizes[*]"),
        columns=ColumnList([Column("size", "TEXT", PathExpression("$"))]),
    )

    query = sql.SQL("""
        SELECT
        o.id AS order_id,
        p.name AS product_name,
        jt.size AS product_size,
        o.quantity
        FROM
        orders o
        JOIN
        products p ON o.product_id = p.id,
        {json_table} AS jt;""").format(json_table=jt.as_sql())

    orders.execute(query)

    assert data == orders.fetchall()
