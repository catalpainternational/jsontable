import json

import psycopg2
import pytest
from psycopg2._psycopg import connection as connection_
from psycopg2._psycopg import cursor

psycopg2.connect


@pytest.fixture
def connection():
    """
    Creates a connection to the default postgres database.
    On teardown, the connection is closed.
    """
    conn = psycopg2.connect(
        "dbname='postgres' user='postgres' host='db' password='postgres'"
    )
    yield conn
    conn.close()


@pytest.fixture
def transaction(connection: connection_):
    """
    A fixture that provides a transactional cursor.
    """
    with connection.cursor() as c:
        c.execute("BEGIN;")
        yield c
        c.execute("ROLLBACK;")


@pytest.fixture
def products(transaction: cursor):
    transaction.execute("""
CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name TEXT,
  attributes JSONB
);

INSERT INTO products (name, attributes) VALUES
  ('T-Shirt', '{"sizes": ["S", "M", "L"], "colors": ["red", "blue"]}'),
  ('Jeans', '{"sizes": ["XS", "M", "XL"], "colors": ["black", "blue"]}'),
  ('Hat', '{"sizes": ["One Size"], "colors": ["green", "yellow", "black"]}');
""")
    yield transaction


@pytest.fixture
def orders(products: cursor):
    products.execute("""
        CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        product_id INT REFERENCES products(id),
        quantity INT
    );""")
    products.execute("""
    INSERT INTO orders (product_id, quantity) VALUES
    (1, 2),
    (2, 1),
    (3, 3);
    """)
    yield products


@pytest.fixture
def families_table_cursor(transaction: cursor):
    families_data = [
        {
            "father": "John",
            "mother": "Mary",
            "children": [{"age": 12, "name": "Eric"}, {"age": 10, "name": "Beth"}],
            "marriage_date": "2003-12-05",
        },
        {
            "father": "Paul",
            "mother": "Laura",
            "children": [
                {"age": 9, "name": "Sarah"},
                {"age": 3, "name": "Noah"},
                {"age": 1, "name": "Peter"},
            ],
        },
    ]

    transaction.execute("CREATE TABLE families (id SERIAL PRIMARY KEY, data JSONB);")
    transaction.execute(
        "INSERT INTO families (data) VALUES (%s);", (json.dumps(families_data),)
    )
    yield transaction
