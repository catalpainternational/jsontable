import operator
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import reduce
from typing import Annotated, Generator, Literal, Union

from psycopg2 import sql


@dataclass
class Rendered(ABC):
    @abstractmethod
    def as_sql_parts(
        self,
    ) -> Generator[sql.SQL | sql.Composed, None, None]: ...  # pragma: no cover

    def as_sql(self) -> sql.SQL | sql.Composed:
        return reduce(operator.add, self.as_sql_parts())


@dataclass
class BaseColumn(Rendered):
    name: str


PathExpression = Annotated[
    str,
    "This is a JSON path expression that specifies the location of the data to be extracted from the JSON data. The path expression is a string that uses the following syntax: $.store.book[0].title. The path expression can be a simple path expression or a complex path expression. A simple path expression is a sequence of property accesses and array accesses. A complex path expression is a sequence of simple path expressions separated by the dot (.) operator. The path expression must be a constant string. The path expression must be a valid JSON path expression.",
]

WithWrapper = Annotated[
    bool,
    "If the path expression may return multiple values, it might be necessary to wrap those values using the WITH WRAPPER clause to make it a valid JSON string, because the default behavior is to not wrap them, as if WITHOUT WRAPPER were specified. The WITH WRAPPER clause is by default taken to mean WITH UNCONDITIONAL WRAPPER, which means that even a single result value will be wrapped. To apply the wrapper only when multiple values are present, specify WITH CONDITIONAL WRAPPER. Getting multiple values in result will be treated as an error if WITHOUT WRAPPER is specified.",
]

FormatJson = Annotated[
    bool,
    "Specifying FORMAT JSON makes it explicit that you expect the value to be a valid json object. It only makes sense to specify FORMAT JSON if type is one of bpchar, bytea, character varying, name, json, jsonb, text, or a domain over these types.",
]


@dataclass
class ContextItem(Rendered):
    """
    The context_item specifies the input document to query
    """

    expression: str

    def as_sql_parts(self) -> Generator[sql.SQL | sql.Composed, None, None]:
        yield sql.SQL(self.expression)


@dataclass
class OrdinalityColumn(BaseColumn):
    def as_sql_parts(self) -> Generator[sql.SQL | sql.Composed, None, None]:
        yield sql.SQL("{} FOR ORDINALITY").format(sql.SQL(self.name))


@dataclass
class Column(BaseColumn):
    type: str
    path_expression: PathExpression | None = None
    format_json: FormatJson = FormatJson(False)
    with_wrapper: WithWrapper = WithWrapper(False)
    encoding: str | None = None
    quotes: Literal["OMIT", "KEEP"] | None = None

    def as_sql_parts(self):
        yield sql.SQL(self.name)
        yield sql.SQL(" ")
        yield sql.SQL(self.type)
        if self.format_json:
            yield sql.SQL(" FORMAT JSON")
        if self.path_expression:
            yield sql.SQL(" PATH {}").format(sql.Literal(self.path_expression))
        if self.with_wrapper:
            yield sql.SQL(" WITH WRAPPER")
        if self.quotes:
            yield sql.SQL(" {} QUOTES").format(sql.SQL(self.quotes))


@dataclass
class ColumnExists(BaseColumn):
    path_expression: PathExpression
    type: str | None = None

    def as_sql_parts(self) -> Generator[sql.SQL | sql.Composed, None, None]:
        yield sql.SQL(self.name)
        if self.type:
            yield sql.SQL(" ")
            yield sql.SQL(self.type)
        yield sql.SQL(" EXISTS PATH {}").format(sql.Literal(self.path_expression))


@dataclass
class Passing(Rendered):
    value: str
    as_: str

    def as_sql_parts(self) -> Generator[sql.SQL | sql.Composed, None, None]:
        yield sql.SQL("{} AS {}").format(sql.Literal(self.value), sql.SQL(self.as_))


@dataclass
class PassingList(Rendered):
    passings: list[Passing]

    def as_sql_parts(self):
        yield sql.SQL("PASSING ")
        for n, passing in enumerate(self.passings):
            if n > 0:
                yield sql.SQL(", ")
            yield from passing.as_sql_parts()


@dataclass
class NestedPath(Rendered):
    path_expression: PathExpression
    columns: "ColumnList"

    def as_sql_parts(self):
        yield sql.SQL("NESTED PATH {} ").format(sql.Literal(self.path_expression))
        yield from self.columns.as_sql_parts()


@dataclass
class ColumnList(Rendered):
    columns: list[Union[Column | ColumnExists | OrdinalityColumn | NestedPath]]

    def as_sql_parts(self):
        yield sql.SQL("COLUMNS (")
        for n, column in enumerate(self.columns):
            if n > 0:
                yield sql.SQL(", ")
            yield from column.as_sql_parts()
        yield sql.SQL(")")


@dataclass
class JsonTable(Rendered):
    context_item: ContextItem
    path_expression: PathExpression
    passing: PassingList | None = None
    columns: ColumnList | None = None

    def as_sql_parts(self) -> Generator[sql.SQL | sql.Composed, None, None]:
        yield sql.SQL("JSON_TABLE (")
        yield from self.context_item.as_sql_parts()
        yield sql.SQL(", ")
        yield sql.SQL("{}").format(sql.Literal(self.path_expression))
        if self.passing:
            yield sql.SQL(" ")
            yield from self.passing.as_sql_parts()
        if self.columns:
            yield sql.SQL(" ")
            yield from self.columns.as_sql_parts()
        yield sql.SQL(")")


@dataclass
class JsonQuery(Rendered):
    """
    Wrap a JSON query with a Table for FROM to work
    """

    json_table: JsonTable

    # If "table_name" is None the context in JSONTable should be a JSON object
    table_name: str | None = None
    alias: str = "jt"

    def as_sql_parts(self) -> Generator[sql.SQL | sql.Composed, None, None]:
        if not self.table_name:
            yield sql.SQL("SELECT * FROM ")
            yield from self.json_table.as_sql_parts()
        else:
            yield sql.SQL("SELECT {}.* FROM {}, ").format(
                sql.Identifier(self.alias), sql.Identifier(self.table_name)
            )
            yield from self.json_table.as_sql_parts()
            yield sql.SQL(" AS {}").format(sql.Identifier(self.alias))
