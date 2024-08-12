# JSON Table

This is a SQL code generator for JSON_TABLE function introduced in Postgres 17. 

# Installing

Tests are intended to be run in dev containers, as this requires Postgres 17 beta 1 or greater.
This has only one hard requirement: some flavour of psycopg2 > 2.7. That's not specified directly in the `pyproject.toml` file
as it can be a bit hard to get the "right" version for your system. However installing `psycopg2-binary` should be sufficient
in a development environment.


To install Python requirements for development, run:
```bash
uv sync --extra dev
```


## Testing 

After installing, `pytest` should work
```
(.venv) vscode ➜ /workspaces/jsontable $ python -m pytest
============================================ test session starts ============================================
platform linux -- Python 3.12.3, pytest-8.3.2, pluggy-1.5.0
rootdir: /workspaces/jsontable
configfile: pyproject.toml
plugins: cov-5.0.0
collected 17 items                                                                                          

tests/test_families.py .                                                                              [  5%]
tests/test_film_examples.py .....                                                                     [ 35%]
tests/test_products.py ..                                                                             [ 47%]
tests/test_renders.py .........                                                                       [100%]

============================================ 17 passed in 0.33s =============================================
```

## With Cov

```
(.venv) vscode ➜ /workspaces/jsontable $ python -m pytest --cov
============================================ test session starts ============================================
platform linux -- Python 3.12.3, pytest-8.3.2, pluggy-1.5.0
rootdir: /workspaces/jsontable
configfile: pyproject.toml
plugins: cov-5.0.0
collected 17 items                                                                                          

tests/test_families.py .                                                                              [  5%]
tests/test_film_examples.py .....                                                                     [ 35%]
tests/test_products.py ..                                                                             [ 47%]
tests/test_renders.py .........                                                                       [100%]

---------- coverage: platform linux, python 3.12.3-final-0 -----------
Name                          Stmts   Miss  Cover
-------------------------------------------------
src/__init__.py                   0      0   100%
src/table.py                    132      4    97%
tests/__init__.py                 0      0   100%
tests/fixtures.py                31      0   100%
tests/test_families.py           14      0   100%
tests/test_film_examples.py      45      0   100%
tests/test_products.py           19      0   100%
tests/test_renders.py            55      0   100%
-------------------------------------------------
TOTAL                           296      4    99%
```

## Further Reading

https://github.com/obartunov/sqljsondoc/blob/master/jsonpath.md
