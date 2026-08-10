"""Test doubles shared by the unit tests."""

from src.storage.catalog import UnityCatalogError


class FakeUnityCatalogClient:
    """In-memory stand-in for the Unity Catalog REST API.

    Reproduces the three behaviours the storage layer relies on: a missing table reads
    back as None, a create is rejected when the name is taken, and the registered
    `storage_location` is what every later read resolves through.
    """

    def __init__(self):
        self.tables = {}

    def get_table(self, full_name: str):
        return self.tables.get(full_name)

    def create_table(self, table: dict) -> dict:
        full_name = f"{table['catalog_name']}.{table['schema_name']}.{table['name']}"

        if full_name in self.tables:
            raise UnityCatalogError(f"Unity Catalog returned 400 creating table {table['name']}")

        self.tables[full_name] = dict(table)

        return self.tables[full_name]

    def delete_table(self, full_name: str) -> None:
        self.tables.pop(full_name, None)

    def list_tables(self, catalog: str, schema: str) -> list:
        prefix = f"{catalog}.{schema}."

        return [table for name, table in self.tables.items() if name.startswith(prefix)]
