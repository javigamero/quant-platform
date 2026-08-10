"""Unity Catalog storage layer.

Every table the pipeline produces is an **external Delta table registered in Unity
Catalog** as `lakehouse.<layer>.<table>`. The catalog is the source of truth for where
a table lives: writers ask it for the storage location, readers ask it too, and nothing
outside this module builds a path by hand. That is what makes the same table reachable
from a pandas job, from Spark (`spark.table("lakehouse.bronze.fact_bars_raw")`) and from
the Unity Catalog UI without three copies of the layout convention.

Delta — not bare Parquet — because the transaction log is what gives the writes below
their idempotency: a partition is replaced in a single atomic commit, so a retry after a
partial failure cannot duplicate rows, and a reader never sees a half-written partition.
That property is what the bronze logical key `(symbol, timestamp_at, feed)` depends on.

The tables are written with `deltalake` (delta-rs) rather than Spark, so extraction jobs
stay pure pandas/pyarrow and run anywhere; Spark reads the very same tables through the
`unitycatalog-spark` connector, which supports external Delta tables only.
"""

import json
import logging
import os

import pyarrow as pa
import requests
from deltalake import DeltaTable, write_deltalake

_LOGGER = logging.getLogger(__name__)

DEFAULT_CATALOG = "lakehouse"
DEFAULT_URI = "http://unitycatalog:8080"
DEFAULT_WAREHOUSE_ROOT = "/data/lake"
DEFAULT_TIMEOUT = 30

API_PREFIX = "/api/2.1/unity-catalog"
TABLE_TYPE = "EXTERNAL"
DATA_SOURCE_FORMAT = "DELTA"

# Arrow type -> (Unity Catalog type_name, Spark type_text). Decimals carry their own
# precision and scale, so they are resolved separately.
_UC_TYPES = (
    (pa.types.is_string, "STRING", "string"),
    (pa.types.is_boolean, "BOOLEAN", "boolean"),
    (pa.types.is_int64, "LONG", "long"),
    (pa.types.is_int32, "INT", "int"),
    (pa.types.is_int16, "SHORT", "short"),
    (pa.types.is_int8, "BYTE", "byte"),
    (pa.types.is_float64, "DOUBLE", "double"),
    (pa.types.is_float32, "FLOAT", "float"),
    (pa.types.is_date, "DATE", "date"),
    (pa.types.is_binary, "BINARY", "binary"),
)


class UnityCatalogError(RuntimeError):
    """Raised when the Unity Catalog API rejects a request."""


class UnityCatalogClient:
    """Thin REST client over the Unity Catalog table API.

    Only the four calls this pipeline needs are implemented. Kept separate from
    `UnityCatalog` so the registration protocol can be exercised without a live server.
    """

    def __init__(self, uri: str=None, token: str=None, session=None, timeout: int=DEFAULT_TIMEOUT):
        self.uri = (uri or os.getenv("UNITY_CATALOG_URI", DEFAULT_URI)).rstrip("/")
        self.token = token or os.getenv("UNITY_CATALOG_TOKEN")
        self.session = session or requests.Session()
        self.timeout = timeout

    def get_table(self, full_name: str) -> dict:
        """Returns the table metadata, or None when it is not registered."""

        response = self._request("GET", f"/tables/{full_name}")

        if response.status_code == 404:
            return None

        return self._payload(response, f"reading table {full_name}")

    def create_table(self, table: dict) -> dict:
        """Registers a table. The caller is expected to have checked it does not exist."""

        return self._payload(self._request("POST", "/tables", json=table), f"creating table {table['name']}")

    def delete_table(self, full_name: str) -> None:
        """Removes a table from the catalog. The data files are left on disk."""

        response = self._request("DELETE", f"/tables/{full_name}")

        if response.status_code != 404:
            self._payload(response, f"deleting table {full_name}")

    def list_tables(self, catalog: str, schema: str) -> list:
        """Lists the tables registered in `catalog.schema`."""

        response = self._request("GET", "/tables", params={"catalog_name": catalog, "schema_name": schema})

        return self._payload(response, f"listing tables in {catalog}.{schema}").get("tables", [])

    def _request(self, method: str, path: str, **kwargs):
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}

        return self.session.request(
            method, f"{self.uri}{API_PREFIX}{path}", headers=headers, timeout=self.timeout, **kwargs
        )

    @staticmethod
    def _payload(response, action: str) -> dict:
        if response.status_code >= 400:
            raise UnityCatalogError(f"Unity Catalog returned {response.status_code} {action}: {response.text}")

        return response.json()


class UnityCatalog:
    """Reads and writes the medallion tables of one Unity Catalog catalog.

    Parameters
    ----------
    * catalog: str
        Catalog name. Defaults to `UNITY_CATALOG_NAME`, then `lakehouse`.
    * warehouse_root: str
        Directory the external tables are laid out under, `<root>/<layer>/<table>`.
        Defaults to `LAKEHOUSE_ROOT`, then `/data/lake` — the volume mounted by
        `quant-infrastructure`. It must resolve to the same storage for every engine
        that reads the catalog, since the location is registered as an absolute URI.
    * client: UnityCatalogClient
    """

    def __init__(self, catalog: str=None, warehouse_root: str=None, client: UnityCatalogClient=None):
        self.catalog = catalog or os.getenv("UNITY_CATALOG_NAME", DEFAULT_CATALOG)
        self.warehouse_root = warehouse_root or os.getenv("LAKEHOUSE_ROOT", DEFAULT_WAREHOUSE_ROOT)
        self.client = client or UnityCatalogClient()

    def get_table_name(self, layer: str, table: str) -> str:
        """Builds the three-level name, e.g. `lakehouse.bronze.fact_bars_raw`."""

        return f"{self.catalog}.{layer}.{table}"

    def get_storage_location(self, layer: str, table: str) -> str:
        """Builds the `file://` URI an external table is laid out under."""

        return to_file_uri(os.path.join(self.warehouse_root, layer, table))

    def register_table(self, layer: str, table: str, schema: pa.Schema, partition_columns: list=None) -> str:
        """Registers the table if the catalog does not know it yet, and returns its location.

        Registration is idempotent and never rewrites an existing entry: the location of a
        table already holding data is the catalog's answer, not this module's convention.
        """

        full_name = self.get_table_name(layer, table)
        registered = self.client.get_table(full_name)

        if registered is not None:
            return registered["storage_location"]

        location = self.get_storage_location(layer, table)
        self.client.create_table({
            "name": table,
            "catalog_name": self.catalog,
            "schema_name": layer,
            "table_type": TABLE_TYPE,
            "data_source_format": DATA_SOURCE_FORMAT,
            "storage_location": location,
            "columns": build_uc_columns(schema, partition_columns),
            "properties": {},
        })

        _LOGGER.info("Registered %s at %s", full_name, location)

        return location

    def write_partitions(self, data: pa.Table, layer: str, table: str, partition_columns: list) -> str:
        """Writes `data`, replacing every partition it touches and leaving the rest alone.

        Partitions present in the table but absent from `data` are untouched, so a monthly
        backfill can be resumed or replayed without rewriting the whole dataset. The
        replacement is one Delta commit, which is what makes a retry safe.
        """

        if not partition_columns:
            return self.write_table(data, layer, table)

        data = to_delta_arrow(data)
        location = self.register_table(layer, table, data.schema, partition_columns)

        if data.num_rows == 0:
            _LOGGER.warning("Nothing to write to %s", self.get_table_name(layer, table))
            return location

        is_existing = DeltaTable.is_deltatable(location)

        write_deltalake(
            location,
            data,
            name=self.get_table_name(layer, table),
            partition_by=partition_columns,
            mode="overwrite",
            # On the very first write there is nothing to scope the overwrite to, and
            # delta-rs rejects a predicate against a table that does not exist yet.
            predicate=build_partition_predicate(data, partition_columns) if is_existing else None,
        )

        return location

    def write_table(self, data: pa.Table, layer: str, table: str) -> str:
        """Overwrites a small, unpartitioned table (calendar, corporate actions) whole."""

        data = to_delta_arrow(data)
        location = self.register_table(layer, table, data.schema)

        write_deltalake(location, data, name=self.get_table_name(layer, table), mode="overwrite")

        return location

    def read_table(self, layer: str, table: str, columns: list=None, filters: list=None) -> pa.Table:
        """Reads a table back, returning an empty table when it does not exist yet.

        `filters` is a delta-rs DNF filter, e.g. `[("symbol", "=", "AAPL")]`; filters on
        partition columns prune whole directories instead of reading and discarding them.
        """

        delta_table = self._load(layer, table)

        if delta_table is None:
            return pa.table({})

        return delta_table.to_pyarrow_table(columns=columns, filters=filters)

    def has_partition(self, layer: str, table: str, partition_values: dict) -> bool:
        """Checks whether the table already holds the given partition."""

        delta_table = self._load(layer, table)

        if delta_table is None:
            return False

        expected = {column: str(value) for column, value in partition_values.items()}

        return any(
            all(partition.get(column) == value for column, value in expected.items())
            for partition in delta_table.partitions()
        )

    def drop_table(self, layer: str, table: str) -> None:
        """Unregisters a table from the catalog. The Delta files are left on disk."""

        self.client.delete_table(self.get_table_name(layer, table))

    def _load(self, layer: str, table: str) -> DeltaTable:
        """Resolves a table through the catalog, returning None when there is nothing to read."""

        registered = self.client.get_table(self.get_table_name(layer, table))

        if registered is None:
            return None

        location = registered["storage_location"]

        if not DeltaTable.is_deltatable(location):
            # Registered but never written: the metadata exists, the data does not.
            return None

        return DeltaTable(location)


def get_catalog() -> UnityCatalog:
    """Returns a catalog handle configured from the environment."""

    return UnityCatalog()


def build_uc_columns(schema: pa.Schema, partition_columns: list=None) -> list:
    """Translates an Arrow schema into the Unity Catalog column specification."""

    partition_columns = list(partition_columns or [])
    columns = []

    for position, field in enumerate(schema):
        type_name, type_text = to_uc_type(field.type)
        column = {
            "name": field.name,
            "type_name": type_name,
            "type_text": type_text,
            "type_json": json.dumps({
                "name": field.name,
                "type": type_text,
                "nullable": field.nullable,
                "metadata": {},
            }),
            "type_precision": field.type.precision if pa.types.is_decimal(field.type) else 0,
            "type_scale": field.type.scale if pa.types.is_decimal(field.type) else 0,
            "position": position,
            "nullable": field.nullable,
        }

        if field.name in partition_columns:
            column["partition_index"] = partition_columns.index(field.name)

        columns.append(column)

    return columns


def to_uc_type(arrow_type) -> tuple:
    """Maps an Arrow type to its `(type_name, type_text)` Unity Catalog pair."""

    if pa.types.is_decimal(arrow_type):
        return "DECIMAL", f"decimal({arrow_type.precision},{arrow_type.scale})"

    if pa.types.is_timestamp(arrow_type):
        # Delta stores instants; a timestamp without a zone is a wall clock, not an instant.
        return ("TIMESTAMP", "timestamp") if arrow_type.tz else ("TIMESTAMP_NTZ", "timestamp_ntz")

    for is_type, type_name, type_text in _UC_TYPES:
        if is_type(arrow_type):
            return type_name, type_text

    raise TypeError(f"No Unity Catalog type for Arrow type '{arrow_type}'")


def to_delta_arrow(data: pa.Table) -> pa.Table:
    """Normalises a table to what Delta can store: every timestamp becomes UTC.

    Delta has one timestamp type — an instant, normalised to UTC. A column carrying a
    regional zone (`timestamp_et_at`) keeps its instant and loses only the display zone,
    which the contract restores on read.
    """

    fields = [
        field.with_type(pa.timestamp(field.type.unit, tz="UTC"))
        if pa.types.is_timestamp(field.type) and field.type.tz and field.type.tz != "UTC"
        else field
        for field in data.schema
    ]

    return data.cast(pa.schema(fields))


def build_partition_predicate(data: pa.Table, partition_columns: list) -> str:
    """Builds the `replaceWhere` predicate covering exactly the partitions present in `data`.

    Scoping the overwrite this way is what keeps the write idempotent per partition
    instead of per table.
    """

    partitions = (
        data.select(partition_columns).to_pandas().drop_duplicates().sort_values(partition_columns)
    )

    clauses = [
        " AND ".join(f"{column} = {to_sql_literal(row[column])}" for column in partition_columns)
        for _, row in partitions.iterrows()
    ]

    return " OR ".join(f"({clause})" for clause in clauses)


def to_sql_literal(value) -> str:
    """Renders a partition value as a SQL literal, quoting anything that is not a number."""

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, (int, float)):
        return str(value)

    return "'" + str(value).replace("'", "''") + "'"


def to_file_uri(path: str) -> str:
    """Turns a local path into the absolute `file://` URI Unity Catalog stores."""

    if "://" in path:
        return path

    return "file://" + os.path.abspath(path)
