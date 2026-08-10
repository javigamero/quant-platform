import pytest

from src.storage.catalog import UnityCatalog
from tests.unit.fakes import FakeUnityCatalogClient


@pytest.fixture
def lakehouse(tmp_path) -> UnityCatalog:
    """A catalog writing real Delta tables under `tmp_path`, registered in a fake server.

    The Delta round trip is real — only the catalog server is faked — so the tests still
    exercise the partitioning, the types and the idempotency of every write.
    """

    return UnityCatalog(
        catalog="lakehouse", warehouse_root=str(tmp_path), client=FakeUnityCatalogClient()
    )
