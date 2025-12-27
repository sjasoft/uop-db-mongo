import pytest
import pytest_asyncio
from uop.core.plugin_testing.harness import Plugin, AsyncPlugin
from uop.db.mongo import adaptor, async_adaptor
from uop.meta.schemas.predefined import pkm_schema
import uuid


@pytest.fixture(scope="session")
def db_harness():
    """
    Pytest fixture to set up and tear down a SQLite test database.
    """
    db_name = f"test_db_{uuid.uuid4().hex}"

    db = adaptor.MongoUOP(
        db_name, pkm_schema, username="admin", password="password"
    )
    db.open_db()
    plug_in = Plugin(db)

    yield plug_in  # Provide the database adapter instance to the tests

    # No need to drop the default database, just close the connection
    db.drop_and_close()



@pytest_asyncio.fixture(scope="function")
async def async_db_harness():
    """
    Pytest fixture to set up and tear down a PostgreSQL test database.
    """
    db_name = f"test_db_{uuid.uuid4().hex}"

    db = async_adaptor.MongoUOP(
        db_name, pkm_schema, username="admin", password="password"
    )
    await db.open_db()
    plug_in = AsyncPlugin(db)

    yield plug_in  # Provide the database adapter instance to the tests

    # No need to drop the default database, just close the connection
    await db.drop_and_close()

