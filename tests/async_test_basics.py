import pytest


@pytest.mark.asyncio
async def test_basics(async_db_harness):
    """
    Test the basic functionality of the SQLite database.
    """
    async_db_harness.setup_random_data()
    await async_db_harness.insert_and_check()
    await async_db_harness.modify_and_check()
    await async_db_harness.delete_and_check()

