# Hybrid Sync/Async MongoDB Usage

This adaptor supports using both synchronous and asynchronous MongoDB operations in the same class, allowing you to:

- **Use sync operations** for metacontext, metadata loading, and initialization
- **Use async operations** for query processing and high-throughput operations

## Key Design

The `MongoUOP` class now maintains both:
- `_sync_client` (pymongo) - for blocking operations
- `_async_client` (motor) - for async operations
- `_db` - sync database reference (default)
- `_async_db` - async database reference

## Usage Example

```python
# Initialize database (sync by default, uses sync client for metacontext)
from uop.db.mongo import MongoUOP

db = MongoUOP('mydb', host='localhost', port=27017)

# Metacontext building - synchronous (fast, small data)
db.open_db()  # Uses sync client internally
db.reload_metacontext()  # Uses sync client

# Query processing - can be async (line 1150-1173 in database.py)
async def process_query():
    # This can now leverage async driver for I/O-bound queries
    results = await db.query(my_query)
    return results

# Most operations remain sync (backward compatible)
obj = db.get_object(uuid)
db.insert('classes', name='MyClass')
```

## Benefits

1. **Selective async**: Only query-heavy operations use async
2. **Metacontext stays sync**: Small, fast operations don't need async overhead
3. **Backward compatible**: Existing sync code continues to work
4. **Performance**: Query operations benefit from async I/O without infecting entire call chain

## Notes

- The `query()` method in `database.py` (line 1150+) can be called from async contexts
- Metacontext operations (`reload_metacontext()`, line 115-117) remain synchronous
- Collections created during `open_db()` use sync client by default
- For async-specific operations, you can access `db._async_db` directly

