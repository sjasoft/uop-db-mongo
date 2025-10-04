from uop.test import test_db_interface as tdi
from mongouop import adaptor
from uop import db_service

#credentials = dict(username='admin', password='password')
credentials = {}

async def test_interface():
    db_service.DatabaseClass.register_db(adaptor.MongoUOP, 'mongo')
    tdi.set_context(db_type='mongo', **credentials)
    await tdi.test_db()