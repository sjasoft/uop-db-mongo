__author__ = 'samantha'

import random
import pymongo
from sjasoft.utils.logging import getLogger
from pymongo import asynchronous as async_pymongo

logging = getLogger('mongouop')
from uop.core import async_database, async_db_collection
from uop.core import async_db_collection as db_coll
from uop.db.mongo import adaptor as base

class MongoCollection(base.MongoCollection, async_db_collection.DBCollection):
    def __init__(self, base_collection, indexed=False, tenant_modifier=None, constraint=None):
        super().__init__(base_collection, indexed=indexed)

    async def update(self, criteria, mods, partial=True):
        criteria = self.modified_criteria(criteria or {})   
        if partial:
            mods = {'$set': mods}
        await self._coll.update_many(criteria, mods)


    async def ensure_index(self, *attr_order):
        '''
        Ensures an index exist on the given ordered attributes
        :param attr_order: each pair is attribute name and bool wher index is ascending
        :return:
        '''
        info = await self._coll.index_information()
        bool_to_pymongo = lambda b: pymongo.ASCENDING if b else pymongo.DESCENDING
        keys = [i['key'] for i in info.values()]
        to_check = []
        for key in keys:
            to_check.append(tuple([(p[0], p[1] == pymongo.ASCENDING) for p in key]))
        if attr_order not in to_check:
            spec = [(name, bool_to_pymongo(ascending)) for name, ascending in attr_order]
            await self._coll.create_index(spec)

    async def update_one(self, key, mods, partial=True):
        if partial:
            mods = {'$set': mods}
        await self._coll.update_one({self.ID_Field: key}, mods)

    async def insert(self, **object_data):
        self.db_id(object_data)
        self._index(object_data)
        return await self._coll.insert_one(object_data)

    async def bulk_load(self, ids):
        res = await self.find({self.ID_Field: {'$in': ids}})
        return self.un_db_id(res)

    async def replace_one(self, key, mods):
        return await self._coll.replace_one({self.ID_Field: key}, mods)

    async def distinct(self, key, criteria):
        criteria = self.modified_criteria(criteria or {})
        res = await self._coll.distinct(key, filter=criteria)
        return self.un_db_id(res)

    async def remove(self, dict_or_key):
        criteria = dict_or_key
        if not isinstance(dict_or_key, dict):
            criteria = {self.ID_Field: dict_or_key}
        else:
            criteria = self.modified_criteria(criteria)
        res = await self._coll.delete_many(criteria)
        return self.un_db_id(res)

    async def count(self, criteria=None):
        criteria = self.modified_criteria(criteria or {})
        return await self._coll.count_documents(criteria)


    async def find_one(self, criteria=None):
        criteria = self.modified_criteria(criteria or {})
        res = await self._coll.find_one(criteria)
        return self.un_db_id(res)

    async def find(self, criteria=None, only_cols=None,
                   order_by=None, limit=None, ids_only=False):
        kwargs = {}
        criteria = criteria or {}
        kwargs['filter'] = self.modified_criteria(criteria)
        if limit == 1:
            order_by = None
        if ids_only:
            only_cols = ['_id']
            order_by = None
        if only_cols:
            kwargs['projection'] = dict([(k, 1) for k in only_cols])
        if limit:
            kwargs['limit'] = limit
        if order_by:
            sort = []
            for fld in order_by:
                if fld.startswith('-'):
                    fld = fld[1:]
                    sort.append((fld, pymongo.DESCENDING))
                else:
                    sort.append((fld, pymongo.ASCENDING))
            kwargs['sort'] = sort
        cursor = self._coll.find(**kwargs)
        data = await cursor.to_list(None)
        if only_cols and len(only_cols) == 1:
            return [x[only_cols[0]] for x in data]
        return [self.un_db_id(d) for d in data]


class MongoUOP(async_database.Database):
    @classmethod
    def make_test_database(cls):
        return cls.make_named_database('testdb%d' % random.randint(1, 10000))

    @classmethod
    def make_named_database(cls, name):
        return cls(dbname=name)

    @classmethod
    def get_client(cls, **kwargs):
        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 27017)
        args = dict(
            host = host,
            port = port,
        )
        username = kwargs.get('username')
        password = kwargs.get('password')
        if username and password:
            args['username'] = username
            args['password'] = password
        args['authSource'] = kwargs.get('authSource', 'admin')
        client = pymongo.AsyncMongoClient(**args)
        return client, args

    def __init__(self, dbname, *schemas, tenant_id=None, **kwargs):
        kwargs.setdefault('host', 'localhost')
        kwargs.setdefault('port', 27017)
        self._db_name = dbname
        self._session = None
        self._credentials = kwargs
        self._db = None
        self._client:pymongo.AsyncMongoClient = None
        super().__init__(*schemas, tenant_id=tenant_id,  **kwargs)



    async def drop_database(self):
        await self._client.drop_database(self._db)

    async def drop_and_close(self):
        await self.drop_database()
        await self._client.close()

    async def get_raw_collection(self, name, schema=None):
    
        if name in await self._db.list_collection_names():
            return self._db[name]
        return await self._db.create_collection(name)  # very simple in mongo

    def wrap_raw_collection(self, raw):
        return MongoCollection(raw)
        
    async def _db_has_collection(self, name):
        return name in await self._db.list_collection_names()

    async def open_db(self, setup=None):
        self._client, args = self.get_client(**self._credentials)
        self._db = self._client.get_database(self._db_name)
        await super().open_db()
        
    async def start_long_transaction(self):
        """
        Mongodb doesn't have transaction support.  But we can fake it by keeping reverse information for the
        set of changes to be imposed by processing changes.  Note that this mechanism will not handle nested txn
        currently.  Many systems do nested txn by simply ignoring the nesting and only really committing at the
        top level anyway.
        """
        self._session = self._client.start_session()
        await self._session.start_transaction()
        return self._session

    async def rollback_transaction(self):
        """Rollback the current transaction"""
        if self._session:
            await self._session.abort_transaction()
            await self._session.end_session()
            self._session = None

    async def abort(self):
        await self.rollback_transaction()
        await super().abort()

    async def commit_transaction(self):
        if self._session:
            await self._session.commit_transaction()  
            await self._session.end_session()
            self._session = None

    async def really_commit(self):
        await self._session.commit_transaction()
        await self.end_long_transaction()
