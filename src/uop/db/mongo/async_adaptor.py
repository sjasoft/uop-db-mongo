__author__ = 'samantha'

import random
import pymongo
from sjasoft.utils.logging import getLogger
import motor.motor_asyncio

logging = getLogger('mongouop')
from uop.core import async_database
from uop.core import async_db_collection as db_coll
from uop.db.mongo import adaptor as base

class MongoCollection(base.MongoCollection):
    def __init__(self, base_collection, indexed=False, tenant_modifier=None, constraint=None):
        super().__init__(base_collection, indexed=indexed)

    async def update(self, criteria, mods, partial=True):
        criteria = criteria or {}
        if partial:
            mods = {'$set': mods}
        print(criteria, mods)
        await self._coll.update_many(self._with_tenant(criteria), mods)
        self._unindex(criteria)

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
        await self._coll.update_one(self._with_tenant({'_id': key}), mods)

    async def insert(self, **object_data):
        self.db_id(object_data)
        self._index(object_data)
        return await self._coll.insert_one(self._with_tenant(object_data))

    async def bulk_load(self, ids):
        return await self.un_db_id(self.find({'uuid': {'$in': ids}}))

    async def replace_one(self, key, mods):
        return await self._coll.replace_one({'_id': key}, mods)

    async def distinct(self, key, criteria):
        self.db_id(criteria)
        res = await self._coll.distinct(key, filter=self._with_tenant(criteria or {}))
        return self.un_db_id(res)

    async def remove(self, dict_or_key):
        self._unindex(dict_or_key)
        criteria = dict_or_key
        if not isinstance(dict_or_key, dict):
            criteria = {self.ID_Field: dict_or_key}
        res = await self._coll.delete_many(self._with_tenant(criteria))
        return self.un_db_id(res)

    async def count(self, criteria=None):
        self.db_id(criteria)
        return await self._coll.count_documents(self._with_tenant(criteria))

    def modified_criteria(self, criteria):
        '''
        Works on presumption of non-commpaund criteria.  May need to get fancier later
        :param criteria:
        :return:
        '''
        criteria = super().modified_criteria(criteria)
        keys = list(criteria.keys())
        key = keys[0] if keys else None
        if key in ('$gt', '$gte', '$lt', '$lte', '$eq', '$neq'):
            prop, val = criteria[key]
            return {prop: {key: val}}
        return criteria

    async def find_one(self, criteria=None):
        criteria = criteria or {}
        filter = self._with_tenant(self.modified_criteria(criteria))
        res = await self._coll.find_one(filter)
        return self.un_db_id(res)

    async def find(self, criteria=None, only_cols=None,
                   order_by=None, limit=None, ids_only=False):
        kwargs = {}
        criteria = criteria or {}
        kwargs['filter'] = self._with_tenant(self.modified_criteria(criteria))
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

    async def bulk_load(self, ids):
        return await self.find({'_id': {'$in': ids}})


class MongoUOP(database.Database):
    @classmethod
    def make_test_database(cls):
        return cls.make_named_database('testdb%d' % random.randint(1, 10000))

    @classmethod
    def make_named_database(cls, name):
        return cls(dbname=name)

    def __init__(self, dbname, **kwargs):
        self._host = kwargs.get('host', 'localhost')
        self._port = kwargs.get('port', 27017)
        self._db_name = dbname
        self._client = motor.motor_asyncio.AsyncIOMotorClient(host=self._host, port=self._port)
        self._cached_collections = {}
        super(MongoUOP, self).__init__(**kwargs)


    async def drop_database(self):
        await self._client.drop_database(self._db)

    async def get_raw_collection(self, name, anIndex=None):
        '''indexed: if True then index at least on _id also on user_id if multiple_users
        Gets a database specific collection creating any corresponding database
        artifacts necessary to support a collection.
        :param name: name of the collection / database artifact.
        :param anIndex: not None then ensure the index specidief
        :return:
        '''
        mongo_collection = None
        return self._db[name]

    async def get_managed_collection(self, name, tenant_modifier=None):
        raw = await self.get_raw_collection(name)
        return MongoCollection(raw, tenant_modifier=tenant_modifier)

    def _db_has_collection(self, name):
        return name in self._db.collection_names()

    def open_db(self, setup=None):
        self._db = self._client.get_database(self._db_name)

    def commit(self):
        'as everything is pushed as we go there is not an extra commit operation'
        pass

    def begin_transaction(self):
        """
        Mongodb doesn't have transaction support.  But we can fake it by keeping reverse information for the
        set of changes to be imposed by processing changes.  Note that this mechanism will not handle nested txn
        currently.  Many systems do nested txn by simply ignoring the nesting and only really committing at the
        top level anyway.
        """
        pass
