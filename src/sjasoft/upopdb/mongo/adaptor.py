__author__ = 'samantha'

import random
import pymongo
from sjasoft.utils.cw_logging import getLogger
from sjasoft.utils.dicts import first_kv
from uop import database
from uop import db_collection as db_coll
import re

logging = getLogger('mongouop')


class MongoCollection(db_coll.DBCollection):
    ID_Field = '_id'

    def __init__(self, base_collection, tenant_modifier=None, indexed=False,
                 constraint=None):
        super().__init__(base_collection, indexed=indexed,
                         tenant_modifier=tenant_modifier)

    def column_class_check(self, column, uuid):
        regex = re.compile(f'_{uuid}$')
        cls_expr = {'$regex': regex}
        return {column: cls_expr}

    def update(self, criteria, mods, partial=True):
        criteria = self._with_tenant(criteria or {})
        if partial:
            mods = {'$set': mods}
        self._coll.update_many(self._with_tenant(criteria), mods)
        self._unindex(criteria)

    def db_id(self, data):
        if 'id' in data:
            data['_id'] = data.pop('id')


    def ensure_index(self, *attr_order):
        '''
        Ensures an index exist on the given ordered attributes
        :param attr_order: each pair is attribute name and bool wher index is ascending
        :return:
        '''
        info = self._coll.index_information()
        bool_to_pymongo = lambda b: pymongo.ASCENDING if b else pymongo.DESCENDING
        keys = [i['key'] for i in info.values()]
        to_check = []
        for key in keys:
            to_check.append(
                tuple([(p[0], p[1] == pymongo.ASCENDING) for p in key]))
        if attr_order not in to_check:
            spec = [(name, bool_to_pymongo(ascending))
                    for name, ascending in attr_order]
            self._coll.create_index(spec)

    def update_one(self, key, mods, partial=True):
        if partial:
            mods = {'$set': mods}
        self._coll.update_one({'_id': key}, mods)

    def insert(self, **object_data):
        self.db_id(object_data)
        self._index(object_data)
        return self._coll.insert_one(self._with_tenant(object_data))

    def bulk_load(self, ids):
        return self.un_db_id(self.find({'uuid': {'$in': ids}}))

    def distinct(self, key, criteria):
        self.db_id(criteria)
        res = self._coll.distinct(key, filter=self._with_tenant(criteria or {}))
        return self.un_db_id(res)

    def remove(self, dict_or_key):
        self._unindex(dict_or_key)
        criteria = dict_or_key
        if not isinstance(dict_or_key, dict):
            criteria = {self.ID_Field: dict_or_key}
        else:
            self.db_id(criteria)
        res = self._coll.delete_many(self._with_tenant(criteria))
        return self.un_db_id(res)

    def count(self, criteria=None):
        self.db_id(criteria)
        return self._coll.count_documents(self._with_tenant(criteria))

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
            prop, val = first_kv(criteria[key])
            return {prop: {key: val}}
        return criteria

    def find_one(self, criteria=None):
        criteria = criteria or {}
        filter = self._with_tenant(self.modified_criteria(criteria))
        res = self._coll.find_one(filter)
        return self.un_db_id(res)

    def find(self, criteria=None, only_cols=None,
                   order_by=None, limit=None, ids_only=False):
        kwargs = {}
        criteria = criteria or {}
        kwargs['filter'] = self._with_tenant(self.modified_criteria(criteria))
        if limit == 1:
            order_by = None
        if ids_only:
            only_cols = [self.ID_Field]
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
        data = list(cursor)
        if only_cols and len(only_cols) == 1:
            return [x[only_cols[0]] for x in data]
        return [self.un_db_id(d) for d in data]


class MongoUOP(database.Database):
    @classmethod
    def make_test_database(cls, **kwargs):
        return cls.make_named_database('testdb%d' % random.randint(1, 10000), **kwargs)

    @classmethod
    def make_named_database(cls, name, **kwargs):
        return cls(dbname=name, **kwargs)

    @classmethod
    def existing_db_names(cls, **kwargs):
        client, _ = cls.get_client(**kwargs)
        return client.list_database_names()

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
        client = pymongo.MongoClient(**args)
        return client, args

    @classmethod
    def drop_named_database(cls, name, **kwargs):
        client, _ = cls.get_client(**kwargs)
        client.drop_database(name)

    def __init__(self, dbname, **kwargs):
        self._client, args = self.get_client(**kwargs)
        self._host = args['host']
        self._port = args['port']
        self._db_name = dbname
        self._cached_collections = {}
        super().__init__(**kwargs)

    def drop_database(self):
        res = self._client.drop_database(self._db.name)
        return res

    def get_raw_collection(self, name, anIndex=None):
        '''indexed: if True then index at least on _id also on user_id if multiple_users
        Gets a database specific collection creating any corresponding database
        artifacts necessary to support a collection.
        :param name: name of the collection / database artifact.
        :param anIndex: not None then ensure the index specidief
        :return:
        '''
        if name in self._db.list_collection_names():
            return self._db[name]
        return self._db.create_collection(name)  # very simple in mongo

    def get_managed_collection(self, name, tenant_modifier=None):
        """
        Gets a DBCollection of the given name.  If a tenant_id is specified then the
        DBCollection will specia
        :param name: name of the collection
        :param tenant_modifier: optional fn modifying where condition for a particular tenant. .
        :return: A managed collection
        """
        raw = self.get_raw_collection(name)
        return MongoCollection(raw, tenant_modifier=tenant_modifier)

    def get_standard_collection(self, kind, tenant_modifier=None):
        coll_name = database.collection_names[kind]
        return self.get_managed_collection(coll_name, tenant_modifier)

    def _db_has_collection(self, name):
        return name in self._db.list_collection_names()

    def open_db(self, setup=None):
        self._db = self._client.get_database(self._db_name)
        super().open_db(setup=setup)

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


if __name__ == '__main__':
    mu = MongoUOP('foobar')
    mu.ensure_basic_collections()
    print(mu._collections)
