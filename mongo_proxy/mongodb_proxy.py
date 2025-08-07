"""
Copyright 2013 Gustav Arngarden
Copyright 2025 Martin Alge <martin@alge.se>

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import time
import pymongo
from pymongo import MongoClient

# A curated set of method names that perform network I/O and need to be
# wrapped in the Executable class for retry-logic.
EXECUTABLE_MONGO_METHODS = {
    'aggregate', 'bulk_write', 'count', 'count_documents', 'create_index',
    'delete_many', 'delete_one', 'distinct', 'drop', 'drop_index',
    'find', 'find_one', 'find_one_and_delete', 'find_one_and_replace',
    'find_one_and_update', 'insert_many', 'insert_one', 'list_indexes',
    'map_reduce', 'replace_one', 'update_many', 'update_one',
    'command', 'create_collection', 'drop_collection', 'list_collection_names',
    'validate_collection',
    'drop_database', 'list_database_names',
}

# Add mongomock types to our check if it's installed for testing
try:
    import mongomock
    MONGOMOCK_TYPES = (mongomock.MongoClient, mongomock.database.Database, mongomock.collection.Collection)
except ImportError:
    MONGOMOCK_TYPES = ()

PYMONGO_TYPES = (pymongo.MongoClient, pymongo.database.Database, pymongo.collection.Collection)
CHAINABLE_TYPES = PYMONGO_TYPES + MONGOMOCK_TYPES


def get_client(obj):
    if isinstance(obj, pymongo.collection.Collection):
        return obj.database.client
    elif isinstance(obj, pymongo.database.Database):
        return obj.client
    elif isinstance(obj, MongoClient):
        return obj
    else:
        return None


class Executable(object):
    """ Wrap a MongoDB-method and handle AutoReconnect-exceptions. """

    def __init__(self, method, logger, wait_time=None,
                 disconnect_on_timeout=True):
        self.method = method
        self.logger = logger
        self.wait_time = wait_time or 120
        self.disconnect_on_timeout = disconnect_on_timeout

    def __call__(self, *args, **kwargs):
        start = time.time()
        round_num = 1
        i = 0
        disconnected = False
        max_time = self.wait_time
        reconnect_errors = (pymongo.errors.AutoReconnect, pymongo.errors.NetworkTimeout, pymongo.errors.ServerSelectionTimeoutError)
        while True:
            try:
                # The result is handled by MongoProxy.__call__, so we just return it
                return self.method(*args, **kwargs)
            except reconnect_errors as e:
                end = time.time()
                delta = end - start
                if delta >= max_time:
                    if not self.disconnect_on_timeout or disconnected:
                        self.logger.error("AutoReconnect timed out after %.1f seconds.", delta)
                        raise
                    client = get_client(self.method.__self__)
                    if client:
                        client.close()
                        disconnected = True
                        max_time *= 2
                        round_num = 2
                        i = 0
                        self.logger.warning('Resetting clock for round 2 after disconnecting')
                self.logger.warning('AutoReconnecting due to %s, try %d.%d (%.1f seconds elapsed)',
                                    type(e).__name__, round_num, i, delta)
                time.sleep(min(5, pow(2, i) * 0.5))
                i += 1


class MongoProxy(object):
    """ Proxy for MongoDB connection. """
    def __init__(self, conn, logger=None, wait_time=None,
                 disconnect_on_timeout=True):
        if logger is None:
            import logging
            logger = logging.getLogger(__name__)

        object.__setattr__(self, 'conn', conn)
        object.__setattr__(self, 'logger', logger)
        object.__setattr__(self, 'wait_time', wait_time)
        object.__setattr__(self, 'disconnect_on_timeout', disconnect_on_timeout)

    def __getitem__(self, key):
        item = self.conn[key]
        return MongoProxy(item, self.logger, self.wait_time, self.disconnect_on_timeout)

    def __getattr__(self, key):
        attr = getattr(self.conn, key)
        if key in EXECUTABLE_MONGO_METHODS:
            return Executable(attr, self.logger, self.wait_time, self.disconnect_on_timeout)
        return MongoProxy(attr, self.logger, self.wait_time, self.disconnect_on_timeout)

    def __call__(self, *args, **kwargs):
        """
        Executes a call on the proxied object and wraps the result if it's a new
        chainable object (like a Collection from with_options).
        """
        # Get the result from the wrapped object (e.g. the with_options method)
        result = self.conn(*args, **kwargs)

        # If the result is a new chainable object, re-wrap it in a proxy
        if isinstance(result, CHAINABLE_TYPES):
            return MongoProxy(result, self.logger, self.wait_time, self.disconnect_on_timeout)

        # Otherwise, return the raw result (e.g., a dict, a cursor, etc.)
        return result

    def __setattr__(self, key, value):
        if key in ('conn', 'logger', 'wait_time', 'disconnect_on_timeout'):
            object.__setattr__(self, key, value)
        else:
            setattr(self.conn, key, value)

    def __dir__(self):
        return dir(self.conn)

    def __str__(self):
        return self.conn.__str__()

    def __repr__(self):
        return self.conn.__repr__()

    def __eq__(self, other):
        return self.conn == other

    def __bool__(self):
        return True