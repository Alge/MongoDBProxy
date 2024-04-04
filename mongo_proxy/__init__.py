from .mongodb_proxy import MongoProxy
from .durable_cursor import DurableCursor, MongoReconnectFailure

import pymongo

__all__ = [
    'MongoProxy',
    'DurableCursor',
    'MongoReconnectFailure',
]

if pymongo.version_tuple[0] == 3:
    from .pymongo3_durable_cursor import PyMongo3DurableCursor
    __all__.append('PyMongo3DurableCursor')
else:
    from .pymongo4_durable_cursor import PyMongo4DurableCursor
    __all__.append('PyMongo4DurableCursor')

