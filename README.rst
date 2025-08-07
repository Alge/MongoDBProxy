MongoDBProxy
============

MongoDBProxy is used to create a proxy around a MongoDB connection in order to
automatically handle AutoReconnect exceptions. You use MongoDBProxy in the
same way you would an ordinary MongoDB connection but don't need to worry about
handling AutoReconnects by yourself.

Usage::

    >>> import pymongo
    >>> from mongo_proxy import MongoProxy
    >>>
    >>> client = pymongo.MongoClient(replicaSet='blog_rs')
    >>> safe_conn = MongoProxy(client)
    >>> safe_conn.blogs.posts.insert_one({'some': 'post'})  # Automatically handles AutoReconnect.

Fork Information
----------------

This is a modernized fork of Gustav Arngarden's original MongoDBProxy. The primary goal of this version is to provide a stable, well-tested proxy compatible with modern Python 3 environments while maintaining support for legacy MongoDB databases.

Installation
------------

To install the package with its testing dependencies, run:

    pip install -e .[test]


Compatibility
--------------

This library is compatible with **Python 3.6+** and **PyMongo 3.12+** (version < 4.0).

This focus on PyMongo 3.x is a deliberate choice to ensure compatibility with older MongoDB server versions, such as MongoDB 3.4, which are not supported by PyMongo 4.x.
