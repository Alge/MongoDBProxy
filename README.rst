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


**See here for more details:**
`<http://www.arngarden.com/2013/04/29/handling-mongodb-autoreconnect-exceptions-in-python-using-a-proxy/>`_

**Contributors**:

- Gustav Arngarden (Original Author)
- Martin Alge (`<https://github.com/Alge>`_)
- Jonathan Kamens (`<https://github.com/jikamens>`_)
- Michael Cetrulo (`<https://github.com/git2samus>`_)
- Richard Frank (`<https://github.com/richafrank>`_)
- David Lindquist (`<https://github.com/dlindquist>`_)

Fork Information
----------------

This is a modernized fork of Gustav Arngarden's original MongoDBProxy. The goal of this version is to provide support for modern environments (Python 3.6+ and PyMongo 4.x) and to include a test suite to ensure reliability.

Installation
------------

pip install git+https://github.com/Alge/MongoDBProxy.git 

Compatibility
--------------

This library is compatible with Python 3.6+ and PyMongo 4.x.
