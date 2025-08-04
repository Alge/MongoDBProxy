import unittest
from unittest.mock import Mock, patch, MagicMock
import pymongo

from mongo_proxy import MongoProxy, DurableCursor, MongoReconnectFailure
from mongo_proxy.mongodb_proxy import Executable


class MongoProxyTest(unittest.TestCase):

    def test_getitem(self):
        conn = Mock()
        conn.__getitem__ = Mock(return_value=Mock())
        proxy = MongoProxy(conn)
        self.assertTrue(isinstance(proxy['db'], MongoProxy))

    def test_getattr(self):
        conn = Mock()
        proxy = MongoProxy(conn)
        self.assertTrue(isinstance(proxy.db, MongoProxy))

    def test_executable(self):
        conn = Mock()
        proxy = MongoProxy(conn)
        self.assertTrue(isinstance(proxy.db.collection.find, Executable))


class ExecutableTest(unittest.TestCase):

    @patch('time.sleep')
    def test_autoreconnect_success(self, sleep_mock):
        method = Mock()
        method.side_effect = [pymongo.errors.AutoReconnect, 'Success']
        executable = Executable(method, Mock())
        self.assertEqual(executable(), 'Success')
        self.assertEqual(method.call_count, 2)

    @patch('time.sleep')
    def test_autoreconnect_failure(self, sleep_mock):
        method = Mock()
        method.side_effect = pymongo.errors.AutoReconnect
        db_mock = MagicMock(spec=pymongo.database.Database)
        db_mock.client = Mock()
        db_mock.client.close = Mock()
        method.__self__ = db_mock
        executable = Executable(method, Mock(), wait_time=0.1)
        with self.assertRaises(pymongo.errors.AutoReconnect):
            executable()

    @patch('time.sleep')
    def test_not_primary_error_handling(self, sleep_mock):
        method = Mock()
        method.side_effect = [pymongo.errors.NotPrimaryError("not primary"), 'Success']
        executable = Executable(method, Mock())
        self.assertEqual(executable(), 'Success')

    @patch('time.sleep')
    def test_network_timeout_handling(self, sleep_mock):
        method = Mock()
        method.side_effect = [pymongo.errors.NetworkTimeout("timeout"), 'Success']
        executable = Executable(method, Mock())
        self.assertEqual(executable(), 'Success')


class DurableCursorTest(unittest.TestCase):

    def test_iteration(self):
        collection = Mock()
        collection.find.return_value = iter([1, 2, 3])
        cursor = DurableCursor(collection)
        self.assertEqual(list(cursor), [1, 2, 3])

    @patch('time.sleep')
    def test_reconnect(self, sleep_mock):
        def find_side_effect(*args, **kwargs):
            yield 1
            yield 2
            raise pymongo.errors.AutoReconnect

        collection = Mock()
        collection.find.side_effect = [
            find_side_effect(),
            iter([3, 4])
        ]
        cursor = DurableCursor(collection)
        self.assertEqual(list(cursor), [1, 2, 3, 4])

    def test_count(self):
        collection = Mock()
        collection.count_documents.return_value = 5
        cursor = DurableCursor(collection, filter={'a': 1})
        self.assertEqual(cursor.count(), 5)
        collection.count_documents.assert_called_once_with({'a': 1}, **{})


if __name__ == '__main__':
    unittest.main()
