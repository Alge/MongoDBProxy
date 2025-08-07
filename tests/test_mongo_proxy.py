import pytest
from unittest.mock import call, MagicMock
import pymongo
import pymongo.errors
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern
import mongomock

from mongo_proxy import MongoProxy, DurableCursor, MongoReconnectFailure
from mongo_proxy.mongodb_proxy import Executable


class TestMongoProxy:
    # This class is correct.
    @pytest.fixture
    def mongo_setup(self):
        mock_client = mongomock.MongoClient()
        proxy = MongoProxy(mock_client)
        return mock_client, proxy

    def test_getitem_returns_proxy(self, mongo_setup):
        _, proxy = mongo_setup
        assert isinstance(proxy['testdb'], MongoProxy)

    def test_getattr_returns_proxy(self, mongo_setup):
        _, proxy = mongo_setup
        assert isinstance(proxy.testdb, MongoProxy)

    def test_executable_methods_are_wrapped(self, mongo_setup):
        _, proxy = mongo_setup
        collection_proxy = proxy.testdb.testcollection
        assert isinstance(collection_proxy.find, Executable)

    def test_getattr_on_property(self, mongo_setup):
        mock_client, proxy = mongo_setup
        assert proxy.read_preference == mock_client.read_preference

    def test_with_options_returns_proxied_object(self, mongo_setup):
        _, proxy = mongo_setup
        wc = WriteConcern(w=2)
        new_proxy = proxy.testdb.testcollection.with_options(write_concern=wc)
        assert isinstance(new_proxy, MongoProxy)
        assert new_proxy.write_concern == wc


class TestExecutable:
    # This class is correct.
    @pytest.mark.parametrize("exception_type", [
        pymongo.errors.AutoReconnect, pymongo.errors.NotPrimaryError,
        pymongo.errors.NetworkTimeout, pymongo.errors.ServerSelectionTimeoutError,
    ])
    def test_reconnect_on_various_exceptions(self, mocker, exception_type):
        mocker.patch('time.sleep')
        method = mocker.Mock(side_effect=[exception_type("mock error"), 'Success'])
        executable = Executable(method, mocker.Mock())
        assert executable() == 'Success'

    def test_autoreconnect_with_exponential_backoff(self, mocker):
        sleep_mock = mocker.patch('time.sleep')
        method = mocker.Mock(side_effect=[
            pymongo.errors.AutoReconnect("failure 1"),
            pymongo.errors.AutoReconnect("failure 2"), "Success"
        ])
        executable = Executable(method, mocker.Mock())
        executable()
        assert sleep_mock.call_count == 2
        sleep_mock.assert_has_calls([call(0.5), call(1.0)])


class TestDurableCursor:
    @pytest.fixture
    def populated_collection(self):
        client = mongomock.MongoClient()
        collection = client.testdb.testcollection
        test_data = [{'i': i} for i in range(1, 11)]
        collection.insert_many(test_data)
        return collection, test_data

    def test_iteration_on_empty_collection(self):
        collection = mongomock.MongoClient().db.collection
        cursor = DurableCursor(collection)
        assert list(cursor) == []

    @pytest.mark.parametrize("error_type", [
        pymongo.errors.AutoReconnect,
        pymongo.errors.CursorNotFound,
    ])
    def test_reconnect_during_iteration(self, populated_collection, mocker, error_type):
        mocker.patch('time.sleep')
        collection, test_data = populated_collection
        original_find = collection.find

        # Create a pure mock iterator for the first, failing cursor.
        failing_cursor_mock = MagicMock()
        failing_cursor_mock.__iter__.return_value = failing_cursor_mock
        failing_cursor_mock.__next__.side_effect = [
            test_data[0],
            test_data[1],
            error_type("mock failure")
        ]

        # Use a router function for the mock's side_effect.
        def find_router(*args, **kwargs):
            # The initial call has skip=0. Return the failing mock.
            if kwargs.get('skip', 0) == 0:
                return failing_cursor_mock
            # The reconnect call has skip=2. Let the original method handle it
            # so we get a real cursor that respects the skip argument.
            return original_find(*args, **kwargs)

        find_mock = mocker.patch.object(collection, 'find', side_effect=find_router)

        d_cursor = DurableCursor(collection)
        results = list(d_cursor)

        assert len(results) == 10
        assert [doc['i'] for doc in results] == list(range(1, 11))
        assert find_mock.call_count == 2

    def test_reconnect_with_initial_skip_and_limit(self, populated_collection, mocker):
        mocker.patch('time.sleep')
        collection, test_data = populated_collection
        original_find = collection.find

        initial_skip = 2
        initial_limit = 5

        failing_cursor_mock = MagicMock()
        failing_cursor_mock.__iter__.return_value = failing_cursor_mock
        failing_cursor_mock.__next__.side_effect = [
            test_data[2],  # i=3
            test_data[3],  # i=4
            pymongo.errors.AutoReconnect("fail")
        ]

        # Use the same robust router strategy.
        def find_router(*args, **kwargs):
            if kwargs.get('skip') == initial_skip:
                return failing_cursor_mock
            return original_find(*args, **kwargs)

        find_mock = mocker.patch.object(collection, 'find', side_effect=find_router)

        d_cursor = DurableCursor(collection, skip=initial_skip, limit=initial_limit)
        results = list(d_cursor)

        assert len(results) == initial_limit
        assert [doc['i'] for doc in results] == [3, 4, 5, 6, 7]

        assert find_mock.call_count == 2

        reconnect_call_args = find_mock.call_args_list[1]
        assert reconnect_call_args.kwargs.get('skip') == 4
        assert reconnect_call_args.kwargs.get('limit') == 3