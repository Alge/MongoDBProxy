"""
Copyright 2015 Quantopian Inc.
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

import logging
import time
from pymongo.cursor import CursorType
from pymongo.errors import (
    AutoReconnect,
    CursorNotFound,
    ExecutionTimeout,
    OperationFailure,
    WTimeoutError,
    NotPrimaryError,
    NetworkTimeout,
    ServerSelectionTimeoutError
)

# How long we are willing to attempt to reconnect when the replicaset
# fails over.  We double the delay between each attempt.
MAX_RECONNECT_TIME = 60
MAX_SLEEP = 5
RECONNECT_INITIAL_DELAY = 1
RETRYABLE_OPERATION_FAILURE_CLASSES = (
    AutoReconnect,
    CursorNotFound,
    ExecutionTimeout,
    WTimeoutError,
    NetworkTimeout,
    ServerSelectionTimeoutError,
)
ALL_RETRYABLE_EXCEPTIONS = RETRYABLE_OPERATION_FAILURE_CLASSES + (OperationFailure,)
log = logging.getLogger(__name__)


class MongoReconnectFailure(Exception):
    pass


class DurableCursor(object):
    logger = log

    def __init__(
            self,
            collection,
            filter=None,
            projection=None,
            sort=None,
            hint=None,
            tailable=False,
            max_reconnect_time=60,
            initial_reconnect_interval=1,
            skip=0,
            limit=0,
            disconnect_on_timeout=True,
            **kwargs):

        self.collection = collection
        self.filter = filter or {}
        self.projection = projection
        self.sort = sort
        self.hint = hint
        self.tailable = tailable
        self.max_reconnect_time = max_reconnect_time
        self.initial_reconnect_interval = initial_reconnect_interval
        self.counter = self.skip = skip
        self.limit = limit
        self.disconnect_on_timeout = disconnect_on_timeout
        self.kwargs = kwargs

        self.cursor = self.fetch_cursor(self.counter, self.kwargs)

    def __iter__(self):
        return self

    def fetch_cursor(self, count, cursor_kwargs):
        """
        Gets a cursor for the options set in the object, using the
        correct API for PyMongo 3.x.
        """
        log.debug("DurableCursor: Entering fetch_cursor with count=%d, limit=%d, initial_skip=%d",
                  count, self.limit, self.skip)

        limit_is_zero = False
        if self.limit:
            limit = self.limit - (count - self.skip)
            log.debug("DurableCursor: fetch_cursor calculated new limit=%d", limit)
            if limit <= 0:
                limit = 1
                limit_is_zero = True
        else:
            limit = 0

        # For PyMongo 3.x, 'tailable' is controlled via cursor_type
        cursor_type = CursorType.TAILABLE_AWAIT if self.tailable else CursorType.NON_TAILABLE

        cursor = self.collection.find(
            filter=self.filter,
            projection=self.projection,
            sort=self.sort,
            skip=count,
            limit=limit,
            cursor_type=cursor_type,
            **cursor_kwargs
        )

        # 'hint' is a separate method call on the cursor in PyMongo 3.x
        if self.hint:
            cursor.hint(self.hint)

        if limit_is_zero:
            next(cursor, None)

        log.debug("DurableCursor: fetch_cursor returning new cursor.")
        return cursor

    def reload_cursor(self):
        log.debug("DurableCursor: reload_cursor called. Current counter is %d.", self.counter)
        self.cursor = self.fetch_cursor(self.counter, self.kwargs)

    @property
    def alive(self):
        return self.tailable and self.cursor.alive

    def __next__(self):
        log.debug("DurableCursor: __next__ called. About to call _with_retry.")
        next_record = self._with_retry(get_next=True, f=lambda: next(self.cursor))
        self.counter += 1
        log.debug("DurableCursor: __next__ success. Counter is now %d.", self.counter)
        return next_record

    next = __next__

    def _with_retry(self, get_next, f, *args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ALL_RETRYABLE_EXCEPTIONS as exc:
            log.warning("DurableCursor: _with_retry caught exception: %r", exc)

            if isinstance(exc, OperationFailure):
                is_retryable_op_failure = (
                    'interrupted at shutdown' in str(exc.args[0]) or
                    exc.__class__ in RETRYABLE_OPERATION_FAILURE_CLASSES
                )
                if not is_retryable_op_failure:
                    log.error("DurableCursor: Unhandleable OperationFailure. Re-raising.")
                    raise

            log.debug("DurableCursor: Exception is retryable. Calling try_reconnect.")
            return self.try_reconnect(get_next=get_next)

    def try_reconnect(self, get_next=True):
        log.debug("DurableCursor: Entered try_reconnect.")
        start = time.time()
        interval = self.initial_reconnect_interval

        while True:
            try:
                self.reload_cursor()
                log.debug("DurableCursor: try_reconnect successfully reloaded cursor. Calling next().")
                return next(self.cursor) if get_next else True
            except RETRYABLE_OPERATION_FAILURE_CLASSES as e:
                log.warning("DurableCursor: try_reconnect caught %r during inner loop.", e)
                if time.time() - start > self.max_reconnect_time:
                    log.error('DurableCursor: Reconnect timed out.')
                    raise MongoReconnectFailure()

                log.debug("DurableCursor: Reconnecting... sleeping for %.1f seconds.", interval)
                time.sleep(interval)
                interval = min(interval * 2, MAX_SLEEP)

    def count(self, with_limit_and_skip=False):
        cursor = self.collection.find(self.filter)
        if with_limit_and_skip:
            if self.skip:
                cursor = cursor.skip(self.skip)
            if self.limit:
                cursor = cursor.limit(self.limit)
        return cursor.count(with_limit_and_skip=with_limit_and_skip)