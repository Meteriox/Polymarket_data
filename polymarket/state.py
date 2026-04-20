"""
Shared runtime state between the fetcher thread and the API server.

This module provides a simple thread-safe state holder. The service entry
point writes to it; the API routes read from it. No circular imports.
"""

import threading


class _ServiceState:
    def __init__(self):
        self._lock = threading.Lock()
        self._fetcher_thread: threading.Thread | None = None
        self._fetcher_instance = None

    @property
    def fetcher_running(self) -> bool:
        with self._lock:
            return (self._fetcher_thread is not None
                    and self._fetcher_thread.is_alive())

    def set_fetcher(self, thread: threading.Thread, instance):
        with self._lock:
            self._fetcher_thread = thread
            self._fetcher_instance = instance

    def stop_fetcher(self):
        with self._lock:
            if self._fetcher_instance is not None:
                self._fetcher_instance.should_stop = True

    def clear(self):
        with self._lock:
            self._fetcher_thread = None
            self._fetcher_instance = None


service_state = _ServiceState()
