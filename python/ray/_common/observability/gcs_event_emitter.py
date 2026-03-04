import threading
from typing import List, Optional

from ray._raylet import (
    RayEvent,
    serialize_events_to_report_events_request,
)


class GcsEventEmitter:
    """Per-process singleton for direct GCS event emission."""

    _singleton_lock = threading.RLock()
    _instance: Optional["GcsEventEmitter"] = None

    def __new__(cls, gcs_client, node_id_hex: str, timeout_s=None):
        with cls._singleton_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(self, gcs_client, node_id_hex: str, timeout_s=None):
        node_id_binary = bytes.fromhex(node_id_hex)
        with type(self)._singleton_lock:
            if getattr(self, "_initialized", False):
                # already initialized, do nothing
                return

            # initialize the instance
            self._gcs_client = gcs_client
            self._node_id_binary = node_id_binary
            self._timeout_s = timeout_s
            self._send_lock = threading.Lock()
            self._initialized = True

    @classmethod
    def shutdown(cls):
        """Drop the singleton instance. Safe to call multiple times."""
        with cls._singleton_lock:
            cls._instance = None

    def emit(self, event: RayEvent) -> None:
        """Emit one event immediately."""
        self.emit_batch([event])

    def emit_batch(self, events: List[RayEvent]) -> None:
        """Emit a batch immediately as a single control-plane report request."""
        if not events:
            return

        with self._send_lock:
            serialized_request = serialize_events_to_report_events_request(
                events, self._node_id_binary
            )
            self._gcs_client.report_events(
                serialized_request,
                timeout_s=self._timeout_s,
            )
