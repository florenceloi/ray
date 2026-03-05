import threading
from typing import List, Optional

from ray._raylet import (
    RayEvent,
    serialize_events_to_report_events_request,
)

# Default timeout for GCS event reporting RPCs (seconds).
_DEFAULT_TIMEOUT_S = 10


class GcsEventEmitter:
    """Per-process singleton for direct GCS event emission."""

    _singleton_lock = threading.RLock()
    _instance: Optional["GcsEventEmitter"] = None

    def __new__(cls, *args, **kwargs):
        with cls._singleton_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def __init__(self, gcs_client, node_id_hex: str, timeout_s=_DEFAULT_TIMEOUT_S):
        with type(self)._singleton_lock:
            if getattr(self, "_initialized", False):
                return

            self._gcs_client = gcs_client
            self._node_id_binary = bytes.fromhex(node_id_hex)
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
