"""Socket client used by the GUI, tests and load tester."""
import itertools
import queue
import socket
import threading

from protocol import decode, encode


class ClientError(Exception):
    pass


class TicketClient:
    def __init__(self, host="127.0.0.1", port=5050, timeout=10):
        self.timeout = timeout
        try:
            self.sock = socket.create_connection((host, port), timeout=timeout)
        except OSError as exc:
            raise ClientError(f"cannot connect to server: {exc}") from exc
        self.sock.settimeout(None)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # avoid Nagle 40ms stalls
        self._ids = itertools.count(1)
        self._pending = {}
        self._lock = threading.Lock()
        self.pushes = queue.Queue()  # server-initiated messages
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self):
        try:
            with self.sock.makefile("rb") as f:
                for line in f:
                    msg = decode(line)
                    if "push" in msg:
                        self.pushes.put(msg)
                        continue
                    with self._lock:
                        q = self._pending.pop(msg.get("id"), None)
                    if q:
                        q.put(msg)
        except (OSError, ValueError):
            pass
        finally:
            with self._lock:  # wake every waiter so nothing hangs forever
                for q in self._pending.values():
                    q.put({"ok": False, "error": "connection lost"})
                self._pending.clear()

    def request(self, cmd, **fields):
        rid = next(self._ids)
        q = queue.Queue(maxsize=1)
        with self._lock:
            self._pending[rid] = q
        try:
            self.sock.sendall(encode({"id": rid, "cmd": cmd, **fields}))
            reply = q.get(timeout=self.timeout)
        except (OSError, queue.Empty) as exc:
            raise ClientError("request failed or timed out") from exc
        if not reply.get("ok"):
            raise ClientError(reply.get("error", "unknown error"))
        return reply

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass