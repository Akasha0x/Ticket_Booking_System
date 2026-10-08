"""Distributed ticket booking server.

Architecture
------------
  client threads (1 per socket) --> request Queue --> N worker threads --> Store
Each connection thread only parses messages and enqueues them; workers do
the real work. A bounded queue gives back-pressure under peak load.
Seat changes are pushed to every connected client (real-time updates).
"""
import argparse
import logging
import queue
import socket
import threading

from protocol import ProtocolError, decode, encode
from store import BookingError, Store

log = logging.getLogger("server")


class ClientSession:
    def __init__(self, conn, addr):
        self.conn, self.addr = conn, addr
        self.user = None
        self._send_lock = threading.Lock()  # sendall from many threads must not interleave
        self.alive = True

    def send(self, msg: dict):
        try:
            with self._send_lock:
                self.conn.sendall(encode(msg))
        except OSError:
            self.alive = False


class TicketServer:
    def __init__(self, host="127.0.0.1", port=5050, workers=4,
                 queue_size=1000, db_path=None):
        self.host, self.port = host, port
        self.store = Store(db_path)
        self.requests = queue.Queue(maxsize=queue_size)
        self.sessions = set()
        self.sessions_lock = threading.Lock()
        self.workers = [threading.Thread(target=self._worker, daemon=True, name=f"worker-{i}")
                        for i in range(workers)]
        self._stop = threading.Event()
        self._sock = None

    # ---------- lifecycle ----------
    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self.port = self._sock.getsockname()[1]  # supports port=0 in tests
        self._sock.listen(128)
        for w in self.workers:
            w.start()
        threading.Thread(target=self._accept_loop, daemon=True, name="acceptor").start()
        log.info("listening on %s:%s", self.host, self.port)

    def stop(self):
        self._stop.set()
        if self._sock:
            self._sock.close()
        with self.sessions_lock:
            for s in list(self.sessions):
                s.conn.close()

    # ---------- networking ----------
    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                conn, addr = self._sock.accept()
            except OSError:
                break
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            session = ClientSession(conn, addr)
            with self.sessions_lock:
                self.sessions.add(session)
            threading.Thread(target=self._client_loop, args=(session,),
                             daemon=True, name=f"client-{addr[1]}").start()

    def _client_loop(self, session):
        try:
            with session.conn.makefile("rb") as reader:
                while not self._stop.is_set():
                    line = reader.readline(65536 + 1)
                    if not line:
                        break
                    try:
                        msg = decode(line)
                    except ProtocolError as exc:
                        session.send({"ok": False, "error": str(exc)})
                        continue
                    try:
                        self.requests.put((session, msg), timeout=2)
                    except queue.Full:
                        session.send({"id": msg.get("id"), "ok": False,
                                      "error": "server busy, try again"})
        except (OSError, ValueError) as exc:
            log.debug("connection error %s: %s", session.addr, exc)
        finally:
            session.alive = False
            with self.sessions_lock:
                self.sessions.discard(session)
            try:
                session.conn.close()
            except OSError:
                pass

    # ---------- request processing ----------
    def _worker(self):
        while not self._stop.is_set():
            try:
                session, msg = self.requests.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                reply = self._handle(session, msg)
            except BookingError as exc:
                reply = {"ok": False, "error": str(exc)}
            except Exception:  # never let a bad request kill a worker
                log.exception("unexpected error handling %s", msg)
                reply = {"ok": False, "error": "internal server error"}
            reply["id"] = msg.get("id")
            session.send(reply)
            self.requests.task_done()

    def _handle(self, session, msg):
        cmd = msg.get("cmd")
        if cmd == "login":
            user, pw = msg.get("user"), msg.get("password")
            if not (isinstance(user, str) and isinstance(pw, str)
                    and 3 <= len(user) <= 20 and user.isalnum() and len(pw) >= 4):
                raise BookingError("username 3-20 alphanumeric, password min 4 chars")
            if not self.store.register_or_login(user, pw):
                raise BookingError("wrong password")
            session.user = user
            return {"ok": True, "user": user}
        if session.user is None:
            raise BookingError("please log in first")
        if cmd == "events":
            return {"ok": True, "events": self.store.list_events()}
        if cmd == "seats":
            return {"ok": True, "event_id": msg.get("event_id"),
                    "seats": self.store.seat_map(msg.get("event_id"))}
        if cmd == "book":
            seats = msg.get("seats")
            if not isinstance(seats, list) or not all(isinstance(s, str) for s in seats):
                raise BookingError("seats must be a list of seat names")
            b = self.store.book(session.user, msg.get("event_id"), seats)
            self._broadcast({"push": "seats_changed", "event_id": b["event_id"],
                             "taken": b["seats"]})
            return {"ok": True, "booking": b}
        if cmd == "cancel":
            b = self.store.cancel(session.user, msg.get("booking_id"))
            self._broadcast({"push": "seats_changed", "event_id": b["event_id"],
                             "freed": b["seats"]})
            return {"ok": True, "booking": b}
        if cmd == "my_bookings":
            return {"ok": True, "bookings": self.store.user_bookings(session.user)}
        raise BookingError(f"unknown command {cmd!r}")

    def _broadcast(self, msg):
        with self.sessions_lock:
            targets = [s for s in self.sessions if s.user]
        for s in targets:
            s.send(msg)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5050)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--db", default="tickets.json")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(threadName)s %(message)s")
    srv = TicketServer(a.host, a.port, a.workers, db_path=a.db)
    srv.start()
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        srv.stop()