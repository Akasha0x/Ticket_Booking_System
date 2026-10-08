"""Thread-safe data layer: events, seats, bookings, users.

Locking strategy
----------------
* One lock per event protects that event's seat map  -> bookings for
  different events never block each other (fine-grained concurrency).
* One lock for the bookings index and one for users.
* A booking touches exactly ONE event lock, then (while holding it) the
  bookings lock. The order is always event -> bookings, never reversed,
  so a lock cycle (deadlock) is impossible.
"""
import hashlib
import hmac
import json
import os
import secrets
import threading
import uuid

EVENTS = [
    {"id": "E1", "name": "Lahore Music Concert", "type": "event", "rows": 5, "cols": 8},
    {"id": "E2", "name": "Cricket: PAK vs IND", "type": "event", "rows": 8, "cols": 10},
    {"id": "T1", "name": "Daewoo Bus Lahore-Islamabad", "type": "transport", "rows": 10, "cols": 4},
    {"id": "T2", "name": "Green Line Train Karachi-Lahore", "type": "transport", "rows": 6, "cols": 8},
]


class BookingError(Exception):
    """Business-rule failure (seat taken, unknown event, ...)."""


def seat_name(r: int, c: int) -> str:
    return f"{chr(ord('A') + r)}{c + 1}"


def hash_password(password: str, salt: bytes = None):
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000)
    return salt.hex(), digest.hex()


class Store:
    def __init__(self, path: str = None):
        self._path = path
        self._events = {}
        self._event_locks = {}
        for e in EVENTS:
            self._events[e["id"]] = {
                **e,
                "seats": {seat_name(r, c): None
                          for r in range(e["rows"]) for c in range(e["cols"])},
            }
            self._event_locks[e["id"]] = threading.RLock()
        self._bookings = {}
        self._bookings_lock = threading.RLock()
        self._users = {}
        self._users_lock = threading.Lock()
        self._save_lock = threading.Lock()
        if path and os.path.exists(path):
            self._load()

    # ---------- users / auth ----------
    def register_or_login(self, username: str, password: str) -> bool:
        """Create the user on first login; verify password afterwards."""
        with self._users_lock:
            rec = self._users.get(username)
            if rec is None:
                salt, digest = hash_password(password)
                self._users[username] = {"salt": salt, "hash": digest}
                created = True
            else:
                _, digest = hash_password(password, bytes.fromhex(rec["salt"]))
                if not hmac.compare_digest(digest, rec["hash"]):
                    return False
                created = False
        if created:
            self._persist()
        return True

    # ---------- queries ----------
    def list_events(self):
        out = []
        for eid, e in self._events.items():
            with self._event_locks[eid]:
                free = sum(1 for v in e["seats"].values() if v is None)
            out.append({"id": eid, "name": e["name"], "type": e["type"],
                        "rows": e["rows"], "cols": e["cols"], "free": free})
        return out

    def seat_map(self, event_id: str):
        e = self._get_event(event_id)
        with self._event_locks[event_id]:
            return {s: (owner is not None) for s, owner in e["seats"].items()}

    def user_bookings(self, username: str):
        with self._bookings_lock:
            return [dict(b) for b in self._bookings.values() if b["user"] == username]

    # ---------- mutations ----------
    def book(self, username: str, event_id: str, seats: list) -> dict:
        e = self._get_event(event_id)
        if not seats or len(seats) > 6 or len(set(seats)) != len(seats):
            raise BookingError("choose between 1 and 6 distinct seats")
        with self._event_locks[event_id]:                 # lock 1
            for s in seats:
                if s not in e["seats"]:
                    raise BookingError(f"unknown seat {s}")
                if e["seats"][s] is not None:
                    raise BookingError(f"seat {s} is already booked")
            booking_id = uuid.uuid4().hex[:8]
            for s in seats:                               # all-or-nothing
                e["seats"][s] = booking_id
            booking = {"id": booking_id, "user": username, "event_id": event_id,
                       "event_name": e["name"], "seats": list(seats)}
            with self._bookings_lock:                     # lock 2 (fixed order)
                self._bookings[booking_id] = booking
        self._persist()
        return booking

    def cancel(self, username: str, booking_id: str) -> dict:
        with self._bookings_lock:
            b = self._bookings.get(booking_id)
        if b is None or b["user"] != username:
            raise BookingError("booking not found")
        e = self._get_event(b["event_id"])
        with self._event_locks[b["event_id"]]:
            with self._bookings_lock:
                if self._bookings.pop(booking_id, None) is None:
                    raise BookingError("booking already cancelled")
            for s in b["seats"]:
                e["seats"][s] = None
        self._persist()
        return b

    # ---------- internals ----------
    def _get_event(self, event_id):
        if event_id not in self._events:
            raise BookingError(f"unknown event {event_id}")
        return self._events[event_id]

    def _persist(self):
        if not self._path:
            return
        with self._save_lock:
            with self._users_lock:
                users = json.loads(json.dumps(self._users))
            with self._bookings_lock:
                bookings = json.loads(json.dumps(self._bookings))
            tmp = self._path + ".tmp"
            with open(tmp, "w") as f:
                json.dump({"users": users, "bookings": bookings}, f)
            os.replace(tmp, self._path)  # atomic: no half-written file

    def _load(self):
        try:
            with open(self._path) as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return
        self._users = data.get("users", {})
        self._bookings = data.get("bookings", {})
        for b in self._bookings.values():
            for s in b["seats"]:
                self._events[b["event_id"]]["seats"][s] = b["id"]