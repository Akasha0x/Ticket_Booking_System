import os
import socket
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from client_lib import ClientError, TicketClient
from server import TicketServer
from store import BookingError, Store


class StoreTests(unittest.TestCase):
    def test_double_booking_rejected(self):
        s = Store()
        s.book("alice", "E1", ["A1"])
        with self.assertRaises(BookingError):
            s.book("bob", "E1", ["A1"])

    def test_all_or_nothing(self):
        s = Store()
        s.book("alice", "E1", ["A2"])
        with self.assertRaises(BookingError):
            s.book("bob", "E1", ["A1", "A2"])
        self.assertFalse(s.seat_map("E1")["A1"])  # A1 must remain free

    def test_validation(self):
        s = Store()
        for bad in ([], ["ZZ99"], ["A1", "A1"], [f"A{i}" for i in range(1, 8)]):
            with self.assertRaises(BookingError):
                s.book("u", "E1", bad)
        with self.assertRaises(BookingError):
            s.book("u", "NOPE", ["A1"])

    def test_cancel_frees_and_checks_owner(self):
        s = Store()
        b = s.book("alice", "E1", ["B1"])
        with self.assertRaises(BookingError):
            s.cancel("mallory", b["id"])
        s.cancel("alice", b["id"])
        s.book("bob", "E1", ["B1"])

    def test_concurrent_same_seat_one_winner(self):
        s, wins, lock = Store(), [], threading.Lock()

        def go(i):
            try:
                s.book(f"u{i}", "E2", ["C3"])
                with lock:
                    wins.append(i)
            except BookingError:
                pass
        ts = [threading.Thread(target=go, args=(i,)) for i in range(200)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(len(wins), 1)

    def test_no_deadlock_mixed_book_cancel(self):
        s, done = Store(), []

        def go(i):
            for k in range(30):
                try:
                    b = s.book(f"u{i}", "E1", [f"A{(i + k) % 8 + 1}"])
                    s.cancel(f"u{i}", b["id"])
                except BookingError:
                    pass
            done.append(i)
        ts = [threading.Thread(target=go, args=(i,)) for i in range(20)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=20)
        self.assertEqual(len(done), 20)

    def test_persistence_roundtrip(self):
        path = os.path.join(tempfile.mkdtemp(), "db.json")
        s = Store(path)
        s.register_or_login("alice", "secret")
        s.book("alice", "T1", ["A1"])
        s2 = Store(path)
        self.assertTrue(s2.seat_map("T1")["A1"])
        self.assertTrue(s2.register_or_login("alice", "secret"))
        self.assertFalse(s2.register_or_login("alice", "wrong"))


class NetworkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = TicketServer(port=0, workers=4)
        cls.srv.start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.stop()

    def client(self, user):
        c = TicketClient(port=self.srv.port)
        c.request("login", user=user, password="pass1")
        self.addCleanup(c.close)
        return c

    def test_login_required_and_validation(self):
        c = TicketClient(port=self.srv.port)
        self.addCleanup(c.close)
        with self.assertRaises(ClientError):
            c.request("events")
        with self.assertRaises(ClientError):
            c.request("login", user="a b!", password="x")

    def test_wrong_password(self):
        self.client("carol")
        c = TicketClient(port=self.srv.port)
        self.addCleanup(c.close)
        with self.assertRaises(ClientError):
            c.request("login", user="carol", password="bad!!")

    def test_end_to_end_and_push(self):
        a, b = self.client("anna"), self.client("ben")
        bk = a.request("book", event_id="T2", seats=["A1", "A2"])["booking"]
        push = b.pushes.get(timeout=3)
        self.assertEqual(push["event_id"], "T2")
        self.assertTrue(b.request("seats", event_id="T2")["seats"]["A1"])
        a.request("cancel", booking_id=bk["id"])

    def test_concurrent_clients_single_winner(self):
        n, results, lock = 40, [], threading.Lock()
        clients = [self.client(f"racer{i}") for i in range(n)]
        barrier = threading.Barrier(n)

        def go(c):
            barrier.wait()
            try:
                c.request("book", event_id="E1", seats=["E8"])
                r = True
            except ClientError:
                r = False
            with lock:
                results.append(r)
        ts = [threading.Thread(target=go, args=(c,)) for c in clients]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(sum(results), 1)

    def test_malformed_input_does_not_crash_server(self):
        s = socket.create_connection(("127.0.0.1", self.srv.port))
        s.sendall(b"not json\n[1,2]\n\xff\xfe\n")
        s.close()
        self.client("dave").request("events")  # server still healthy


if __name__ == "__main__":
    unittest.main(verbosity=2)