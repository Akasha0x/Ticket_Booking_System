"""Performance evaluation: N concurrent clients each making M booking attempts.
Usage: python load_test.py   (sweeps 1,10,50,100 clients vs 1/4/8 workers)
Reports throughput, latency percentiles and checks integrity (no double booking)."""
import random
import statistics
import threading
import time

from client_lib import ClientError, TicketClient
from server import TicketServer


def run(clients, workers, attempts=20, latency_ms=0):
    srv = TicketServer(port=0, workers=workers)
    srv.start()
    lats, ok, fail, lock = [], [0], [0], threading.Lock()
    barrier = threading.Barrier(clients)

    def user(i):
        c = TicketClient(port=srv.port, timeout=30)
        c.request("login", user=f"load{i}", password="pass1")
        barrier.wait()
        for _ in range(attempts):
            seat = f"{random.choice('ABCDEFGH')}{random.randint(1, 10)}"
            t0 = time.perf_counter()
            try:
                c.request("book", event_id="E2", seats=[seat])
                good = True
            except ClientError:
                good = False
            dt = (time.perf_counter() - t0) * 1000
            if latency_ms:
                time.sleep(latency_ms / 1000)  # simulated think/network delay
            with lock:
                lats.append(dt)
                (ok if good else fail)[0] += 1
        c.close()

    t0 = time.perf_counter()
    ts = [threading.Thread(target=user, args=(i,)) for i in range(clients)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    elapsed = time.perf_counter() - t0
    booked = sum(srv.store.seat_map("E2").values())
    consistent = booked == ok[0]  # every success == exactly one taken seat
    srv.stop()
    lats.sort()
    return dict(clients=clients, workers=workers, reqs=len(lats), rps=len(lats) / elapsed,
                avg=statistics.mean(lats), p95=lats[int(len(lats) * .95) - 1],
                p99=lats[int(len(lats) * .99) - 1], ok=ok[0], rejected=fail[0],
                consistent=consistent)


if __name__ == "__main__":
    print(f'{"clients":>7} {"workers":>7} {"req/s":>8} {"avg ms":>8} {"p95 ms":>8} '
          f'{"p99 ms":>8} {"ok":>5} {"rej":>5} integrity')
    for w in (1, 4, 8):
        for n in (1, 10, 50, 100):
            r = run(n, w)
            print(f'{r["clients"]:>7} {r["workers"]:>7} {r["rps"]:>8.0f} {r["avg"]:>8.2f} '
                  f'{r["p95"]:>8.2f} {r["p99"]:>8.2f} {r["ok"]:>5} {r["rejected"]:>5} '
                  f'{"PASS" if r["consistent"] else "FAIL"}')