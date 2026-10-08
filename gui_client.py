"""Tkinter GUI: login -> choose event -> pick seats -> book / cancel.

All network calls run in worker threads; results return to the Tk main
thread via a queue polled with after() (Tk is not thread-safe).
"""
import queue
import threading
import tkinter as tk
from tkinter import messagebox, ttk

from client_lib import ClientError, TicketClient


class App(tk.Tk):
    def __init__(self, host, port):
        super().__init__()
        self.title("Distributed Ticket Booking")
        self.geometry("760x560")
        self.host, self.port = host, port
        self.client = None
        self.events, self.selected, self.seat_buttons = {}, set(), {}
        self.bookings = []
        self.current_event = None
        self.ui_queue = queue.Queue()
        self._build_login()
        self.after(100, self._drain_ui_queue)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ---------- threading helpers ----------
    def run_async(self, func, on_done):
        def task():
            try:
                self.ui_queue.put((on_done, func(), None))
            except ClientError as exc:
                self.ui_queue.put((on_done, None, exc))
        threading.Thread(target=task, daemon=True).start()

    def _drain_ui_queue(self):
        try:
            while True:
                cb, result, err = self.ui_queue.get_nowait()
                cb(result, err)
        except queue.Empty:
            pass
        if self.client:
            try:
                while True:
                    push = self.client.pushes.get_nowait()
                    if push.get("event_id") == self.current_event:
                        self._load_seats(self.current_event)
                    self._refresh_events()
            except queue.Empty:
                pass
        self.after(100, self._drain_ui_queue)

    # ---------- login ----------
    def _build_login(self):
        self.login = ttk.Frame(self, padding=30)
        self.login.pack(expand=True)
        ttk.Label(self.login, text="Ticket Booking Login", font=("Arial", 16)).grid(columnspan=2, pady=10)
        self.user_var, self.pw_var = tk.StringVar(), tk.StringVar()
        ttk.Label(self.login, text="Username").grid(row=1, column=0, sticky="e")
        ttk.Entry(self.login, textvariable=self.user_var).grid(row=1, column=1, pady=3)
        ttk.Label(self.login, text="Password").grid(row=2, column=0, sticky="e")
        ttk.Entry(self.login, textvariable=self.pw_var, show="*").grid(row=2, column=1, pady=3)
        ttk.Button(self.login, text="Login / Register", command=self._do_login).grid(columnspan=2, pady=10)
        ttk.Label(self.login, text="New usernames are registered automatically.").grid(columnspan=2)

    def _do_login(self):
        def work():
            self.client = TicketClient(self.host, self.port)
            return self.client.request("login", user=self.user_var.get(), password=self.pw_var.get())

        def done(_, err):
            if err:
                messagebox.showerror("Login failed", str(err))
            else:
                self.login.destroy()
                self._build_main()
        self.run_async(work, done)

    # ---------- main screen ----------
    def _build_main(self):
        left = ttk.Frame(self, padding=8)
        left.pack(side="left", fill="y")
        ttk.Label(left, text="Events & Transport").pack(anchor="w")
        self.event_list = tk.Listbox(left, width=38, height=10, exportselection=False)
        self.event_list.pack()
        self.event_list.bind("<<ListboxSelect>>", self._on_event_select)
        ttk.Label(left, text="My bookings").pack(anchor="w", pady=(10, 0))
        self.booking_list = tk.Listbox(left, width=38, height=8, exportselection=False)
        self.booking_list.pack()
        ttk.Button(left, text="Cancel selected booking", command=self._cancel).pack(pady=5)

        right = ttk.Frame(self, padding=8)
        right.pack(side="right", expand=True, fill="both")
        self.title_lbl = ttk.Label(right, text="Select an event", font=("Arial", 13))
        self.title_lbl.pack()
        self.grid_frame = ttk.Frame(right)
        self.grid_frame.pack(pady=10)
        self.status = tk.StringVar(value="Connected")
        ttk.Button(right, text="Book selected seats", command=self._book).pack()
        ttk.Label(right, text="grey = free, green = selected by you, red = taken").pack(pady=4)
        ttk.Label(right, textvariable=self.status).pack(side="bottom")
        self._refresh_events()
        self._refresh_bookings()

    def _refresh_events(self):
        def done(res, err):
            if err:
                return self.status.set(str(err))
            self.events = {e["id"]: e for e in res["events"]}
            sel = self.event_list.curselection()
            self.event_list.delete(0, "end")
            for e in res["events"]:
                self.event_list.insert("end", f'{e["id"]}  {e["name"]}  ({e["free"]} free)')
            if sel:
                self.event_list.selection_set(sel[0])
        self.run_async(lambda: self.client.request("events"), done)

    def _refresh_bookings(self):
        def done(res, err):
            if err:
                return
            self.bookings = res["bookings"]
            self.booking_list.delete(0, "end")
            for b in self.bookings:
                self.booking_list.insert("end", f'{b["id"]} {b["event_name"]} {",".join(b["seats"])}')
        self.run_async(lambda: self.client.request("my_bookings"), done)

    def _on_event_select(self, _):
        sel = self.event_list.curselection()
        if sel:
            eid = list(self.events)[sel[0]]
            self.current_event, self.selected = eid, set()
            self._load_seats(eid)

    def _load_seats(self, eid):
        def done(res, err):
            if err:
                return self.status.set(str(err))
            self._draw_seats(eid, res["seats"])
        self.run_async(lambda: self.client.request("seats", event_id=eid), done)

    def _draw_seats(self, eid, seats):
        for w in self.grid_frame.winfo_children():
            w.destroy()
        e = self.events[eid]
        self.title_lbl.config(text=e["name"])
        self.seat_buttons = {}
        self.selected &= {s for s, taken in seats.items() if not taken}
        for name, taken in seats.items():
            r, c = ord(name[0]) - 65, int(name[1:]) - 1
            btn = tk.Button(self.grid_frame, text=name, width=4,
                            bg="#d9534f" if taken else ("#5cb85c" if name in self.selected else "#e0e0e0"),
                            state="disabled" if taken else "normal",
                            command=lambda n=name: self._toggle(n))
            btn.grid(row=r, column=c, padx=1, pady=1)
            self.seat_buttons[name] = btn

    def _toggle(self, name):
        self.selected ^= {name}
        self.seat_buttons[name].config(bg="#5cb85c" if name in self.selected else "#e0e0e0")

    def _book(self):
        if not self.current_event or not self.selected:
            return messagebox.showinfo("Booking", "Select an event and at least one seat.")
        eid, seats = self.current_event, sorted(self.selected)

        def done(res, err):
            if err:
                messagebox.showerror("Booking failed", str(err))
            else:
                messagebox.showinfo("Booked", f'Booking {res["booking"]["id"]}: {", ".join(seats)}')
                self.selected = set()
                self._refresh_bookings()
            self._load_seats(eid)
        self.run_async(lambda: self.client.request("book", event_id=eid, seats=seats), done)

    def _cancel(self):
        sel = self.booking_list.curselection()
        if not sel:
            return
        bid = self.bookings[sel[0]]["id"]

        def done(_, err):
            if err:
                messagebox.showerror("Cancel failed", str(err))
            self._refresh_bookings()
        self.run_async(lambda: self.client.request("cancel", booking_id=bid), done)

    def _on_close(self):
        if self.client:
            self.client.close()
        self.destroy()


if __name__ == "__main__":
    ap = argparse.ArgumentParser() if False else None
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5050)
    a = ap.parse_args()
    App(a.host, a.port).mainloop()