"""
NovaTech - Concurrent Order Processing Simulator
Phase 2: Synchronization logic (workers)

This module defines:
  - A SharedState object that groups all shared resources and their
    locks (inventory, counters, results log).
  - The worker() function that each Worker thread runs: it takes orders
    from the queue, simulates processing time OUTSIDE the lock, and
    validates + deducts inventory INSIDE a minimal critical section.

Key design decisions (explained further in the report, Phase 5):

1. A single Lock protects the inventory (`self.inventory_lock`). We chose
   a single lock (instead of one per product) because the catalog is
   small (5 products) and an order can contain several items across
   different products: using per-product locks would require a
   multi-lock acquisition protocol with deadlock risk (lock ordering).
   With a single lock we avoid that complexity; the cost in concurrency
   is acceptable because the critical section is very short (only reads
   and integer subtraction, no I/O).

2. The work simulation (0.5 to 2 seconds, time.sleep) happens BEFORE
   acquiring the lock, never inside it. This satisfies the assignment's
   rule: "Do not hold a lock during artificial waits... or simulation
   pauses."

3. Counters (approved, rejected, error) and the event log use the SAME
   lock as the inventory when they are updated together with the
   deduction (so the count stays consistent with the inventory state at
   all times), but a separate lock (`counters_lock`) is used for cases
   that do not touch inventory (invalid orders), so an invalid order
   does not compete for the inventory lock unnecessarily.

4. queue.Queue.get() is already thread-safe: two workers can never pull
   the same Order from the queue. That is why "duplicate order" cannot
   occur at the queue level; the real risk is in the inventory, which is
   what we explicitly protect here.
"""

import threading
import time
import random
from datetime import datetime
from queue import Queue, Empty


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

class SharedState:
    """
    Groups all the resources the threads share, along with the
    synchronization mechanisms that protect them.

    inventory:        dict code -> {name, stock}. Protected by
                       inventory_lock.
    inventory_lock:    Lock protecting the critical section: checking
                       stock + deducting it, as ONE atomic operation.
    counters:          dict with approved/rejected/error counts.
                       Protected by counters_lock (or by inventory_lock
                       when the update happens together with the
                       deduction, to keep a single short critical
                       section).
    counters_lock:     Separate lock for counter updates that don't
                       require touching inventory (e.g. invalid orders).
    events:            list of (timestamp, message) tuples for the final
                       log. Protected by events_lock.
    events_lock:       Lock for the log, so lines don't interleave at
                       the character level when printing.
    active_workers:    count of workers still alive, which the monitor
                       checks periodically. Also protected by
                       counters_lock (it's just another figure).
    """

    def __init__(self, initial_inventory):
        self.inventory = initial_inventory
        self.inventory_lock = threading.Lock()

        self.counters = {
            "approved": 0,
            "rejected": 0,
            "error": 0,
            "processed": 0,
        }
        self.counters_lock = threading.Lock()

        self.events = []
        self.events_lock = threading.Lock()

        self.active_workers = 0

    # -- logging utilities -------------------------------------------------

    def log_event(self, message: str):
        """
        Appends a line to the shared log and prints it immediately.
        A short lock is used only for the 'print' + append, never for
        slow work, so it does not create significant contention even if
        many threads write at the same time.
        """
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        line = f"[{timestamp}] {message}"
        with self.events_lock:
            self.events.append(line)
            print(line, flush=True)


# ---------------------------------------------------------------------------
# Order validation (outside the critical section: it is pure logic,
# it does not touch the shared inventory).
# ---------------------------------------------------------------------------

def validate_order_structure(order, inventory_codes):
    """
    Checks that the order is well-formed BEFORE attempting to touch the
    inventory. No lock is required because it only reads data that
    belongs to the order plus a list of valid codes that does not change
    at runtime.

    Returns (is_valid: bool, reason: str).
    """
    if not order.customer or not order.customer.strip():
        return False, "Empty or invalid customer"

    if not order.items:
        return False, "Order with no items"

    for item in order.items:
        if item.product_code not in inventory_codes:
            return False, f"Nonexistent product: {item.product_code}"
        if item.quantity is None or item.quantity <= 0:
            return False, f"Invalid quantity ({item.quantity}) for {item.product_code}"

    return True, ""


# ---------------------------------------------------------------------------
# Function executed by each worker thread
# ---------------------------------------------------------------------------

def worker(worker_name: str, order_queue: Queue, state: SharedState,
           simulate_time=True, wait_range=(0.5, 2.0)):
    """
    Life cycle of a worker thread (RF-03, RF-04, RF-05, RF-06, RF-08):

      1. Pulls an order from the shared queue (thread-safe by design of
         queue.Queue). If the queue is empty, the worker terminates
         (that is its termination condition, per the assignment's
         table).
      2. Validates the order's STRUCTURE (without touching inventory).
         If it is malformed, it is logged as an error and the worker
         moves on to the next order, without stopping the other workers
         (RF-08).
      3. Simulates processing time (0.5 to 2s) OUTSIDE any lock, as
         required by the assignment.
      4. Enters the CRITICAL SECTION: checks stock and deducts inventory
         as a single atomic operation protected by
         state.inventory_lock. If stock is insufficient, the order is
         rejected WITHOUT modifying the inventory.
      5. Logs the result (approved/rejected/error) with time, thread,
         id, customer, and rejection reason.

    With try/except around the processing of each order, a problematic
    order can never bring down the entire thread (RF-08).
    """
    with state.counters_lock:
        state.active_workers += 1

    state.log_event(f"[{worker_name}] Thread started.")

    try:
        while True:
            try:
                # Non-blocking extraction: if there are no orders left,
                # the worker terminates instead of waiting indefinitely
                # (termination condition: "when there are no pending
                # orders left").
                order = order_queue.get_nowait()
            except Empty:
                break

            try:
                state.log_event(
                    f"[{worker_name}] Starting order {order.order_id} | Customer: {order.customer}"
                )

                # --- 1) Structure validation (outside the lock) ---
                is_valid, reason = validate_order_structure(
                    order, state.inventory.keys()
                )
                if not is_valid:
                    with state.counters_lock:
                        state.counters["error"] += 1
                        state.counters["processed"] += 1
                    state.log_event(
                        f"[{worker_name}] {order.order_id} ERROR | Reason: {reason}"
                    )
                    continue

                # --- 2) Work simulation, OUTSIDE the critical section ---
                if simulate_time:
                    time.sleep(random.uniform(*wait_range))

                # --- 3) CRITICAL SECTION: check + deduct inventory ---
                # This is the only part protected by inventory_lock.
                # It is kept as short as possible: only comparisons and
                # integer subtraction, no sleep or I/O inside.
                approved = False
                deduction_details = []
                rejection_reason = ""

                with state.inventory_lock:
                    # Check that ALL items in the order have enough
                    # stock before deducting any of them (so an order
                    # with several products is all-or-nothing).
                    sufficient = all(
                        state.inventory[item.product_code]["stock"] >= item.quantity
                        for item in order.items
                    )

                    if sufficient:
                        for item in order.items:
                            state.inventory[item.product_code]["stock"] -= item.quantity
                            deduction_details.append(f"{item.product_code}: -{item.quantity} units")
                        approved = True
                    else:
                        # Identify which product fell short, just for
                        # the message (does not modify anything).
                        for item in order.items:
                            available = state.inventory[item.product_code]["stock"]
                            if available < item.quantity:
                                rejection_reason = f"Insufficient stock {item.product_code} (available={available}, requested={item.quantity})"
                                break

                # --- 4) Result logging (outside the inventory lock) ---
                with state.counters_lock:
                    state.counters["processed"] += 1
                    if approved:
                        state.counters["approved"] += 1
                    else:
                        state.counters["rejected"] += 1

                if approved:
                    state.log_event(
                        f"[{worker_name}] {order.order_id} APPROVED | {', '.join(deduction_details)}"
                    )
                else:
                    state.log_event(
                        f"[{worker_name}] {order.order_id} REJECTED | {rejection_reason}"
                    )

            except Exception as exc:
                # Any unexpected error while processing ONE order must
                # not stop the worker or the other workers (RF-08).
                with state.counters_lock:
                    state.counters["error"] += 1
                    state.counters["processed"] += 1
                state.log_event(
                    f"[{worker_name}] {order.order_id} UNEXPECTED ERROR | {exc}"
                )
    finally:
        with state.counters_lock:
            state.active_workers -= 1
        state.log_event(f"[{worker_name}] Thread finished. No pending orders.")


# ---------------------------------------------------------------------------
# Quick self-test for Phase 2 (creates 3 workers manually, no monitor yet
# -- the monitor and the full main thread arrive in Phase 3).
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from models import create_inventory, generate_orders, create_order_queue

    inventory = create_inventory()
    orders = generate_orders()
    order_queue = create_order_queue(orders)
    state = SharedState(inventory)

    threads = []
    for i in range(1, 4):
        t = threading.Thread(target=worker, args=(f"WORKER-{i}", order_queue, state), daemon=False)
        threads.append(t)

    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    end = time.time()

    print("\n--------------------------------------------------------------------")
    print("SUMMARY (Phase 2 test, no monitor)")
    print(f"Processed: {state.counters['processed']} | "
          f"Approved: {state.counters['approved']} | "
          f"Rejected: {state.counters['rejected']} | "
          f"Error: {state.counters['error']}")
    print(f"Total time: {end - start:.2f} s")
    print("\nFinal inventory:")
    for code, data in state.inventory.items():
        print(f"  {code}: {data['name']:<22} stock={data['stock']}")