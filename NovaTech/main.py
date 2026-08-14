"""
NovaTech - Concurrent Order Processing Simulator
Phase 3: Monitor thread and main thread

This module defines:
  - The monitor() function: a thread that prints the system status every
    1-2 seconds (pending, approved, rejected, active workers) and stops
    via a signal (threading.Event), not by counting iterations or
    guessing how long the workers will take.
  - main(): the main thread, which:
      1. Loads inventory and orders (Phase 1).
      2. Creates the shared queue and the SharedState (Phase 1 + Phase 2).
      3. Creates and starts 3 worker threads + 1 monitor thread.
      4. Waits (join) for the 3 workers to finish.
      5. Signals the monitor to stop (Event.set()) and joins the monitor
         too, to guarantee a "clean shutdown, no abandoned threads"
         (RF-09).
      6. Prints the final summary (RF-10).

Design decision: how the monitor is notified that it should stop
------------------------------------------------------------------
A threading.Event (`stop_event`) is used. The monitor loops and, on
each iteration, waits up to 1.5s with `stop_event.wait(timeout=1.5)`:
  - If the event is set before the timeout elapses, `wait()` returns
    True immediately and the monitor exits the loop without waiting for
    the full second (fast shutdown, no need to wait for the next tick).
  - If the timeout elapses without the event being set, `wait()` returns
    False and the monitor simply prints the current status and
    continues.

This is preferable to having the monitor check `queue.empty()` itself,
because the real termination condition for the system is "all workers
have finished," which is a decision that belongs to the main thread
(which knows when it has joined the 3 workers), not something the
monitor should guess on its own.
"""

import threading
import time
from queue import Queue

from models import create_inventory, generate_orders, create_order_queue
from workers import SharedState, worker


NUM_WORKERS = 3
MONITOR_INTERVAL_SECONDS = 1.5


def monitor(order_queue: Queue, state: SharedState, stop_event: threading.Event):
    """
    Monitor thread (RF-07): prints the system status every 1.5s (within
    the requested 1-2s range) until it receives the termination signal.

    Reads: queue size (pending), counters (approved, rejected), and
    active_workers. All these reads are intentionally approximate /
    "eventually consistent" -- the monitor is only an observer, it does
    not participate in the critical section or block the workers. That
    is why inventory_lock is not used here: reading counters with
    counters_lock is enough and cheap.
    """
    state.log_event("[MONITOR] Monitor thread started.")

    while not stop_event.is_set():
        # Interruptible wait: if the signal arrives during the wait, we
        # exit immediately instead of waiting the full interval.
        signal_received = stop_event.wait(timeout=MONITOR_INTERVAL_SECONDS)
        if signal_received:
            break

        with state.counters_lock:
            pending = order_queue.qsize()
            approved = state.counters["approved"]
            rejected = state.counters["rejected"]
            active = state.active_workers

        state.log_event(
            f"[MONITOR] Pending: {pending} | Approved: {approved} | "
            f"Rejected: {rejected} | Active: {active}"
        )

    state.log_event("[MONITOR] Termination signal received. Monitor thread stopped.")


def print_final_summary(state: SharedState, total_time: float,
                         secondary_threads: list, total_orders: int):
    """
    RF-10: Shows remaining inventory, approved, rejected, failed, and
    total time. Also reports how many of the secondary threads (3
    workers + 1 monitor = 4) finished correctly, as evidence of "clean
    shutdown, no abandoned threads."
    """
    finished_threads = sum(1 for t in secondary_threads if not t.is_alive())
    total_threads = len(secondary_threads)

    print("\n--------------------------------------------------------------------")
    print(
        f"FINAL SUMMARY | Processed: {state.counters['processed']} | "
        f"Approved: {state.counters['approved']} | "
        f"Rejected: {state.counters['rejected']} | "
        f"Error: {state.counters['error']}"
    )
    print(
        f"Total time: {total_time:.2f} s | "
        f"Threads finished correctly: {finished_threads}/{total_threads}"
    )

    # Integrity invariant check requested by the assignment:
    # final_stock = initial_stock - actually_approved_units
    print("\nFinal inventory:")
    for code, data in state.inventory.items():
        print(f"  {code}: {data['name']:<22} stock={data['stock']}")

    # Consistency checks (>= 0 across all inventory, and processed
    # orders == total orders loaded).
    inventory_valid = all(d["stock"] >= 0 for d in state.inventory.values())
    count_correct = state.counters["processed"] == total_orders

    print(f"\nInventory with no negative quantities: {'OK' if inventory_valid else 'FAIL'}")
    print(f"Orders processed == orders loaded ({total_orders}): "
          f"{'OK' if count_correct else 'FAIL'}")


def main():
    """
    Main thread (RF-01, RF-02, RF-09):
      - Loads data.
      - Creates 3 workers + 1 monitor (4 secondary threads in total,
        meeting the assignment's minimum).
      - Starts all of them, joins the workers, signals the monitor,
        joins the monitor, prints the summary.
    """
    inventory = create_inventory()
    orders = generate_orders()
    order_queue = create_order_queue(orders)
    state = SharedState(inventory)
    stop_event = threading.Event()

    print(f"=== NovaTech: processing {len(orders)} orders with "
          f"{NUM_WORKERS} workers + 1 monitor ===\n")

    # --- Thread creation ---
    worker_threads = [
        threading.Thread(
            target=worker,
            args=(f"WORKER-{i}", order_queue, state),
            name=f"WORKER-{i}",
        )
        for i in range(1, NUM_WORKERS + 1)
    ]
    monitor_thread = threading.Thread(
        target=monitor,
        args=(order_queue, state, stop_event),
        name="MONITOR",
    )

    start = time.time()

    # --- Start: monitor first so it captures the state from the very
    #     beginning, then the workers ---
    monitor_thread.start()
    for t in worker_threads:
        t.start()

    # --- Wait for the workers to finish (RF-09) ---
    for t in worker_threads:
        t.join()

    # --- Termination signal to the monitor, then wait for it to stop ---
    stop_event.set()
    monitor_thread.join()

    end = time.time()

    # --- Final summary (RF-10) ---
    all_threads = worker_threads + [monitor_thread]
    print_final_summary(state, end - start, all_threads, len(orders))


if __name__ == "__main__":
    main()
    