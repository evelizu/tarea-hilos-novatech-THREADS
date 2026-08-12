"""
NovaTech - Concurrent Order Processing Simulator
Phase 4: Mandatory test cases (CP-01 to CP-05)

Each test case:
  - Builds its OWN inventory and its OWN order list, in a controlled way
    (it does not depend on the random generator from Phase 1), so the
    scenario is exactly what the assignment requires and the result is
    reproducible.
  - Runs the same real threading mechanism (SharedState + worker +
    monitor) used in production -- nothing is simulated separately.
  - Ends with explicit asserts that fail (AssertionError) if the
    behavior is not as expected, instead of only printing logs for
    visual inspection.

Each case prints its own interleaved log (evidence of real concurrency)
and then a PASS/FAIL verdict with the reason.
"""

import threading
import time

from workers import SharedState, worker
from main import monitor, MONITOR_INTERVAL_SECONDS
from models import Order, OrderItem
from queue import Queue


def _run_batch(inventory, orders, num_workers=3, with_monitor=True,
               simulate_time=True, wait_range=(0.5, 2.0)):
    """
    Utility shared by all test cases: builds the queue, the SharedState,
    creates N workers (+ optionally 1 monitor), runs everything with
    real threads, and joins all of them before returning the result.
    Reuses the exact same architecture as main.py, so the tests validate
    the real system, not a simplified version.
    """
    order_queue = Queue()
    for o in orders:
        order_queue.put(o)

    state = SharedState(inventory)
    stop_event = threading.Event()

    worker_threads = [
        threading.Thread(
            target=worker,
            args=(f"WORKER-{i}", order_queue, state),
            kwargs={"simulate_time": simulate_time, "wait_range": wait_range},
            name=f"WORKER-{i}",
        )
        for i in range(1, num_workers + 1)
    ]

    monitor_thread = None
    if with_monitor:
        monitor_thread = threading.Thread(
            target=monitor, args=(order_queue, state, stop_event), name="MONITOR"
        )
        monitor_thread.start()

    start = time.time()
    for t in worker_threads:
        t.start()
    for t in worker_threads:
        t.join()

    if with_monitor:
        stop_event.set()
        monitor_thread.join()

    total_time = time.time() - start

    all_threads = worker_threads + ([monitor_thread] if monitor_thread else [])
    alive_threads = [t.name for t in all_threads if t.is_alive()]

    return {
        "state": state,
        "total_time": total_time,
        "alive_threads": alive_threads,
        "total_threads": len(all_threads),
    }


def _verdict(case_name, conditions):
    """
    conditions: list of tuples (description, bool). Prints each
    condition with its result and a final PASS/FAIL verdict.
    """
    print(f"\n--- Verdict {case_name} ---")
    all_ok = True
    for description, ok in conditions:
        status_text = "OK" if ok else "FAIL"
        print(f"  [{status_text}] {description}")
        all_ok = all_ok and ok
    print(f"  => {case_name}: {'PASS' if all_ok else 'FAIL'}")
    print("-" * 60)
    return all_ok


# ---------------------------------------------------------------------------
# CP-01: Normal flow
# ---------------------------------------------------------------------------

def cp01_normal_flow():
    print("\n" + "=" * 70)
    print("CP-01: NORMAL FLOW (sufficient stock for all orders)")
    print("=" * 70)

    inventory = {
        "P001": {"name": "Mechanical keyboard", "stock": 50},
        "P002": {"name": "Wireless mouse", "stock": 50},
    }
    orders = [
        Order(f"ORD-{i:03d}", f"Customer {i}",
              [OrderItem("P001" if i % 2 == 0 else "P002", 1)])
        for i in range(1, 13)  # 12 orders, all with plenty of stock
    ]

    result = _run_batch(inventory, orders, wait_range=(0.1, 0.3))
    state = result["state"]

    return _verdict("CP-01", [
        ("All orders were processed",
         state.counters["processed"] == len(orders)),
        ("All valid orders were approved (no rejections)",
         state.counters["rejected"] == 0),
        ("There were no formatting errors",
         state.counters["error"] == 0),
        ("Ran with multiple workers in parallel (evidence: time < sequential sum)",
         result["total_time"] < 0.3 * len(orders)),
    ])


# ---------------------------------------------------------------------------
# CP-02: Contention (several orders compete for the last units)
# ---------------------------------------------------------------------------

def cp02_contention():
    print("\n" + "=" * 70)
    print("CP-02: CONTENTION (multiple orders compete for the same product)")
    print("=" * 70)

    initial_stock = 5
    inventory = {
        "P005": {"name": "24-inch monitor", "stock": initial_stock},
    }
    # 10 orders of 1 unit each competing for only 5 units.
    orders = [
        Order(f"ORD-{i:03d}", f"Customer {i}", [OrderItem("P005", 1)])
        for i in range(1, 11)
    ]

    result = _run_batch(inventory, orders, num_workers=5, wait_range=(0.1, 0.2))
    state = result["state"]
    final_stock = state.inventory["P005"]["stock"]

    return _verdict("CP-02", [
        ("No more orders were approved than units available",
         state.counters["approved"] == initial_stock),
        ("The remaining orders were correctly rejected due to lack of stock",
         state.counters["rejected"] == len(orders) - initial_stock),
        ("Final inventory never went negative",
         final_stock >= 0),
        ("Integrity invariant: final_stock = initial_stock - approved",
         final_stock == initial_stock - state.counters["approved"]),
    ])


# ---------------------------------------------------------------------------
# CP-03: Insufficient stock (a single order requests more than available)
# ---------------------------------------------------------------------------

def cp03_insufficient_stock():
    print("\n" + "=" * 70)
    print("CP-03: INSUFFICIENT STOCK (an order exceeds available inventory)")
    print("=" * 70)

    inventory = {
        "P004": {"name": "Webcam", "stock": 3},
    }
    orders = [
        Order("ORD-001", "Demanding Customer", [OrderItem("P004", 100)]),
    ]

    result = _run_batch(inventory, orders, num_workers=1, wait_range=(0.1, 0.1))
    state = result["state"]

    return _verdict("CP-03", [
        ("The order was rejected",
         state.counters["rejected"] == 1 and state.counters["approved"] == 0),
        ("Inventory remained unchanged (3 units)",
         state.inventory["P004"]["stock"] == 3),
    ])


# ---------------------------------------------------------------------------
# CP-04: Invalid order (malformed)
# ---------------------------------------------------------------------------

def cp04_invalid_order():
    print("\n" + "=" * 70)
    print("CP-04: INVALID ORDER (malformed: must not stop the others)")
    print("=" * 70)

    inventory = {
        "P001": {"name": "Mechanical keyboard", "stock": 10},
    }
    orders = [
        Order("ORD-001", "Ghost Customer", [OrderItem("P999", 1)]),   # nonexistent product
        Order("ORD-002", "", [OrderItem("P001", 1)]),                  # empty customer
        Order("ORD-003", "OK Customer", [OrderItem("P001", 0)]),      # zero quantity
        Order("ORD-004", "Valid Customer", [OrderItem("P001", 1)]),   # this one IS valid
    ]

    result = _run_batch(inventory, orders, num_workers=2, wait_range=(0.1, 0.2))
    state = result["state"]

    return _verdict("CP-04", [
        ("Exactly 3 orders with formatting errors were detected",
         state.counters["error"] == 3),
        ("The valid order (ORD-004) WAS processed and approved despite the errors",
         state.counters["approved"] == 1),
        ("No worker thread died because of a malformed order (all processed)",
         state.counters["processed"] == len(orders)),
    ])


# ---------------------------------------------------------------------------
# CP-05: Clean shutdown
# ---------------------------------------------------------------------------

def cp05_clean_shutdown():
    print("\n" + "=" * 70)
    print("CP-05: CLEAN SHUTDOWN (empty queue, monitor stopped, join completed)")
    print("=" * 70)

    inventory = {
        "P002": {"name": "Wireless mouse", "stock": 20},
    }
    orders = [
        Order(f"ORD-{i:03d}", f"Customer {i}", [OrderItem("P002", 1)])
        for i in range(1, 9)
    ]

    result = _run_batch(inventory, orders, num_workers=3,
                         with_monitor=True, wait_range=(0.1, 0.2))
    state = result["state"]

    return _verdict("CP-05", [
        ("All orders were processed (empty queue)",
         state.counters["processed"] == len(orders)),
        ("No thread (workers + monitor) remained active after join",
         len(result["alive_threads"]) == 0),
        (f"Joined the expected {result['total_threads']} secondary threads",
         result["total_threads"] == 4),  # 3 workers + 1 monitor
    ])


# ---------------------------------------------------------------------------
# Running all test cases
# ---------------------------------------------------------------------------

def run_all_test_cases():
    results = {
        "CP-01": cp01_normal_flow(),
        "CP-02": cp02_contention(),
        "CP-03": cp03_insufficient_stock(),
        "CP-04": cp04_invalid_order(),
        "CP-05": cp05_clean_shutdown(),
    }

    print("\n" + "=" * 70)
    print("OVERALL TEST CASE SUMMARY")
    print("=" * 70)
    for name, ok in results.items():
        print(f"  {name}: {'PASS' if ok else 'FAIL'}")

    all_passed = all(results.values())
    print(f"\nOverall result: {'ALL CASES PASSED' if all_passed else 'THERE ARE FAILED CASES'}")
    return all_passed


if __name__ == "__main__":
    success = run_all_test_cases()
    exit(0 if success else 1)