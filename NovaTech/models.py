"""
NovaTech - Concurrent Order Processing Simulator
Phase 1: Data structures and inventory

This module defines:
  - The initial inventory (shared across threads).
  - The Order data structure.
  - Generation of at least 20 test orders.
  - The shared (thread-safe) queue used by the worker threads.

Design note: "data" (this file) is kept separate from "synchronization"
(Phase 2), so the lock that protects the inventory is defined and used
together with the worker logic, not here.
"""

from dataclasses import dataclass, field
from queue import Queue
import random


# ---------------------------------------------------------------------------
# Initial inventory
# ---------------------------------------------------------------------------
# Represented as a simple dictionary: code -> {name, stock}.
# This dictionary will be the "shared resource" that workers read from and
# modify in Phase 2, protected by a Lock.

INITIAL_INVENTORY = {
    "P001": {"name": "Mechanical keyboard", "stock": 12},
    "P002": {"name": "Wireless mouse",      "stock": 18},
    "P003": {"name": "USB headphones",      "stock": 10},
    "P004": {"name": "Webcam",              "stock": 8},
    "P005": {"name": "24-inch monitor",     "stock": 6},
}


def create_inventory():
    """
    Returns a fresh copy of the initial inventory.

    We use a function (instead of reusing the global dictionary directly)
    so every run of the program (or every test case) starts from a clean
    state, without carrying over changes from a previous run.
    """
    return {
        code: {"name": data["name"], "stock": data["stock"]}
        for code, data in INITIAL_INVENTORY.items()
    }


# ---------------------------------------------------------------------------
# Order structure
# ---------------------------------------------------------------------------

@dataclass
class OrderItem:
    """A single product requested within an order, with its quantity."""
    product_code: str
    quantity: int


@dataclass
class Order:
    """
    Represents a customer order.

    order_id:       unique identifier (e.g. 'ORD-007').
    customer:       customer name.
    items:          list of OrderItem (an order can contain 1+ products).
    is_valid:       flag we set to False when the order is malformed
                    (missing code, quantity <= 0, etc.). This lets us
                    generate intentionally invalid orders for test case
                    CP-04 without breaking the data-loading step.
    invalid_reason: reason for invalidity, if any.
    """
    order_id: str
    customer: str
    items: list = field(default_factory=list)
    is_valid: bool = True
    invalid_reason: str = ""


# ---------------------------------------------------------------------------
# Test order generation (>= 20, covering normal, contention, insufficient
# stock, and malformed cases)
# ---------------------------------------------------------------------------

def generate_orders(count: int = 24, seed: int = 42):
    """
    Generates a list of test orders.

    Intentionally included:
      - Normal orders (sufficient stock).
      - Several orders competing for the same low-stock product (to force
        real contention on the lock, e.g. P005 which only has 6 units).
      - At least one order requesting more units than available.
      - At least one invalid order (nonexistent code, zero quantity, or
        an incomplete structure) for test case CP-04.

    A fixed seed is used so the run is reproducible, as required by the
    README/rubric.
    """
    random.seed(seed)
    customer_names = [
        "Ana Lopez", "Mario Perez", "Lucia Gomez", "Carlos Ruiz",
        "Elena Torres", "Diego Ramirez", "Sofia Castillo", "Jorge Diaz",
        "Valeria Mendez", "Pablo Rios", "Camila Ortiz", "Andres Vargas",
    ]
    valid_codes = list(INITIAL_INVENTORY.keys())

    orders = []

    # 1) Normal orders: small quantities, within available stock.
    for i in range(1, 13):
        code = random.choice(valid_codes)
        requested_quantity = random.randint(1, 3)
        orders.append(Order(
            order_id=f"ORD-{i:03d}",
            customer=random.choice(customer_names),
            items=[OrderItem(code, requested_quantity)],
        ))

    # 2) Contention orders: several compete for P005 (only 6 units) to
    #    force the synchronization mechanism to be genuinely exercised
    #    (CP-02).
    for i in range(13, 18):
        orders.append(Order(
            order_id=f"ORD-{i:03d}",
            customer=random.choice(customer_names),
            items=[OrderItem("P005", 2)],  # 5 orders x 2 = 10 > 6 available
        ))

    # 3) Order with clearly insufficient stock (CP-03).
    orders.append(Order(
        order_id="ORD-018",
        customer="Mariana Solis",
        items=[OrderItem("P004", 999)],
    ))

    # 4) Invalid / malformed orders (CP-04): nonexistent code, zero
    #    quantity, and negative quantity.
    orders.append(Order(
        order_id="ORD-019",
        customer="Ghost Customer",
        items=[OrderItem("P999", 1)],  # product that does not exist
    ))
    orders.append(Order(
        order_id="ORD-020",
        customer="Roberto Nunez",
        items=[OrderItem("P002", 0)],  # invalid quantity
    ))
    orders.append(Order(
        order_id="ORD-021",
        customer="",  # empty customer -> also invalid
        items=[OrderItem("P001", -2)],
    ))

    # 5) Fill up to 'count' with additional normal orders, in case a
    #    total greater than 21 is requested.
    next_id = 22
    while len(orders) < count:
        code = random.choice(valid_codes)
        orders.append(Order(
            order_id=f"ORD-{next_id:03d}",
            customer=random.choice(customer_names),
            items=[OrderItem(code, random.randint(1, 2))],
        ))
        next_id += 1

    return orders


def create_order_queue(orders):
    """
    Places the list of orders into a queue.Queue, which is thread-safe by
    design (it uses an internal lock). This will be the 'shared order
    queue' that the 3+ workers consume concurrently in Phase 2, each one
    calling queue.get() with no risk of two workers taking the same
    order.
    """
    order_queue = Queue()
    for order in orders:
        order_queue.put(order)
    return order_queue


# ---------------------------------------------------------------------------
# Quick self-test for Phase 1 (this file can be run directly to verify
# that the data is generated correctly before moving on to the threading
# logic).
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    inventory = create_inventory()
    orders = generate_orders()
    order_queue = create_order_queue(orders)

    print("=== INITIAL INVENTORY ===")
    for code, data in inventory.items():
        print(f"  {code}: {data['name']:<22} stock={data['stock']}")

    print(f"\n=== ORDERS GENERATED: {len(orders)} ===")
    for o in orders:
        items_str = ", ".join(f"{it.product_code}x{it.quantity}" for it in o.items)
        print(f"  {o.order_id} | customer='{o.customer}' | items=[{items_str}]")

    print(f"\nShared queue size: {order_queue.qsize()} orders ready to process.")