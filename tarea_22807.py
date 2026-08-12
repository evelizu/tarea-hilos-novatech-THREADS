import threading
import time
import queue
import random

# ==========================================
# PHASE 1: INITIAL DATA AND INVENTORY
# ==========================================

inventory = {
    "P001": {"name": "Mechanical Keyboard", "stock": 12},
    "P002": {"name": "Wireless Mouse", "stock": 18},
    "P003": {"name": "USB Headset", "stock": 10},
    "P004": {"name": "Webcam", "stock": 8},
    "P005": {"name": "24-inch Monitor", "stock": 6}
}

customers = ["Ana Lopez", "Mario Perez", "Carlos Ruiz", "Lucia Gomez", "Jorge Diaz", "Marta Silva"]

orders_queue = queue.Queue()
inventory_lock = threading.Lock()

def generate_orders():
    product_codes = list(inventory.keys())
    
    for i in range(1, 21):
        order_id = f"ORD-{i:03}"
        customer = random.choice(customers)
        requested_product = random.choice(product_codes)
        requested_quantity = random.randint(1, 3)
        
        if i == 15:
            order = {"id": order_id, "customer": customer, "product": "P999", "quantity": 5}
        else:
            order = {"id": order_id, "customer": customer, "product": requested_product, "quantity": requested_quantity}
            
        orders_queue.put(order)

generate_orders()

print(f"Inventory loaded with {len(inventory)} products.")
print(f"Orders queue generated with {orders_queue.qsize()} pending orders.")
print("-" * 50)

# ==========================================
# PHASE 2: THREADING AND WORKERS
# ==========================================

def process_orders(worker_name):
    while not orders_queue.empty():
        try:
            order = orders_queue.get_nowait()
        except queue.Empty:
            break

        product_code = order["product"]
        requested_qty = order["quantity"]

        # Acquire lock to update stock safely
        with inventory_lock:
            if product_code in inventory:
                if inventory[product_code]["stock"] >= requested_qty:
                    inventory[product_code]["stock"] -= requested_qty
                    print(f"[{worker_name}] PROCESSED: Order {order['id']} - Customer: {order['customer']} | Product: {inventory[product_code]['name']} | Qty: {requested_qty} | Remaining stock: {inventory[product_code]['stock']}")
                else:
                    print(f"[{worker_name}] REJECTED: Order {order['id']} - Insufficient stock for product {product_code} (Requested: {requested_qty}, Stock: {inventory[product_code]['stock']})")
            else:
                print(f"[{worker_name}] ERROR: Order {order['id']} - Product code {product_code} does not exist in inventory.")

        time.sleep(0.1)  # Simulate processing time
        orders_queue.task_done()

# Create 3 worker threads
workers = []
for i in range(1, 4):
    t = threading.Thread(target=process_orders, args=(f"Worker-{i}",))
    workers.append(t)
    t.start()

# Wait for all threads to complete
for t in workers:
    t.join()

print("-" * 50)
print("All orders have been processed.")