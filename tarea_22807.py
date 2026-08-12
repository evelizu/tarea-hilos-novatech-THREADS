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