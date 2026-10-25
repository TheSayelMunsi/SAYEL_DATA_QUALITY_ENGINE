import csv
import random
from datetime import datetime, timedelta
import os

# Set deterministic seed
random.seed(42)

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
os.makedirs(DATA_DIR, exist_ok=True)

def generate_customers(num_records=100):
    filepath = os.path.join(DATA_DIR, 'customers.csv')
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['customer_id', 'first_name', 'last_name', 'email', 'city', 'state', 'country', 'signup_date'])
        
        for i in range(1, num_records + 1):
            cid = f"C{i:04d}"
            email = f"user{i}@example.com"
            # Intentional Errors:
            if i == 5: email = "invalid-email.com" # Missing @
            if i == 10: cid = "" # Missing ID
            
            writer.writerow([
                cid, f"First{i}", f"Last{i}", email, "Mumbai", "MH", "India", "2023-01-15"
            ])
        # Intentional Duplicate
        writer.writerow(["C0001", "First1", "Last1", "user1@example.com", "Mumbai", "MH", "India", "2023-01-15"])
    print(f"Generated {filepath}")

def generate_products(num_records=20):
    filepath = os.path.join(DATA_DIR, 'products.csv')
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['product_id', 'product_name', 'category', 'unit_price', 'active_status'])
        
        categories = ['Electronics', 'Clothing', 'Home']
        for i in range(1, num_records + 1):
            pid = f"P{i:03d}"
            price = round(random.uniform(10.0, 5000.0), 2)
            status = 'Y'
            
            # Intentional Errors:
            if i == 3: price = -50.00 # Negative price
            if i == 7: status = 'X' # Invalid status
            
            writer.writerow([pid, f"Product {i}", random.choice(categories), price, status])
    print(f"Generated {filepath}")

def generate_orders(num_records=200):
    filepath = os.path.join(DATA_DIR, 'orders.csv')
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'unit_price', 'discount', 'order_status'])
        
        statuses = ['PLACED', 'SHIPPED', 'DELIVERED', 'CANCELLED']
        for i in range(1, num_records + 1):
            oid = f"O{i:05d}"
            cid = f"C{random.randint(1, 100):04d}"
            pid = f"P{random.randint(1, 20):03d}"
            qty = random.randint(1, 5)
            price = round(random.uniform(10.0, 5000.0), 2)
            discount = round(random.uniform(0.0, 0.3), 2)
            status = random.choice(statuses)
            
            # Intentional Errors & Referential issues:
            if i == 15: qty = -2 # Negative quantity
            if i == 25: cid = "C9999" # Missing customer reference
            if i == 30: pid = "P999" # Missing product reference
            if i == 40: discount = 1.50 # Invalid discount
            
            writer.writerow([oid, cid, pid, "2023-10-01", qty, price, discount, status])
    print(f"Generated {filepath}")

if __name__ == "__main__":
    generate_customers()
    generate_products()
    generate_orders()
