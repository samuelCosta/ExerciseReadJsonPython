import json
import sqlite3
import itertools

conn = sqlite3.connect('teste_db.db')

cursor = conn.cursor()
cursor.execute('''DROP TABLE IF EXISTS ORDERS;''')
cursor.execute('''CREATE TABLE IF NOT EXISTS orders
                 (order_no INT  NOT NULL,
                 order_entry_data TEXT NOT NULL,
                 order_delivery_date TEXT NOT NULL,
                 customer_no TEXT NOT NULL,
                 product_no TEXT NOT NULL,
                 qty NUMBER NOT NULL,
                 unit_price NUMBER NOT NULL,
                 FOREIGN KEY(customer_no) REFERENCES customer(customer_no),
                 FOREIGN KEY(product_no) REFERENCES product(product_no));''')

cursor.execute('''DROP TABLE IF EXISTS customers;''')                 
cursor.execute('''CREATE TABLE IF NOT EXISTS customers
                 (customer_no TEXT PRIMARY KEY NOT NULL,
                 customer_name TEXT NOT NULL,
                 customer_address TEXT NOT NULL);''')

cursor.execute('''DROP TABLE IF EXISTS products;''')
cursor.execute('''CREATE TABLE IF NOT EXISTS products
                 (product_no TEXT PRIMARY KEY NOT NULL,
                 product_name TEXT NOT NULL,
                 product_cat TEXT NOT NULL);''')

                 
customers = [] 
products = []
list_order = []


with open('data_example.json') as json_file:
    data = json.load(json_file)
    
    for idx, obj in enumerate(data):

        customers.append(data[idx]["customer"])
        order_items = data[idx]["order_items"]
        for key, obj in enumerate(order_items):
            order = {}
            order['order_no'] = data[idx]["order_no"]
            order['order_entry_data'] = data[idx]["order_entry_data"]
            order['order_delivery_date'] = data[idx]["order_delivery_date"]
            order['customer_no'] = data[idx]["customer"]["customer_no"]
            products.append(order_items[key]['product'])
            order['product_no'] = order_items[key]['product']['product_no']
            del order_items[key]['product']
            order['qty'] = order_items[key]["qty"]
            order['unit_price'] = order_items[key]["unit_price"]
            # print(order)
            list_order.append(order)
            #print(list_order)
            
        del data[idx]['order_items']
        del data[idx]['customer']
        


cursor.executemany('INSERT INTO orders (order_no, order_entry_data, order_delivery_date, customer_no, product_no, qty, unit_price) '
                 'VALUES (:order_no,:order_entry_data,:order_delivery_date,:customer_no,:product_no,:qty,:unit_price)', list_order)
cursor.executemany('INSERT INTO customers (customer_no, customer_name, customer_address) '
                 'VALUES (:customer_no,:customer_name,:customer_address)', customers)  
cursor.executemany('INSERT INTO products (product_no, product_name, product_cat) '
                 'VALUES (:product_no,:product_name,:product_cat)', products)                   
conn.commit()  
        
        
        
cursor.execute("SELECT * FROM orders")
rows = cursor.fetchall()
for row in rows:
    print(row)
print("\n")
cursor.execute("SELECT * FROM customers")
rows = cursor.fetchall()
for row in rows:
    print(row)
print("\n")
cursor.execute("SELECT * FROM products")
rows = cursor.fetchall()
for row in rows:
    print(row)


    




