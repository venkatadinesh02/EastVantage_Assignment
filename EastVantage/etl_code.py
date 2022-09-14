import json
import pandas as pd
import sqlite3


# ----------------------------------Question(1) Connection to SQLite3 -----------------------------
def db_connection():
    conn = None
    try:
        conn = sqlite3.connect('S30 ETL Assignment.db')
    except sqlite3.error as e:
        print(e)
    return conn


# --------------------------- Question(2): Solution Using SQL Query ---------------------------------
conn = db_connection()
cursor = conn.cursor()
cursor.execute("""
                    select sl.customer_id,
                    min(cst.age) As age,
                    it.item_name As Item,
                    sum(od.quantity) As quantity
                    from items it
                    inner join orders od
                    on od.item_id = it.item_id
                    inner join sales sl
                    on sl.sales_id = od.sales_id
                    inner join customers cst
                    on cst.customer_id = sl.customer_id
                    where age>=18 and age<=35 and quantity is not null
                    GROUP by sl.customer_id,it.item_name
                """)

for row in cursor.fetchall():
    print(row)
# ----------------------------------Question(2): Solution using Python Pandas -----------------------------------

conn = db_connection()
cur_customer = conn.cursor()
cur_sales = conn.cursor()
cur_orders = conn.cursor()
cur_items = conn.cursor()

cur_customer.execute("Select * from customers where age>=18 and age<=35;")
cur_sales.execute("Select * from sales;")
cur_orders.execute("Select * from orders where quantity is not null;")
cur_items.execute("Select * from items;")

customer = pd.DataFrame([
    dict(custid=row[0], age=row[1])
    for row in cur_customer.fetchall()
])

sales = pd.DataFrame([
    dict(salesid=row[0], custid=row[1])
    for row in cur_sales.fetchall()
])

orders = pd.DataFrame([
    dict(orderid=row[0], salesid=row[1], itemid=row[2], quantity=int(row[3]))
    for row in cur_orders.fetchall()
])

items = pd.DataFrame([
    dict(itemid=row[0], name=row[1])
    for row in cur_items.fetchall()
])

df = pd.merge(pd.merge(pd.merge(items, orders, on='itemid'), sales, on='salesid'), customer, on='custid')[
    ['custid', 'age', 'name', 'quantity']]

df = df.groupby(['custid', 'name']).agg(dict(age='min', quantity='sum'))
print(df)

# --------------------------Question(3): convert the data to CSV format -------------------------------
df.to_csv('result.csv', sep=";")
