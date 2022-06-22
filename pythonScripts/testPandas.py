import pandas as pd
import psycopg2


def get_df(path, schema):
    df = pd.read_csv(
        path,
        header=None,
        names=schema
    )
    return df

def load_orders(connection, cursor, query, data, batch_size=1000):
    for i in range(0,len(data),batch_size):
        cursor.executemany(query, data[i:i+batch_size])
        connection.commit()

def get_connection(host, port, database, user, password):
    connection = None
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
    except Exception as e:
        raise (e)

    return connection

host = 'localhost'
port = '5432'
database = 'itversity_retail_db'
user = 'itversity_retail_user'
password = 'retail_password'

sms_connection = get_connection(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password
)

orders_path = "C:/Users/aeins/Projects/internal/bootcamp/data-engineering-spark/data/retail_db/orders/part-00000"
orders_schema = [
    "order_id",
    "order_date",
    "order_customer_id",
    "order_status"
]
orders = get_df(orders_path, orders_schema)

order_items_path = "C:/Users/aeins/Projects/internal/bootcamp/data-engineering-spark/data/retail_db/order_items/part-00000"
order_items_schema = [
    "order_item_id",
    "order_item_order_id",
    "order_item_product_id",
    "order_item_quantity",
    "order_item_subtotal",
    "order_item_product_price"
]
order_items = get_df(order_items_path, order_items_schema)

cursor = sms_connection.cursor()
query = ("""INSERT INTO order_items
         (order_item_id, order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price)
         VALUES
         (%s, %s, %s, %s, %s, %s)""")

load_orders(sms_connection, cursor, query, order_items.values.tolist())

cursor.close()
sms_connection.close()