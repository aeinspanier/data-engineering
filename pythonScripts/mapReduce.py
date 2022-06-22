import datetime as dt, calendar as c
from functools import reduce

def my_filter(c, f):
    c_f = []
    for e in c:
        if f(e):
            print(e.split(','))
            c_f.append(e)
    return c_f


def my_map(c,f):
    c_t = []
    for e in c:
        c_t.append(f(e))
    return c_t


def get_order_date(order):
    order_id = int(order.split(',')[0])
    order_date = order.split(',')[1]
    order_date_datetime = dt.datetime.strptime(order_date.split(' ')[0], '%Y-%m-%d')
    order_day_name = c.day_name[order_date_datetime.weekday()]
    return order_id, order_date, order_day_name


def main():
    orders = open('C:/Users/aeins/Projects/internal/bootcamp/data-engineering-spark/data/retail_db/orders/part-00000')\
                .read().splitlines()
    order_items = open('C:/Users/aeins/Projects/internal/bootcamp/data-engineering-spark/data/retail_db/order_items/part-00000')\
                    .read().splitlines()

    items_for_product = filter(lambda order_item: int(order_item.split(',')[2]) == 502, order_items)
    item_subtotals = map(lambda order_item: float(order_item.split(',')[4]), items_for_product)
    total = reduce(lambda total, item_subtotal: total+item_subtotal, item_subtotals)
    print(total)



if __name__ == "__main__":
    main()
