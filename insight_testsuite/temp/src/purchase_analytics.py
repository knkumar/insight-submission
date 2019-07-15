from readData import read_csv
from merge_data import merge_data
from collections import defaultdict
import sys


if len(sys.argv) != 6:
    print("Usage is python py_file input-orders order-index-column input-products product-index-column output\n")

print(sys.argv)

input_orders = sys.argv[1]
order_index = int(sys.argv[2])
input_products = sys.argv[3]
product_index = int(sys.argv[4])
output_file = sys.argv[5]

orders_data = read_csv(input_orders, order_index, False)
orders_data = orders_data.select_columns(['product_id', 'reordered'])
products_data = read_csv(input_products, product_index)
products_data = products_data.select_columns(['product_id','department_id'])

# merge based on column name
products_orders = merge_data(products_data, orders_data, 'product_id')

# group orders by department
department_group = products_orders.group('department_id')
# groups orders by reordered status and get items ordered for first time
reordered_group = products_orders.group('reordered')
first_order = set(reordered_group[0])

# create department structure
result = defaultdict(list)
for department in department_group:
    department_row = set(department_group[department])
    result[department].append(department)
    result[department].append(len(department_row))
    result[department].append(len(department_row.intersection(first_order)))
    result[department].append(float(result[department][2])/result[department][1])

# sort department and write to file
with open(output_file,"w") as result_file:
    result_file.write("department_id,number_of_orders,number_of_first_orders,percentage\n")
    for item in sorted(result.keys()):
        ans = result[item]
        result_file.write("{0},{1},{2},{3:0.2f}\n".format(ans[0],ans[1],ans[2],ans[3]))
