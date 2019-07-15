from readData import read_csv
from merge_data import merge_data
from collections import defaultdict
import sys


# if parameters are not given show error and exit
# 1. input order file location to read file from
# 2. order index column
# 3. input products file location to read file from
# 4. product index column
# 5. output file location to write results
if len(sys.argv) != 6:
    print("Usage is python py_file input-orders order-index-column input-products product-index-column output\n")
    sys.exit(1)

try:
    input_orders = sys.argv[1]
    order_index = int(sys.argv[2])
    input_products = sys.argv[3]
    product_index = int(sys.argv[4])
    output_file = sys.argv[5]
except (ValueError, IndexError) as e:
    print("Parsing error for arguments {0}".format(sys.argv[1:]))

# read data from csv and select relevant columns for processing
orders_data = read_csv(input_orders, order_index, False)
orders_data = orders_data.select_columns(['product_id', 'reordered'])
products_data = read_csv(input_products, product_index)
products_data = products_data.select_columns(['product_id','department_id'])

# merge products and orders data based on column name
products_orders = merge_data(products_data, orders_data, 'product_id')

# group merged products-orders by department
department_group = products_orders.group('department_id')
# groups orders by reordered status and get items ordered for first time
reordered_group = products_orders.group('reordered')
first_order = set(reordered_group[0])

# create department structure
result = defaultdict(list)
for department in department_group:
    department_row = set(department_group[department])
    result[department].append(department)
    # count number of orders in department
    result[department].append(len(department_row))
    # count number of orders which also are first order
    result[department].append(len(department_row.intersection(first_order)))
    result[department].append(float(result[department][2])/result[department][1])

# sort department and write to file
with open(output_file,"w") as result_file:
    result_file.write("department_id,number_of_orders,number_of_first_orders,percentage\n")
    for item in sorted(result.keys()):
        ans = result[item]
        result_file.write("{0},{1},{2},{3:0.2f}\n".format(ans[0],ans[1],ans[2],ans[3]))
