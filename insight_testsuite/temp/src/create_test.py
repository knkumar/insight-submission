import pandas as pd
import timeit

orders = pd.read_csv("../insight_testsuite/tests/test_2/input/order_products.csv")
products = pd.read_csv("../insight_testsuite/tests/test_2/input/products.csv")

merged = orders.merge(products)

department_count = merged[['department_id','reordered']].groupby(['department_id']).count()
department_ordered = merged[['department_id','reordered']].groupby(['department_id']).sum()

result = pd.DataFrame()

result = result.assign(department_id=department_count.index)
result.index = department_count.index
result = result.assign(number_of_orders=department_count['reordered'])
result = result.assign(number_of_first_orders=(department_count-department_ordered))
result = result.assign(percentage=(result['number_of_first_orders']/result['number_of_orders']).round(2) )

print(result)
result.to_csv('result.csv', sep=',', index=False, float_format='%.2f')