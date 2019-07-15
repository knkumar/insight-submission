#!/bin/bash

# call python file purchase_analytics.py with arguments
# 1. input order file location to read file from
# 2. order index column
# 3. input products file location to read file from
# 4. product index column
# 5. output file location to write results

python src/purchase_analytics.py ./input/order_products.csv 0 ./input/products.csv 0 ./output/report.csv