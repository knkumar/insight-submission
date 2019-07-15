# Code design and organizaton for purchase analytics problem


##  Problem Statement
Calculate Purchase analytics statistics using basic python data structures.
For each department calculate:

1. Number of times a product was requested
2. Number of times a product was requested for the first time
3. Ratio of the two numbers

```
Input : multiple csv files with a certain number of records and attributes (columns)

Design :
	For each csv file:
		identify number of rows, columns

		intialize a hash table for unique identifier
			- key is the unique identifier
			- value is the row number
		initalize empty list for each column

		process rows by
			1. adding unique identifier to hash table
			2. adding each column value to the corresponding list (may need to check for type consistency?)

Operation to implement:
	Join - merge data from different csv on a key - explore merge based and has based joins
		1. check if key to join in unique
	Grouping - Use a hash table for grouping (departments)
	Aggregation - count from the hash table 


Output : results in a CSV file

```

## readData.csv
readData.csv define the readData class which defines the data class with the following attributes and methods

	Attributes
    ------------
    data : list of lists representing columns in data
    index : column on which index is created, defaults to none
    nrows : number of rows in data
    ncols : number of columns in data
    col_name : dictionary of column names in data frame mapping to index in data list
    col_types : types of data for conversion when parsing

    Methods
    ----------
    get_row_by_num : Select multiple rows given number and allow option to skip columns.
    _get_col_by_num : Select multiple columns from data object.
    _get_col_by_name : Select column given column name.
    select_columns : Select multiple columns from data object.
    select_rows : Select multiple rows from data object.
    create_index : Create index adding elements one at a time. This is useful when reading a file to create index on the fly.
    _create_index_column : Create index on an existing column.
    group : Create an index for given column.
    aggregate : Function for future implementation.


## merge_data.py
Perform left join on the data objects. Larger data is assigned to left and smaller data is assigned to right. Ideally the column to be joined on has unique values to facilitate quick processing of the join. If the join is performed on non-unique column the design of the data containment structure needs to be revisited.

	Parameters
    ----------
    data_join : first object to merge (1)
    data : second object to merge (2)
    column : column to perform merge on

    Returns
    -------
    data object merged on common column

## purchase_analytics.py
The "main" file which 
1. Parses arguments from the command line
```
# 1. input order file location to read file from
# 2. order index column
# 3. input products file location to read file from
# 4. product index column
# 5. output file location to write results
```
2. Reads data from csv and selects relevant columns for processing
3. Merge data objects based on column name for aggregation and processing
4. Group merged products-orders by department
5. Groups orders by reordered status and get items ordered for first time
6. Create result structure to store department counts
7. Sort department and write to file

## create_test.py
Use pandas to select a subset of the large file for testing



