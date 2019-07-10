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

Optimization:
	Grouping - Use a hash table for grouping departments
	Aggregation - 


Output : results in a CSV file

```