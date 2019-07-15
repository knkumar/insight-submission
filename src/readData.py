from collections import defaultdict
import re


class Data(object):
    """
    Read data class implements routines to read data from a source

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

    """

    def __init__(self, index=None):
        self.data = []  # [ [col1],[col2], ... ]
        if index is not None:
            # index for accessing by unique identifier
            # mapping from value to row number
            self.index = defaultdict(list)
        else:
            # set index to None for testing if a index exists
            self.index = None
        self.nrows = 0
        self.ncols = 0
        # mapping from column names to index
        self.col_names = {}
        self.col_types = []

    def get_row_by_num(self, num, skip=None):
        """
        Select multiple rows given number and allow option to skip columns.
        Parameters
        ----------
        num : row numbers to select from data object
        skip : column numbers to skip
        Returns
        -------
        list of columns with selected rows
        """
        if type(num) is int:
            num = [num]
        if type(skip) is int:
            skip = [skip]
        result = []
        for col_id, col in enumerate(self.data):
            if skip and col_id in skip:
                continue
            result.append([col[row_num] for row_num in num])
        return result

    def _get_col_by_num(self, num):
        return self.data[num]

    def _get_col_by_name(self, name):
        return self.data[self.col_names[name]]

    def select_columns(self, columns):
        """
        Select multiple columns from data object.
        Parameters
        ----------
        columns : column to select from data
        Returns
        -------
        data object with only selected columns
        """
        select_data = Data()
        for col_id, col_name in enumerate(columns):
            select_data.data.append(self._get_col_by_name(col_name))
            select_data.col_names[col_name] = col_id
        select_data.nrows = len(select_data.data[0])
        select_data.ncols = len(columns)

        return select_data

    def select_rows(self, rows):
        """
        Select multiple rows from data object.
        Parameters
        ----------
        rows : rows to select from data
        Returns
        -------
        data object with only selected rows
        """
        select_data = Data()
        rows.sort()
        select_data.data = [[] for _ in range(self.ncols)]

        for row_id, row_num in enumerate(rows):
            current_row = self.get_row_by_num(row_num)
            for col_id, value in enumerate(current_row):
                select_data.data[col_id].append(value)

    def create_index(self, identifier, linenum, csv_name, unique=True):
        """
        Create index adding elements one at a time. This is useful when reading a file to create index on the fly.
        Parameters
        ----------
        identifier : object to merge 
        linenum : column to merge on
        csv_name : name of csv file for error in indexing
        unique : determines if column should have unique values and single row for each value
        Returns
        -------
        data object merged on common column

        """
        self.index = defaultdict(list)
        # raise exception when duplicates occur in indexed column
        if unique and (identifier in self.index):
            raise csvError(csv_name,
                           "Indexed column has non-unique values.\nThe column specified for index could be incorrect \
                            or the file could contain errors.\nPlease check your function call.")
        else:
            self.index[identifier].append(linenum)

    def _create_index_column(self, column):
        """
        Create index on an existing column.
        Parameters
        ----------
        column : column to group by
        Returns
        -------
        data object grouped on column
        """
        unique = True
        group = defaultdict(list)
        for linenum, item in enumerate(self.data[column]):
            if unique and item in group:
                unique = False
            group[item].append(linenum)
        return group, unique

    def group(self, column):
        """
        Create an index for given column.
        Parameters
        ----------
        column : column to group by
        Returns
        -------
        data object grouped on column
        """
        if type(column) == str:
            column = self.col_names[column]
        group, unique = self._create_index_column(column)
        return group

    def aggregate(self, type):
        pass


def read_csv(csv_name, index=None, unique=True):
    """
    read a csv file and create a data object.
    Parameters
    ----------
    csv_name : path to csv file for reading data in csv format
    index : column number to create index on. Use None to not create an index
    unique : indexed column should have unique values (set to False is not unqiue)

    Returns
    -------
    data object from csv file
    """
    csv_data = Data(index)
    print("Reading file {0}".format(csv_name))
    f_handle = open(csv_name, "r")
    for linenum, line in enumerate(f_handle.readlines()):
        # process header of csv file to gather informaton about column names
        if linenum == 0:
            values = line.strip().split(',')
            csv_data.ncols = len(values)
            csv_data.data = [[] for _ in range(csv_data.ncols)]
            for col_id, col_name in enumerate(values):
                csv_data.col_names[col_name] = col_id
            continue
        if '"' in line:
            match = re.match(r'(\d*),(.*),(\d*),(\d*)', line)
            values = list(match.groups())
        else:
            values = line.strip().split(',')
        # check for values equal to number of columns established
        assert(csv_data.ncols == len(values))
        # read data from csv
        for idx, item in enumerate(values):
            if linenum == 1:
                try:
                    item = int(item)
                    csv_data.col_types.append(int)
                except:
                    item = item
                    csv_data.col_types.append(str)
            # values[idx] = item
            else:
                item = csv_data.col_types[idx](item)
            csv_data.data[idx].append(item)
        # create index if requested
        if index != None:
            ind_col = csv_data.data[index]
            csv_data.create_index(ind_col[-1], linenum, csv_name, unique)
    csv_data.nrows = len(csv_data.data[0])
    return(csv_data)


class csvError(Exception):
    """
    Exception raised for errors in the csv.

    Parameters
    ----------
        filename : input filename in which the error occurred
        message : explanation of the error
    """

    def __init__(self, filename, message):
        self.filename = filename
        self.message = message

    # print message after raise using __str__
    def __str__(self):
        return "\nError when processing: '{0}'\n{1}\n".format(self.filename, self.message)
