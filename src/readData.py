import logging
from collections import defaultdict
import re

class Data(object):
    """
    Read data class implements routines to read data from a source

    """

    def __init__(self, index):
        self.data = None # [ [col1],[col2], ... ]
        if index != None:
            self.index = defaultdict(list) # index for accessing by unique identifier
        self.nrows = 0
        self.ncols = 0
        self.colnames = {}
        self.colTypes = []

    def _getrow_by_num(self,):
        pass
    def _getrow_by_name(self,):
        pass
    def _getcol_by_num(self,):
        pass
    def _getcol_by_name(self,):
        pass

    def select_columns(self):
        pass

    def select_rows(self):
        pass

    def merge_data(self, data, column):
        """
        Parameters
        ----------
        data : object to merge 
        column : column to merge on
        Returns
        -------
        data object merged on common column

        To check: if the key is common for both data
        """ 
        try:
            col1 = self.colnames.index(column)
            col2 = data.colnames.index(column)
        except:
            print("column {0} not in data".format(column))

        
        
        pass

    def create_index(self, identifier, linenum, csv_name, unique=True):
        """
        Parameters
        ----------
        identifier : object to merge 
        linenum : column to merge on
        csv_name : 
        unique : 
        Returns
        -------
        data object merged on common column

        To check: if the key is common for both data
        """ 
        # raise exception when duplicates occur in indexed column
        if unique and (identifier in self.index):
            raise csvError(csv_name,"Indexed column has non-unique values.\nThe column specified for index could be incorrect or the file could contain errors.\nPlease check your function call.")
        else:
            self.index[identifier].append(linenum)

    def _create_index_column(self, column, unique):
        """
        Parameters
        ----------
        data : object to merge 
        column : column to group by
        Returns
        -------
        data object grouped on column
        """ 
        # for linenum,item in enumerate(column):
        #     if unique and (item in self.index):
        #         raise csvError(csv_name,"Indexed column has non-unique values.\nThe column specified for index could be incorrect or the file could contain errors.\nPlease check your function call.")
        #     else:
        #         self.index[item].append(linenum)
        
        group = defaultdict(list)
        for linenum, item in self.data[column]:
            group[item].append(linenum)
        return(group)
        

    def group(self,column):
        """
        Parameters
        ----------
        data : object to merge 
        column : column to group by
        Returns
        -------
        data object grouped on column
        """ 
        group = self._create_index_group(column)
        return(group)

    def aggregate(self,type):
        pass
    
def read_csv(csv_name, index=None, unique=True):
    """
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
    print(csv_name)
    f_handle = open(csv_name,"r")
    for linenum, line in enumerate(f_handle.readlines()):
        # process header of csv file to gather informaton about column names
        if linenum == 0:
            values = line.strip().split(',')
            csv_data.ncols = len(values)
            csv_data.data = [[] for _ in range(csv_data.ncols)]
            csv_data.colnames = values
            continue
        if '"' in line:
            match = re.match(r'(\d*),(.*),(\d*),(\d*)',line)
            values = list(match.groups())
            # print(values)
        else:
            values = line.strip().split(',')
        assert(csv_data.ncols == len(values))
        # read data from csv 
        for idx,item in enumerate(values):
            if linenum == 1:
                try:
                    item = int(item)
                    csv_data.colTypes.append(int)
                except:
                    item = item
                    csv_data.colTypes.append(str)
            # values[idx] = item
            else:
                item = csv_data.colTypes[idx](item)
            csv_data.data[idx].append(item)
        # create index if requested
        if index != None:
            ind_col = csv_data.data[index]
            csv_data.create_index(ind_col[-1], linenum, csv_name, unique)
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
        return("\nError when processing: '{0}'\n{1}\n".format(self.filename, self.message))


