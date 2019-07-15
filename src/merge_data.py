import copy
from readData import Data


def merge_data(data_join, data, column):
    """
    Perform left join on the data objects. Larger data is assigned to left and smaller data is assigned to right.
    Ideally the column to be joined on has unique values to facilitate quick processing of the join.
    If the join is performed on non-unique column the design of the data containment structure needs to be revisited.
    Parameters
    ----------
    data_join : first object to merge (1)
    data : second object to merge (2)
    column : column to perform merge on

    Returns
    -------
    data object merged on common column

    To-Do: work on selecting certain columns from both data when merging.
    """
    # determine the smaller dataset to hash and join
    # set smaller data to right
    if data_join.nrows >= data.nrows:
        data_left = data_join
        data_right = data
    else:
        data_left = data
        data_right = data_join

    try:
        col_left = data_left.col_names[column]
        col_right = data_right.col_names[column]
    except KeyError:
        print("column {0} not in data".format(column))
    # choose which column to create index on depending on which object is merged
    merge_data = Data()
    # add column names from data_left to merge_data
    merge_data.col_names = copy.deepcopy(data_left.col_names)
    # use sub_id to count the column number in merged data
    col_id_to_add = []
    # add column names from data_right to merge_data
    for idx, key in enumerate(data_right.col_names):

        if data_right.col_names[key] == col_right:
            continue
        sub_id = 1 if data_right.col_names[key] > col_right else 0
        merge_data.col_names[key] = data_right.col_names[key] - sub_id + data_left.ncols

        col_id_to_add.append(merge_data.col_names[key])

    # add rows to merge data
    # find indexes to merge on for hash join
    group_right, unique_right = data_right._create_index_column(col_right)
    group_left, unique_left = data_left._create_index_column(col_left)
    if unique_right:
        # merge on unique column - copy the left data and loop for each row
        merge_data.data = copy.deepcopy(data_left.data)

        rows_right = [group_right[row_val][0] for row_val in merge_data.data[col_left]]

        row_to_merge = data_right.get_row_by_num(rows_right, col_right)
        merge_data.data.extend(row_to_merge)

    else:
        # non-unique column merge
        for row_id, row_val in enumerate(data_left.data[col_left]):
            if row_id == 0:
                merge_data.data.extend(data_left.get_row_by_num(group_left[row_val]))
                merge_data.data.extend(data_right.get_row_by_num(group_right[row_val], col_right))
            else:
                # non-unique right so many rows are possible
                right_rows = group_right[row_val]
                for right_row_num in right_rows:
                    row_to_merge = data_left.get_row_by_num(row_id)+data_right.get_row_by_num(right_row_num, col_right)
                    for col_id, col_val in enumerate(row_to_merge):
                        merge_data.data[col_id].append(col_val[0])

    return merge_data
