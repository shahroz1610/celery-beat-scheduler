import pandas as pd
from pandas.core.frame import DataFrame


def query_csv(df:DataFrame,query_dict:dict)->list:
    """[Function to query all of the rows according to the query]

    Args:
        query (dict): [It has one key-value pair. Key is the name of the column in the csv and value is the value of that column that you want to query]

    Returns:
        list: [Result of query]
    """
    res = df[df[query_dict['key']] == query_dict['value']]
    res_dict = res.to_dict()
    res_list = []
    idx_list = []
    for col in res_dict:
        for idx in res_dict[col]:
            idx_list.append(idx)
    length = len(idx_list)
    for i in range(length):
        d = {}
        for col in res_dict:
            d[col] = res_dict[col][idx_list[i]]
        res_list.append(d)
    return res_list