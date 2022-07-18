import pandas as pd
import datetime
import traceback
import typing
import os
import json
from numpy import nan
from IPython.display import display

# PARAMETERS
ts_mili = "MILISECOND"
ts_mikro = "MIKROSECOND"

breakdown_col = {
    "c": "data",
    "u": "set",
    "d": "set"
}

# CUSTOM FUNCTIONS
def convert_timestamp(ts:int,unit:str=ts_mili) -> datetime.datetime:
    """
    > It converts a timestamp to a datetime object
    
    :param ts: the timestamp to convert
    :type ts: int
    :param unit: The unit of the timestamp
    :type unit: str
    :return: A datetime object
    """
    if unit == ts_mikro:
        ts /= 1000
    try:
        return datetime.datetime.fromtimestamp(ts)
    except Exception as e:
        traceback.print_exc()


def get_list_of_json(dir:str) -> typing.List[dict]:
    """
    > This function takes a directory as input and returns a list of dictionaries, where each dictionary
    is the contents of a json file in the directory
    
    :param dir: the directory where the json files are
    :type dir: str
    :return: A list of dictionaries
    """
    return [json.load(open(os.path.join(dir,x))) for x in os.listdir(dir)]


def separate_update_create(data:typing.List[dict]) -> typing.Dict[str,typing.List[dict]]:
    """
    > This function takes a list of dictionaries and returns a dictionary with three keys, "c", "u", and
    "d", each of which contains a list of dictionaries
    
    :param data: a list of dictionaries, each of which has the following keys:
    :type data: typing.List[dict]
    :return: A dictionary with three keys: "c", "u", and "d". The values are lists of dictionaries.
    """
    separate_dict = {"c": [], "u":[], "d":[]}
    for x in data:
        separate_dict[x["op"]].append(x)
    return separate_dict    


def create_df(data:typing.Dict[str,typing.List[dict]], data_col:dict=breakdown_col, data_col_name:str="data") -> pd.DataFrame:
    """
    > It takes a dictionary of lists of dictionaries, and returns a single dataframe
    
    :param data: the dictionary of dataframes that we created in the previous step
    :type data: typing.Dict[str,typing.List[dict]]
    :param data_col: the column name of the data you want to break down
    :type data_col: dict
    :param data_col_name: the name of the column that will contain the data, defaults to data
    :type data_col_name: str (optional)
    :return: A dataframe
    """
    df_collection = {}
    for key, val in data.items():
        if len(val) == 0:
            df_collection[key] = pd.DataFrame()
            continue
        raw_df = pd.DataFrame(val)
        break_df = pd.DataFrame(raw_df[data_col[key]].to_list())
        combine_df = pd.concat([raw_df,break_df], axis=1).reset_index(drop=True)
        combine_df.rename(columns={data_col[key]:data_col_name},inplace=True)
        df_collection[key] = combine_df
    final_df = pd.concat(list(df_collection.values()), axis=0).reset_index(drop=True)
    return final_df


def load_data(dir:str, timestamp_col:typing.Dict[str,str]={}) -> pd.DataFrame:
    """
    > This function takes in a directory of json files and returns a dataframe of the json files
    
    :param dir: the directory where the json files are stored
    :type dir: str
    :param timestamp_col: a dictionary of column name and unit of time
    :type timestamp_col: typing.Dict[str,str]
    :return: A dataframe with the data from the json files.
    """
    print("Start working ", dir)
    list_of_json = get_list_of_json(dir)
    data_list = separate_update_create(list_of_json)
    final_data = create_df(data_list)
    if timestamp_col != {}:
        for col, unit in timestamp_col.items():
            final_data[col] = final_data[col].apply(lambda x:convert_timestamp(x,unit))
    print("Done working ", dir)
    return final_data


def join_table(left_df:pd.DataFrame, right_df:pd.DataFrame, 
               left_on:str,right_on:str, how:str="inner",
               suffix:tuple=('_left', '_right')) -> pd.DataFrame:
    """
    > Join two tables together using a common column
    
    :param left_df: the left dataframe
    :type left_df: pd.DataFrame
    :param right_df: the dataframe you want to join to the left_df
    :type right_df: pd.DataFrame
    :param left_on: The column name in the left dataframe to join on
    :type left_on: str
    :param right_on: The column name in the right dataframe to join on
    :type right_on: str
    :param how: type of join to perform. Options are 'left', 'right', 'outer', 'inner', defaults to
    inner
    :type how: str (optional)
    :param suffix: tuple of string values to append to column names in case of overlap
    :type suffix: tuple
    :return: A dataframe
    """
    return pd.merge(left_df,right_df,left_on=left_on,right_on=right_on,how=how,suffixes=suffix)


def add_value(x:int,y:int)->int:
    """
    If both x and y are NaN, return NaN. Otherwise, return the sum of x and y
    
    :param x: the first value to add
    :type x: int
    :param y: the column name of the column you want to sum
    :type y: int
    :return: the sum of x and y.
    """
    if pd.isna(x) and pd.isna(y):
        return nan
    else:
        return (x if not pd.isna(x) else 0) + (y if not pd.isna(y) else 0)


def main():
    """ Entrypoint/Wrapper Function
    """
    # SOURCE DECLARATION
    
    # Data information structure
    data_collection = {
        "cards": {
            "url": "data/cards/",
            "data": None
        },
        "accounts": {
            "url": "data/accounts/",
            "data": None
        },
        "savings": {
            "url": "data/savings_accounts/",
            "data": None
        }
    }
    
    for key,val in data_collection.items():
        data_collection[key]["data"] = load_data(val["url"], {"ts":ts_mikro})

    # PROCESSING STEPS

    # Card self join
    create_df = data_collection["cards"]["data"][data_collection["cards"]["data"]["op"] == "c"]
    update_df = data_collection["cards"]["data"][data_collection["cards"]["data"]["op"] == "u"]
    self_update_df = join_table(update_df,create_df[["id","card_id","card_number"]],"id","id",how="left")
    self_update_df = self_update_df.loc[:, ~self_update_df.columns.isin(["card_id_left","card_number_left"])]
    self_update_df.rename(columns={"card_id_right":"card_id","card_number_right":"card_number"},inplace=True)
    data_collection["cards"]["data"] = pd.concat([create_df,self_update_df],axis=0).sort_values(
                                          by=["ts"],ascending=[True]).reset_index(
                                          drop=True)

    # Account self join
    create_df = data_collection["accounts"]["data"][data_collection["accounts"]["data"]["op"] == "c"]
    update_df = data_collection["accounts"]["data"][data_collection["accounts"]["data"]["op"] == "u"]
    self_update_df = join_table(update_df,create_df[["id","account_id"]],"id","id",how="left")
    self_update_df = self_update_df.loc[:, ~self_update_df.columns.isin(["account_id_left"])]
    self_update_df.rename(columns={"account_id_right":"account_id"},inplace=True)
    data_collection["accounts"]["data"] = pd.concat([create_df,self_update_df],axis=0).sort_values(
                                          by=["ts"],ascending=[True]).reset_index(
                                          drop=True)
    
    # Saving self join
    create_df = data_collection["savings"]["data"][data_collection["savings"]["data"]["op"] == "c"]
    update_df = data_collection["savings"]["data"][data_collection["savings"]["data"]["op"] == "u"]
    self_update_df = join_table(update_df,create_df[["id","savings_account_id"]],"id","id",how="left")
    self_update_df = self_update_df.loc[:, ~self_update_df.columns.isin(["savings_account_id_left"])]
    self_update_df.rename(columns={"savings_account_id_right":"savings_account_id"},inplace=True)
    data_collection["savings"]["data"] = pd.concat([create_df,self_update_df],axis=0).sort_values(
                                         by=["ts"],ascending=[True]).reset_index(
                                         drop=True)

    # Complete historical denormalized tables
    denom_account_card_df = join_table(data_collection["accounts"]["data"], 
                                       data_collection["cards"]["data"],
                                       "card_id", "card_id","left",
                                       suffix=("_account","_card"))

    denom_final_df = join_table(denom_account_card_df, data_collection["savings"]["data"],
        "savings_account_id", "savings_account_id","left",suffix=("","_saving"))
    
    denom_final_df["ts_transaction"] = denom_final_df.apply (
        lambda row: row["ts"] if not pd.isnull(row["ts"]) else row["ts_card"], 
        axis=1)

    denom_final_df["transaction_val"] = denom_final_df.apply(
        lambda x:add_value(x["credit_used"],x["balance"]),axis=1)

    # Transaction data
    valid_transaction_df = denom_final_df[[
        "id_account","id_card","id","op_card","op","credit_used","balance",
        "transaction_val","ts_transaction"]][
            ~pd.isnull(denom_final_df["ts_transaction"]) & 
            ~pd.isnull(denom_final_df["transaction_val"])].sort_values(
                by=["ts_transaction"],ascending=[True]).reset_index(drop=True)
    
    valid_transaction_df = valid_transaction_df[
        (valid_transaction_df["op_card"] == "u") | (valid_transaction_df["op"] == "u")].reset_index(
            drop=True)

    vol_transaction_df = valid_transaction_df.groupby(
        by=["ts_transaction"], axis=0,dropna=True)["id_account"].size().reset_index(
            name='transaction_count')
    
    # Fill Unknown
    data_collection["accounts"]["data"].fillna(method="ffill",inplace=True)
    data_collection["savings"]["data"].fillna(method="ffill", inplace=True)
    data_collection["cards"]["data"].fillna(method="ffill",inplace=True)

    # Display processed data
    print("Accounts Table")
    display(data_collection["accounts"]["data"])
    print("Savings Account Table")
    display(data_collection["savings"]["data"])
    print("Cards Table")
    display(data_collection["cards"]["data"])
    print("Complete Denormalized Tables")
    display(denom_final_df)
    print("Transaction Tables")
    display(valid_transaction_df)
    display(vol_transaction_df)


if __name__ == "__main__":
    main()