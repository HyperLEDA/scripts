import pandas as pd
import numpy as np


df = pd.read_csv("./alfalfa/tables/raw_info.csv", sep="  ", engine="python")
df.rename(columns={
    "Units": "unit", 
    "Label": "name",
    "Explanations": "description"}
    , inplace=True)
df = df[["name", "unit","description"]]

def check_nans(row) -> str: 
    if row["unit"] == "---":
        return np.nan
    return row["unit"]

df['unit'] = df.apply(check_nans, axis=1)

def escape_percent(value):
    if isinstance(value, str):
        return value.replace('%', '%%')
    return value

df['description'] = df['description'].apply(escape_percent)


# also replacing dots with NaN in catalog data
def check_nans_in_df(row) -> str: 
    if row["Name"] == "........":
        return np.nan
    return row["Name"]

data = pd.read_csv("./alfalfa/tables/main_data.csv")
data['Name'] = data.apply(check_nans_in_df, axis=1)
data.to_csv(f"./alfalfa/tables/main_data.csv", index=False)


table_columns = data.dtypes
table_columns = pd.DataFrame({'name':table_columns.index, 'data_type':table_columns.values})
table_columns = table_columns.replace({
    "int64": "int",
    "float64": "float",
    "object": "str",
    })

table_columns = pd.merge(table_columns, df, on="name", how="left")
table_columns.to_csv(f"./alfalfa/tables/main_info.csv", index=False)