import pandas as pd
from bs4 import BeautifulSoup
 

# предположительно надо куда то сохранить таблички адекватно, я пока не знаю куда
folder_path = './htmls/'
save_path = './tables/'

# типы объектов
objtype_path = 'objtype.html'

with open(folder_path + objtype_path, 'r') as f:
        soup = BeautifulSoup(f, 'html.parser')
tables = soup.find_all('table')

objtype_df = pd.read_html(str(tables))[0] 
objtype_df.to_csv(save_path + 'objtype.csv', index=False)
#print(objtype_df.head())

# адекватные меры измерения с описанием
objdesc_path ='objdesc.html'

with open(folder_path + objdesc_path, 'r') as f:
        soup = BeautifulSoup(f, 'html.parser')
tables = soup.find_all('table')

units_df = pd.read_html(str(tables))[10]
units_df = units_df.rename(columns=units_df.iloc[0]).drop(units_df.index[0])
units_df = units_df.reset_index(drop=True)
units_df = units_df[["Parameter", "Unit", "Description"]]
units_df.to_csv(save_path + 'units.csv', index=False)
#print(units_df.head(10))
