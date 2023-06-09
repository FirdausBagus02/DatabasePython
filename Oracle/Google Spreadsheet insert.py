import cx_Oracle
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urlparse

# google API connection  
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(r'loacation of your json file', scope)
client = gspread.authorize(credentials)

# Database connection 
user      = 'User Database'
password  = 'Password Database'
dsn       = ''hostname',port_number,service_name='database_name''
conn = cx_Oracle.connect(user, password, dsn)
cursor = conn.cursor()

# Google Sheet connection 
sheet_url = 'Spreadsheet URL'
worksheet_name = 'Worksheet Name'
parsed_url = urlparse(sheet_url)
sheet_key = parsed_url.path.split('/')[3]

# convert google sheet data to pandas dataframe
sheet = client.open_by_url(sheet_url)
worksheet = sheet.worksheet(worksheet_name)
data = worksheet.get_all_values()
df = pd.DataFrame(data[1:], columns=data[0])

for _, row in df.iterrows():
    insert_query = f"INSERT INTO table_name VALUES ({', '.join([':%s' % (i+1) for i in range(len(row))])})"
    cursor.execute(insert_query, row.tolist())

# close the cursor and connection
conn.commit()
cursor.close()
conn.close()

# print the completed process
print("The data has been successfully updated.")
