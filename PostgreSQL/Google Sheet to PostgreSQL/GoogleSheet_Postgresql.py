import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urlparse

# Google Sheet credentials
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('path/to/key.json', scope)
client = gspread.authorize(credentials)

# PostgreSQL connection
conn = psycopg2.connect(
host      ='your_postgresql_host',
database  ='your_postgresql_database',
user      ='your_postgresql_user',
password  ='your_postgresql_password'
)
cursor = conn.cursor()

# Google Sheet details
sheet_url = 'https://docs.google.com/spreadsheets/d/your_sheet_id/edit?usp=sharing'
worksheet_name = 'Your_Worksheet_Name'

# Extract sheet key from URL
parsed_url = urlparse(sheet_url)
sheet_key = parsed_url.path.split('/')[3]


# Identify sheet by name
sheet = client.open_by_url(sheet_url)
worksheet = sheet.worksheet(worksheet_name)
sheet_key = worksheet.id

# Fetch data from Google Sheet
data = worksheet.get_all_values()

# Extract column names from the first row of the sheet
column_names = data[0]

# Insert data into PostgreSQL table
for row in data[1:]:
insert_query = f"INSERT INTO your_table_name VALUES ({', '.join(['%s'] * len(row))})"
cursor.execute(insert_query, row)

# Commit changes and close connections
conn.commit()
cursor.close()
conn.close()

print("Data has been successfully imported into PostgreSQL.")
