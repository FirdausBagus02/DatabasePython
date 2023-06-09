import psycopg2
import csv
import os
from datetime import datetime, timedelta

#Postgresql Database Connection
conn = psycopg2.connect(
    host="your-hostname",
    database="your-database",
    user="your-username",
    password="your-password"
)

days_to_keep = 0 #the desired time to save the csv file from the last date 
time_delta = datetime.now() - timedelta(days=days_to_keep)

cur = conn.cursor()
export_query = "SELECT * FROM your_table WHERE date_trunc('day', timestamp) < %s"

cur.execute(export_query, (time_delta.date(),))

results = cur.fetchall()
colnames = [desc[0] for desc in cur.description]

for row in results:
    row_date = row[0].strftime('%Y-%m-%d')
    filename = f'{row_date}.csv'
    folder_name = 'extracted_data'  
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    filename = os.path.join(folder_name, filename)
    with open(filename, 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(row)

conn.commit()
#Delete data that does not belong to the time range 
delete_query = "DELETE FROM your_table WHERE date_trunc('day', timestamp) < %s"
cur.execute(delete_query, (time_delta.date(),))
conn.commit()

cur.close()
conn.close()

print("Data extraction and deletion completed.")
