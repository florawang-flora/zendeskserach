import pymssql
conn = pymssql.connect(
   server='localhost',
    port = 1433,
    user = 'sa',
    password = 'SqlPass!1234',
    database = 'master'
)



cursor = conn.cursor()
cursor.execute("SELECT @@VERSION")
row = cursor.fetchone()
print(row[0])
