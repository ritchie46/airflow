import pyodbc

print(pyodbc.connect('DSN=SQLDNS;UID=<user>;PWD=<password>;database=<db>'))
