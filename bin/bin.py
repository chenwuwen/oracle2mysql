import datetime
from core.mysql.mysql_core import mysql_connection
from core.oracle.oracle_core import oracle_connection, select

start_date = datetime.datetime.now()
mysql_connection()
oracle_connection()
select()
end_date = datetime.datetime.now()

print((end_date - start_date).seconds)
