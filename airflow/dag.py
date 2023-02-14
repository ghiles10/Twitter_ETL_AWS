import sys
sys.path.append('.') # add path to package
from src.IaC import iac


# connection a red shift pour recuperer la date la plus recente 
cursor = iac._conn.cursor()

# grab the latest date from the tweets creation
sql = """SELECT * FROM ghiles.tweets"""

cursor.execute(sql)
# convert the date from datetime to string
fetchedDate = cursor.fetchall()
print('len', len(fetchedDate))

for i in fetchedDate:
    print(i)






