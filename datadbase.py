import os

import mysql.connector

mydb = mysql.connector.connect(
    # host="ab-mum-prod-workattest.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
    # port='3306',
    # user="pvt_app_usr",
    # password="d9f3d2-a2a5-c26dd-49a5d-s33A-840a-d31c3",
    # database="pvt_prod"
    host="ab-mum-prod-workattest.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
    port='3306',
    user="pvt_app_usr",
    password="d9f3d2-a2a5-c26dd-49a5d-s33A-840a-d31c3",
    database="pvt_prod"
)

mycursor = mydb.cursor(buffered=True)
mycursor2 = mydb.cursor(buffered=True)
mycursor3 = mydb.cursor(buffered=True)
mycursor4 = mydb.cursor(buffered=True)
query = """SELECT candidate_id From pvt_prod.infosys_casedownload where status='download_pending';"""


mycursor.execute(query)
data = mycursor.fetchall()
print(data)


condition = True
while condition:
    if data=='download_pending':
        condition = False
        os.system('python infosisportallogin2.py')
        break