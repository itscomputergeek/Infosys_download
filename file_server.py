import time
import boto3
import mysql.connector
from uuid import uuid4
import os
import json

import psycopg2
from google.cloud import storage

files=[]
bucketes=[]
urlses=[]
conn = psycopg2.connect(host='localhost', database='postgres', user='postgres', password='1234', port=5432)
print("Connected")
cursor = conn.cursor()
session = boto3.Session(
    region_name='ap-south-1',
    aws_access_key_id='AKIATBUPJGMTA3PJUJUV',
    aws_secret_access_key='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')
s3 = session.resource('s3')

def insert_data(values):
    mydb = mysql.connector.connect(
        host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
        port='3306',
        user="sequelstring_app_usr",
        password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
        database="pvt_prod"
    )
    mycursor = mydb.cursor(buffered=True)
    query = """insert into shared_document values('%s','%s','%s','%s', '%s','%s','%s', '%s','%s','%s')""" % values
    # print(query)
    mycursor.execute(query)
    mydb.commit()

def upload_data(client_id,year,month,uuid,list_of_documents,file_names,candidate_id):
    mydb = mysql.connector.connect(
        host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
        port='3306',
        user="sequelstring_app_usr",
        password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
        database="pvt_prod"
    )
    l = []
    buckets = []
    for file_path in list_of_documents:
        mycursor = mydb.cursor(buffered=True)
        query = """SELECT id FROM pvt_prod.shared_document order by id desc limit 1;"""
        mycursor.execute(query)
        data = mycursor.fetchone()
        key = data[0] + 2
        # print(file_path)
        file_name = (file_path.split('image')[1])[1:]
        # print(file_name)
        bucket_name = 'sequelstring/{}/{}/{}/{}/{}'.format(client_id, year, month, uuid, file_name)
        object1 = s3.Object('ab-mum-prod-fp-bridge', bucket_name)
        result = object1.put(Body=open(file_path, 'rb'))
        res = result.get('ResponseMetadata')
        # print(bucket_name)
        # bucket_name2= json.dumps(bucket_name)
        # print(bucket_name2)
        time.sleep(2)
        buckets.append(bucket_name)
    # buckets=json.dumps(buckets)


        # print(bucket_name2[])
        # print(buckets)
    # print(bucket_name)
    import io
    from io import BytesIO
    import pandas as pd
    import datetime
    storage_client = storage.Client.from_service_account_json(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/sage-sunrise-251211-17a773d51f80.json")
    bucket = storage_client.get_bucket("pvt-sequelstring")
    # print(bucket)
    # k=[]
    # for filess in os.listdir(r"C:\Users\A sylvia\PycharmProjects\KT\KT\extractedzip"):
    #     if '.pdf'in filess:
    #         file_paths = f'C:\\Users\\A sylvia\\PycharmProjects\\KT\\KT\\extractedzip\\%s'%filess
    #         k.append(file_paths)
    #         for file_pathss in k:
    #             file_names = (file_pathss.split('extractedzip')[1])[1:]

    # url_link=[]
    # print(buckets)


    # blob.upload_from_filename('folder.png')
    filename = f"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf/{file_names}/{time.time()}"
    blob = bucket.blob(filename)
    # print(blob)
    import datetime
    blob.upload_from_filename(f'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf/{file_names}')

    url = blob.generate_signed_url(expiration=datetime.timedelta(minutes=10080))
    files.append(file_names)
    print("Generated PUT signed URL:")
    # print(url)
    print("You can use this URL with any user agent, for example:")
    (
        "curl -X PUT -H 'Content-Type: application/octet-stream' "
        "--upload-file my-file '{}'".format(url)
    )
    urlses.append(url)
    # print(docs)
    from datetime import datetime,date
    current_time = datetime.now()
    buckets=json.dumps(buckets)
    # print(buckets)
    bucketes.append(buckets)
    query5 = f"""Update infosys_downloades set gcs_upload='{','.join(files)}',path_s3_upload='{','.join(bucketes)}',upload_url='{','.join(urlses)}' where cid='{candidate_id}';"""
    cursor.execute(query5)
    conn.commit()
    insert_data(('', current_time,uuid, file_names,buckets, f'INFOYS/{uuid}/{file_names}',url,current_time,'application/pdf',client_id))
    if res.get('HTTPStatusCode') == 200:
        status = 'File Uploaded Successfully'
    else:
        status = 'File Not Uploaded'
    l.append(bucket_name)
    return l
# id=uuid4()
# id1=id
# print(id1)
# print(id)
# l = []
# for files in os.listdir(r"C:\Users\A sylvia\PycharmProjects\KT\KT\images"):
#     file_path = f'C:\\Users\\A sylvia\\PycharmProjects\\KT\\KT\\images\\%s'%files
#     l.append(file_path)
# # print(l)
# upload_data(214884, 2022, 2, id1, l)





#print(upload_data(111, 2022,1,234123,[r'C:\Users\shrey\Downloads\Authbridge\Attachmnet\hr_case (2).pdf']))

def search_data():
    my_bucket = s3.Bucket('ab-mum-prod-test-niranjan')
    for b in my_bucket.objects.filter(Prefix='sequelstring/').all():
        print(b.key)
    return "Success"
# print(search_data())
