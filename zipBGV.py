import json
import re
import time
import zipfile
import os
import shutil
from uuid import uuid4

import pdfplumber
import mysql.connector
from file_server import upload_data

import psycopg2

import smtplib

print("Process started.........")
conn = psycopg2.connect(host='localhost', database='postgres', user='postgres', password='1234', port=5432)
print("Connected")
cursor = conn.cursor()


# id=uuid4()
# id1=id
######convert pdf to image#########
def convert(id1,canddate_id):
    print("Process started.........")
    from pdf2image import convert_from_path
    dpi = 200  # dots per inch
    pdf_file = r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf"
    # print(pdf_file)
    k=[]
    image_count = 0
    from datetime import datetime
    currentMonth = datetime.now().month
    for k in os.listdir(pdf_file):
        if '.pdf' in k:
            print(k.split('.')[0]+'.pdf')
            fname = os.path.splitext(k)[0]
            data2=r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf/%s"%k
            pages = convert_from_path(data2, dpi)
            for i in range(len(pages)):
                # print()
                page = pages[i]
                if len(str(i)) == 1:
                    page.save(f"//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//image//{fname}" + '0'+str(i) + ".jpg", 'JPEG')
                else:
                    page.save(f"//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//image//{fname}" + str(i) + ".jpg", 'JPEG')
            l = []
            for files in os.listdir(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/image/"):
                image_count += 1
                file_path = r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/image/%s'%files
                # print(file_path)
                l.append(file_path)
                l.sort()
                # breakpoint()
            upload_data(214884, 2022, currentMonth, id1, l,k.split('.')[0]+'.pdf',canddate_id)
            print(upload_data)
            query5 = f"""Update infosys_downloades set image_count_s3_upload='{image_count}' where cid='{canddate_id}'"""
            cursor.execute(query5)
            conn.commit()
            time.sleep(5)
            for file_name in os.listdir(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/image"):
                os.remove(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/image//"+file_name)
    if __name__ == "__main__":
        for files in os.listdir(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf"):
            if 'Launchpad_BVF_' in files or 'Launchpad_FRESHERBVF_'in files:
                shutil.copy(f"//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//extractedpdf//{files}",
                            f"//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//pdf_folder//{files}")

        for data in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder/'):
            if '.pdf' in data:
                data2 = (r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder/%s')%data
                BVF_Extract(data2, data, id1)
            else:
                pass
        time.sleep(2)
        for data3 in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder'):
            os.remove(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder//'+data3)


                # print(data2)
                # pdf_path = zip(r'C:\Users\A sylvia\PycharmProjects\KT\KT\New folder')
                # print(pdf_path)
                # with pdfplumber.open(data2) as pdf:
                #     pages = pdf.pages
                #     single_page_text = pages[0].extract_text()
                #     # print(single_page_text)
                #     if 'BACKGROUND VERIFICATION FORM' in single_page_text:
                #         path = os.path.join(r"C:\Users\A sylvia\PycharmProjects\KT\KT\extractedzip\\", data2)
                #         print(path)
                #         with pdfplumber.open(path) as pdf:
                #             pages1 = pdf.pages
                #             single_page_text1 = pages1[1].extract_text()
                #             if 'Education Qualification (Highest Education)- Please attach copy of Degree and Final year mark' in single_page_text1:
                #                 # print(single_page_text1)
                #                 path2 = os.path.join(r'C:\Users\A sylvia\PycharmProjects\KT\KT\extractedzip\\', path)
                #                 BVF_Extract(path2, data,id1)

            # os.remove(r"C:\Users\A sylvia\PycharmProjects\KT\KT\extractedzip\\" + data)

#########image jpeg to pdf#######
def image3():
    import img2pdf
    import glob
    with open(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf/SignoFF.pdf","wb") as f:
        f.write(img2pdf.convert(glob.glob(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf/*.png")))
#########image jpg to pdf#######
def image1():
    import os
    import img2pdf
    import glob
    pdf_file = r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf"
    for k in os.listdir(pdf_file):
        if '.jpg' in k or '.jpeg' in k:
            # print(k)
            with open(f"//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//extractedpdf//{k.split('.')[0] + '.pdf'}","wb") as f:
                f.write(img2pdf.convert(glob.glob(f"//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//extractedpdf//{k}")))
def image2():
    import img2pdf
    import glob
    with open(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf/imagepdf.pdf","wb") as f:
        f.write(img2pdf.convert(glob.glob(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf/*.jpeg")))


###### case documents unresolved########
def default_case_documents(file_names,id):

    from google.cloud import storage
    storage_client = storage.Client.from_service_account_json(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/sage-sunrise-251211-17a773d51f80.json")
    bucket = storage_client.get_bucket("pvt-sequelstring")

    filename = f"//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//extractedpdf//{file_names}//{time.time()}"
    blob = bucket.blob(filename)
    import datetime
    blob.upload_from_filename(f'//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//extractedpdf//{file_names}')

    url = blob.generate_signed_url(expiration=datetime.timedelta(minutes=10080))
    # print(file_names)

    print("Generated PUT signed URL:")
    # print(url)
    print("You can use this URL with any user agent, for example:")
    (
        "curl -X PUT -H 'Content-Type: application/octet-stream' "
        "--upload-file my-file '{}'".format(url)
    )

    for document_name, document_id_bridge in default_docs_to_show.items():
        if 'conso.pdf' in file_names and "Conso PDF" in document_name:
            mydb = mysql.connector.connect(
                host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
                port='3306',
                user="sequelstring_app_usr",
                password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
                database="pvt_prod"
            )
            mycursor = mydb.cursor(buffered=True)
            query1 = f"""select id from pvt_prod.shared_document where pvt_id='{id}' and name='conso.pdf';"""
            mycursor.execute(query1)
            data = mycursor.fetchone()
            # print(data[0])
            return {
                    "id": data[0],
                    "bridge_name": document_name,
                    "actual_name":file_names,#conso.pdf
                    "id_bridge": document_id_bridge,
                    "start_page": None,#none
                    "end_page": None,
                    "gcs_signed_url":url,
                    "document_type": 'application/pdf'}

        elif 'Launchpad_LOA_' in file_names and "Authorized Release Note" in document_name:
            mydb = mysql.connector.connect(
                host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
                port='3306',
                user="sequelstring_app_usr",
                password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
                database="pvt_prod"
            )
            mycursor = mydb.cursor(buffered=True)
            query2 = f"""select id from pvt_prod.shared_document where pvt_id='{id}' and name Like 'Launchpad_LOA_%';"""
            mycursor.execute(query2)
            data = mycursor.fetchone()
            # print(data[0])
            return {
                    "id": data[0],
                    "bridge_name": document_name,
                    "actual_name": file_names,  # conso.pdf
                    "id_bridge": document_id_bridge,
                    "start_page": None,  # noneC:\Users\A sylvia\PycharmProjects\KT\KT\extractedzip
                    "end_page": None,
                    "gcs_signed_url": url,
                    "document_type": 'application/pdf'}

        elif 'Launchpad_BVF_' in file_names or 'Launchpad_FRESHERBVF_' in file_names and "Application Form PDF" in document_name:
            mydb = mysql.connector.connect(
                host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
                port='3306',
                user="sequelstring_app_usr",
                password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
                database="pvt_prod"
            )
            mycursor = mydb.cursor(buffered=True)
            if 'Launchpad_BVF_' in file_names:
                query1 = f"""select id from pvt_prod.shared_document where pvt_id='{id}' and name Like 'Launchpad_BVF_%';"""
            elif 'Launchpad_FRESHERBVF_' in file_names:
                query1 = f"""select id from pvt_prod.shared_document where pvt_id='{id}' and name Like 'Launchpad_FRESHERBVF_%';"""
            mycursor.execute(query1)
            data = mycursor.fetchone()
            # print(data[0])
            return {
                    "id": data[0],
                    "bridge_name": "Application Form PDF",
                    "actual_name":file_names,#conso.pdf
                    "id_bridge": 32,
                    "start_page": None,#none
                    "end_page": None,
                    "gcs_signed_url":url,
                    "document_type": 'application/pdf'}
        elif 'SignoFF' in file_names and "Sign-off" in document_name:
            mydb = mysql.connector.connect(
                host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
                port='3306',
                user="sequelstring_app_usr",
                password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
                database="pvt_prod"
            )
            mycursor = mydb.cursor(buffered=True)
            query1 = f"""select id from pvt_prod.shared_document where pvt_id='{id}' and name='SignoFF.pdf';"""
            mycursor.execute(query1)
            data = mycursor.fetchone()
            # print(data[0])
            return {
                "id": data[0],
                "bridge_name": document_name,
                "actual_name": file_names,  # conso.pdf
                "id_bridge": document_id_bridge,
                "start_page": None,  # none
                "end_page": None,
                "gcs_signed_url": url,
                "document_type":'application/pdf'}

        else:
            pass

default_docs_to_show = {
    "Conso PDF": 27,
    "Sign-off": 91,
    "Application Form PDF": 32,
    "Authorized Release Note": 26,
}

"""
self.con_val = {"1001523008_4134477_Graduation_MS_0242_31":"https://"}
"""
########consolated pdf##############
def consopdf():
    import os
    from PyPDF2 import PdfFileMerger
    merger = PdfFileMerger(strict=False)

    pdfs = os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf')

    # iterate among the documents
    pdf1 = []
    for pdf in pdfs:
        if pdf.endswith('.pdf'):
            path_with_file = os.path.join(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf', pdf)
            # print(path_with_file)


            merger.append(path_with_file, import_bookmarks=False)

    merger.write(r'//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//extractedpdf//conso.pdf')
    merger.close()

######## return package id#############
def package_id():
    import json
    import requests
    url = "http://35.244.24.8:9000/api/infoys/external/package-details/"
    payload = json.dumps({
      "package_name": "Fresher"
    })
    headers = {
      'Authorization': 'AuthBridge eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJQVlQgTWljb3JzZXJ2aWNlIiwibmFtZSI6IlBhY2thZ2UgRGV0YWlscyBBUEkiLCJpYXQiOiJBbGwgQ2xpZW50cyJ9.trURv_dLOXKfytKlrThWwnlRTfHAz9TytS3F8PCoCrY',
      'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    # print(response.text)
    json_obj = json.loads(response.text.encode('utf8'))
    # print(json_obj)
####zip to unzip##########
def unzip(path_to_zip_file):
    directory_to_extract_to = r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf'
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)

    for item in os.listdir(directory_to_extract_to):
        if item.endswith('.pdf') and 'BVF' in item:
            pdf_path = os.path.join(directory_to_extract_to, item)

    # return pdf_path

####EXRAXt to education_details##########
def education_details(idnt_str):
    from pdf2docx import Converter
    import os

    path_input = r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder//'
    path_output = r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/tex//'
    os.chdir(path_input)
    for file in os.listdir(path_input):
        cv = Converter(path_input + file)
        cv.convert(path_output + file + '.docx', start=0, end=None)
        cv.close()
        # print(file)

    # save to csv
    from docx import Document
    import pandas as pd
    document = Document(f"//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//tex//{file}.docx")

    tables = []
    for table in document.tables:
        df = [['' for i in range(len(table.columns))] for j in range(len(table.rows))]
        for i, row in enumerate(table.rows):
            for j, cell in enumerate(row.cells):
                if cell.text:
                    df[i][j] = cell.text
        tables.append(pd.DataFrame(df))

    for nr, i in enumerate(tables):
        i.to_csv("table_" + str(nr) + ".csv")
    document.save(f"//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//tex//{file}.docx")

    # # to save all csv in one

    import os
    import csv
    path = r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder//'

    edu_list = []
    for item in os.listdir(path):
        if item.endswith('.csv'):
            final_path = os.path.join(path, item)
            with open(final_path, mode='r', encoding='utf-8') as file:
                # reading the CSV file
                csvFile = csv.reader(file)
                #print(csvFile)
                # displaying the contents of the CSV file
                flag = False
                temp_list = []
                if idnt_str == 'fresher':
                    for lines in csvFile:
                        # print(lines)
                        temp_list.append(lines)
                        if "Please account for any and all gaps of more than 3 months between last education and first employment" in lines[1]:
                            flag = True
                            print(flag)
                            break
                    # print(temp_list)

                    if flag == True:
                        for i in range(14, len(temp_list), 5):
                            import datetime
                            if 'Please account for any and all gaps of more than 3 months between last education and ' == temp_list[i]:
                                break
                            temp = []
                            #print(temp_list[i], 'ppppppppppppppppppppppppppppp')
                            College_name_address = temp_list[i][1].capitalize()
                            temp.append(College_name_address)
                            University_name_address = temp_list[i][2]
                            temp.append(University_name_address)
                            Date_from = temp_list[i][6]
                            # Date_from = datetime.datetime.strptime(Date_from, '%d-%b-%y ').strftime('%Y')
                            temp.append(Date_from)
                            Date_to = temp_list[i][7]
                            # Date_to = datetime.datetime.strptime(Date_to, '%d-%b-%y ').strftime('%Y')
                            temp.append(Date_to)
                            Qualification = temp_list[i][8]
                            # print(Qualification)
                            temp.append(Qualification)
                            Roll_no = temp_list[i][-1]
                            temp.append(Roll_no)
                            Time_status = temp_list[i + 2][-1]
                            temp.append(Time_status)
                            #print(temp)
                            edu_list.append(temp)
                else:
                    for lines in csvFile:
                        #print(lines)
                        temp_list.append(lines)
                        if "Graduation details" in lines[1]:
                            flag = True
                            print(flag)
                            break
                    # print(temp_list)
                    if flag == True:
                        for i in range(2, len(temp_list), 4):
                            import datetime
                            if 'Graduation details' == temp_list[i][1]:
                                break
                            temp = []
                            #print(temp_list[i], 'ppppppppppppppppppppppppppppp')
                            College_name_address = temp_list[i][1]
                            temp.append(College_name_address)
                            University_name_address = temp_list[i][2]
                            temp.append(University_name_address)
                            Date_from = temp_list[i][3]
                            # Date_from=datetime.datetime.strptime(Date_from, '%d-%b-%y ').strftime('%Y')
                            temp.append(Date_from)
                            Date_to = temp_list[i][4]
                            # Date_to=datetime.datetime.strptime(Date_to, '%d-%b-%y ').strftime('%Y')
                            temp.append(Date_to)
                            Qualification = temp_list[i][5]
                            temp.append(Qualification)
                            Roll_no = temp_list[i][6]
                            temp.append(Roll_no)
                            Time_status = temp_list[i+2][-1]
                            temp.append(Time_status)
                            #print(temp)
                            edu_list.append(temp)
    # print(edu_list)
    print('ppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp')
    return edu_list
####EXRAXt to address_details##########
def address_details(idnt_str):

    import os
    import csv

    path = r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder//'

    address_details = []
    address_detailses=[]
    for item in os.listdir(path):
        if item.endswith('.csv') and 'table_6' in item:
            final_path = os.path.join(path, item)
            with open(final_path, mode='r', encoding='utf-8') as file:
                # reading the CSV file
                csvFile = csv.reader(file)
                # print(csvFile)
                # displaying the contents of the CSV file
                flag = False
                temp_list = []
                if idnt_str == 'fresher':
                    for lines in csvFile:
                        # print(lines)
                        temp_list.append(lines)
                    flag = True
                # print(temp_list)
                if flag == True:
                    for i in range(6, len(temp_list), 4):
                        temp = []
                        try:
                            import datetime
                            Pos_from = temp_list[i][1].strip()
                            if re.search('\d+\-\w{3}\-\d+', Pos_from.strip()):
                                Pos_from = datetime.datetime.strptime(Pos_from, '%d-%b-%y').strftime('%B, %Y')
                            else:
                                Pos_from = Pos_from
                            temp.append(Pos_from)
                            Pos_to = temp_list[i][3].strip()
                            if re.search('\d+\-\w{3}\-\d+', Pos_to.strip()):
                                Pos_to = datetime.datetime.strptime(Pos_to, '%d-%b-%y').strftime('%B, %Y')
                            else:
                                Pos_to = Pos_to
                            temp.append(Pos_to)
                            Address = temp_list[i][4]
                            temp.append(Address)
                            state = temp_list[i][5]
                            temp.append(state)
                            country = temp_list[i][6]
                            temp.append(country)
                            pin_code = temp_list[i][7]
                            temp.append(pin_code)
                            contact_no = temp_list[i][-1]
                            temp.append(contact_no)
                            address_details.append(temp)
                            address_detailss = {"Period of Stay":Pos_from + ' '+'To'+' '+ Pos_to,"POS_form": Pos_from, "POS_to": Pos_to, "Address": Address.capitalize().title() +','+ state +'-' + pin_code,"state": state,"country": country, "pin_code": pin_code, "contact_no": contact_no}
                            address_detailses.append(address_detailss)

                        except:
                           pass
        elif item.endswith('.csv') and 'table_7' in item:
            final_path = os.path.join(path, item)
            with open(final_path, mode='r', encoding='utf-8') as file:
                # reading the CSV file
                csvFile = csv.reader(file)

                flag = False
                temp_list = []
                if idnt_str == 'fresher':
                    for lines in csvFile:
                        # print(lines)
                        temp_list.append(lines)
                    flag = True
                # print(temp_list)
                if flag == True:
                    for i in range(6, len(temp_list), 4):
                        temp = []
                        try:
                            import datetime
                            Pos_from = temp_list[i][1].strip()
                            if re.search('\d+\-\w{3}\-\d+', Pos_from.strip()):
                                Pos_from = datetime.datetime.strptime(Pos_from, '%d-%b-%y').strftime('%B, %Y')
                            else:
                                Pos_from = Pos_from
                            temp.append(Pos_from)
                            Pos_to = temp_list[i][3].strip()
                            if re.search('\d+\-\w{3}\-\d+', Pos_to.strip()):
                                Pos_to = datetime.datetime.strptime(Pos_to, '%d-%b-%y').strftime('%B, %Y')
                            else:
                                Pos_to = Pos_to
                            temp.append(Pos_to)
                            Address = temp_list[i][4]
                            temp.append(Address)
                            state = temp_list[i][5]
                            temp.append(state)
                            country = temp_list[i][6]
                            temp.append(country)
                            pin_code = temp_list[i][7]
                            temp.append(pin_code)
                            contact_no = temp_list[i][-1]
                            temp.append(contact_no)
                            address_details.append(temp)
                            address_detailss = {"Period of Stay":Pos_from + ' '+'To'+' '+ Pos_to,"POS_form": Pos_from, "POS_to": Pos_to, "Address": Address.capitalize().title() +','+ state +'-' + pin_code,"state": state,"country": country, "pin_code": pin_code, "contact_no": contact_no}
                            address_detailses.append(address_detailss)

                        except:
                           pass
    return address_details,address_detailses

def emplyoment_details(idnt_str):
    import os
    import csv

    path = r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder//"
    temp_list = []
    emp_details = []
    emp_details_ = []
    if idnt_str == 'fresher':
        i = 2
        for item in os.listdir(path):
            if item.endswith('.csv') and f'table_{i}' in item:
                final_path = os.path.join(path, item)
                i+=1
                with open(final_path, mode='r', encoding='utf-8') as file:
                    # reading the CSV file
                    csvFile = csv.reader(file)
                    # print(csvFile)
                    # displaying the contents of the CSV file
                    flag = False
                    for lines in csvFile:
                        if 'Complete Address and Location' in lines[1]:
                            flag = True
                            break
                        #print(lines)
                        temp_list.append(lines)
                    if flag == True:
                        break
        for i in range(len(temp_list)):
            temp = []
            if 'Note: Ensure that you are descriptive' in temp_list[i][1]:
                name_of_employer = temp_list[i+3][1]
                temp.append(name_of_employer)
                address_of_employer = temp_list[i+3][2]
                temp.append(address_of_employer)
                designation = temp_list[i+6][-2].replace('Designation: \n', '')
                temp.append(designation)
                telephone = temp_list[i + 6][1].replace('Telephone No: ', '').replace('\n', '')
                temp.append(telephone)
                # import datetime
                from_ = temp_list[i+8][1].replace('From: \n', '')
                # from_ = datetime.datetime.strptime(from_, '%d-%b-%y').strftime('%B, %d %Y')
                # print(from_)
                temp.append(from_)
                to_ = temp_list[i+8][2].replace('To: \n', '')
                # to_ = datetime.datetime.strptime(to_, '%d-%b-%y').strftime('%B, %d %Y')
                temp.append(to_)
                # print(to_)
                reason_for_leaving = temp_list[i+9][-1].replace('Reasons for leaving:', '')
                temp.append(reason_for_leaving)
                emp_details.append(temp)

        # print(temp_list)
        #print(emp_details)
        for item in emp_details:
            if item[0] == '0' or 'From' in item[0] or 'no' == item[0]:
                break
            else:
                emp_details_.append(item)
        # print(emp_details_)
    else:
        i = 7
        for item in os.listdir(path):
            if item.endswith('.csv') and f'table_{i}' in item:
                final_path = os.path.join(path, item)
                i += 1
                with open(final_path, mode='r', encoding='utf-8') as file:
                    # reading the CSV file
                    csvFile = csv.reader(file)
                    # print(csvFile)
                    # displaying the contents of the CSV file
                    flag = False
                    for lines in csvFile:
                        if 'Complete Address and Location' in lines[1]:
                            flag = True
                            break
                        #print(lines)
                        temp_list.append(lines)
                    if flag == True:
                        break
        # print(temp_list)
        print('temppppppppppppppppppppppppppppp')
        for i in range(len(temp_list)):
            temp = []
            if 'Note: Ensure that you are descriptive' in temp_list[i][1] or 'Quest Global' in temp_list[i][1]:
                name_of_employer = temp_list[i][1].split('\n')[-1]
                temp.append(name_of_employer)
                address_of_employer = temp_list[i][-1].split('\n')[-1]
                temp.append(address_of_employer)
                designation = temp_list[i+1][-2].replace('Designation: \n', '')
                temp.append(designation)
                telephone = temp_list[i+1][1].replace('Telephone No: ', '').replace('\n', '')
                temp.append(telephone)
                employe_code = temp_list[i+1][2].replace('Employee \nCode/No: \n', '')
                temp.append(employe_code)
                from_ = temp_list[i+3][1].replace('From: \n', '')
                # from_ =datetime.datetime.strptime(from_, '%d-%b-%y').strftime('%B, %d %Y')
                print(from_)
                temp.append(from_)
                to_ = temp_list[i+3][2].replace('To: \n', '')
                # to_=datetime.datetime.strptime(to_, '%d-%b-%y').strftime('%B, %d %Y')
                temp.append(to_)
                reason_for_leaving = temp_list[i+4][-1].replace('Reasons for leaving: \n', '')
                temp.append(reason_for_leaving)
                emp_details.append(temp)
        # print(emp_details)
        for item in emp_details:
            print(item[0])
            if item[0] == '0' or 'Note: Ensure that you' in item[0] or 'no' == item[0]:
                break
            else:
                emp_details_.append(item)

    return emp_details_



def BVF_Extract(filepath, file_name,id):
    import datetime


    with pdfplumber.open(filepath) as pdf:
        pages = pdf.pages
        first_page = pages[0].extract_tables()
        first_page_ = pages[0].extract_text()

        if r'Fresher’s' in first_page_:
            second_page = pages[1].extract_tables()
            second_page_ = pages[1].extract_text()
            # print(second_page)
            # print(second_page_)
            Date_Of_Join = second_page[0][2][-10]
            Candidate_Id = second_page[0][3][3]
            Uan_No = None
            full_name = second_page[0][7][0]
            first, *middle, last = full_name.split()
            first_name = "{first}".format(first=first)
            print(first_name)

            middle_name = "{middle}".format(middle=" ".join(middle))
            print(middle_name)
            last_name = "{last}".format(last=last)
            print(last_name)
            father_name = second_page[0][9][0]
            Gender = second_page[0][9][-5]
            Date_Of_Birth = second_page[0][9][-10]
            Date_Of_Birth=datetime.datetime.strptime(Date_Of_Birth, '%d-%b-%y').strftime('%d-%m-%y')
            Date_Of_Birth = datetime.datetime.strptime(Date_Of_Birth, '%d-%m-%y').strftime('%Y-%m-%d')

            Nationality = second_page[0][9][7]
            edu_details = education_details('fresher')
            edu_details_ = []
            if edu_details != []:

                for item in edu_details:
                    if re.search('\d+\-\w{3}\-\d+', edu_details[1][3].strip()):
                        Date_em1 = datetime.datetime.strptime(edu_details[1][3].strip(), '%d-%b-%y').strftime('%Y')
                    elif re.search('\w{3}\-\d+', edu_details[1][3].strip()):
                        Date_em1 = datetime.datetime.strptime(edu_details[1][3].strip(), '%b-%y').strftime('%Y')
                    else:
                        Date_em1= edu_details[1][3].strip()

                    temp_data = {'College_Institute_University, Location': item[0].title()+ ' ' + '/' + ' ' + item[1].capitalize().title(), 'Roll No_Registration No_Enrollment No.': item[5].replace(r'\n','').lstrip(r"\n") ,
                                 'Year of Passing':Date_em1, 'Period of the course (Date From : To) ': item[2].replace(r'\n','').lstrip(r"\n")  + '-' + item[3].replace(r'\n','').lstrip(r"\n") , 'Qualification': item[4].replace(r'\n','').lstrip(r"\n"),
                                 'Mode of Qualification': item[-1].replace(r'\n','').lstrip(r"\n")}
                    temp_data={x: v.replace('\n', '') for x, v in temp_data.items()}
                    # print(temp_data)
                    edu_details_.append(temp_data)
            print(edu_details_)
            Address_details, Address_detailses = address_details('fresher')


            if Address_detailses is not None:
                if len(Address_detailses)>0:
                    for i in Address_detailses:
                        if 'Period of Stay' in i:
                            if 'Period of Stay' in Address_detailses[0]:
                                Address_detailses[0] = {x: v.replace('\n', '') for x, v in Address_detailses[0].items()}
                                Addressa_detailses = Address_detailses[0]
                                # print(Addressa_detailses)

                            else:
                                Address_detailses='{}'
                        # breakpoint()
                            if len(Address_detailses) > 0:
                                if 'Period of Stay' in Address_detailses[1]:
                                    Address_detailses[1] = {x: v.replace('\n', '') for x, v in Address_detailses[1].items()}
                                    Addressa_detailses1 = Address_detailses[1]
                                else:
                                    Addressa_detailses1='{}'
                            else:
                                Addressa_detailses1 = '{}'

                            if len(Address_detailses) >1:
                                if 'Period of Stay' in Address_detailses[2]:
                                    Address_detailses[2] = {x: v.replace('\n', '') for x, v in Address_detailses[2].items()}
                                    Addressa_detailses2 = Address_detailses[2]
                                else:
                                    Addressa_detailses2='{}'
                            else:
                                Addressa_detailses2 = '{}'


                        else:
                            Address_detailses=[{},{},{}]
                            Addressa_detailses='{}'
                            Addressa_detailses1='{}'
                            Addressa_detailses2 = '{}'
                else:
                    Address_detailses = [{}, {}, {}]
                    Addressa_detailses = '{}'
                    Addressa_detailses1 = '{}'
                    Addressa_detailses2='{}'
            try:
                current_address = Address_details[0]
                permanent_address = Address_details[1]
                previous_address = Address_details[-1]
                if previous_address[0] == '':
                    previous_address = Address_details[-2]
            except:
                current_address = []
                permanent_address = []
                previous_address = []
            if current_address != [] and permanent_address != [] and previous_address != []:
                current_add = current_address[2] + ' ' + current_address[-2]
                current_add = current_add.replace('\n', '')
                current_pos = current_address[0] + ' ' + current_address[1]
                current_scope = 'Current'
                permanent_add = permanent_address[2] + ' ' + permanent_address[-2]
                permanent_add = permanent_add.replace('\n', '')
                permanent_pos = permanent_address[0] + ' ' + permanent_address[1]
                permanent_scope = 'Permanent'
                previous_add = previous_address[2] + ' ' + previous_address[-2]
                previous_add = previous_add.replace('\n', '')
                previous_pos = previous_address[0] + ' ' + previous_address[1]
                previous_scope = 'Previous'
                Phone_no = current_address[-1]
                country = current_address[-3]
            else:
                current_add = 'N/A'
                current_pos = 'N/A'
                current_scope = 'N/A'
                permanent_add = 'N/A'
                permanent_pos = 'N/A'
                permanent_scope = 'N/A'
                previous_add = 'N/A'
                previous_pos = 'N/A'
                previous_scope = 'N/A'
                country = 'N/A'
                Phone_no = None
            print(country)
            if country != 'India ' and country != 'N/A':
                criminal_add = current_address[2] + ' ' + current_address[-2]
                criminal_add = criminal_add.replace('\n', '')
                criminal_pos = current_address[0] + ' ' + current_address[1]
                criminal_scope = 'Criminal'
                Indian_add = current_address[2] + ' ' + current_address[-2]
                Indian_add = Indian_add.replace('\n', '')
                Indian_pos = current_address[0] + ' ' + current_address[1]
                Indian_scope = 'India'
            else:
                criminal_add = 'N/A'
                criminal_pos = 'N/A'
                criminal_scope = 'N/A'
                Indian_add = 'N/A'
                Indian_pos = 'N/A'
                Indian_scope = 'N/A'

            current_addresses={"current_address":current_address}
            permanent_addresss={"resident_address":permanent_address}

            edu_details_[0] = {x: v.replace(f'{Date_em1}', '') for x, v in edu_details_[0].items()}
            data_jaf = {"Previous Employment History": [{}], "Education_details": [edu_details_[0],edu_details_[1]],"others":[Addressa_detailses,Addressa_detailses1,Addressa_detailses2], "Address_details": {"current_address": Addressa_detailses,"residence_address": Addressa_detailses1}}
            data_jaf = json.dumps(data_jaf)
            # print(data_jaf)
            # breakpoint()
            mydb = mysql.connector.connect(
                host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
                port='3306',
                user="sequelstring_app_usr",
                password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
                database="pvt_prod"
            )
            flag=True
            mycursor = mydb.cursor(buffered=True)
            mycursor2 = mydb.cursor(buffered=True)
            mycursor3 = mydb.cursor(buffered=True)
            mycursor4 = mydb.cursor(buffered=True)
            mycursor5=mydb.cursor(buffered=True)
            query = f"""SELECT candidate_id From pvt_prod.infosys_casedownload where candidate_id='{Candidate_Id}';"""
            query1 = f"""SELECT client_reference_no From pvt_prod.infosys_casedownload where candidate_id='{Candidate_Id}';"""
            query2 = f"""SELECT project_name From pvt_prod.infosys_casedownload where candidate_id='{Candidate_Id}';"""
            query3 = f"""SELECT po_no From pvt_prod.infosys_casedownload where candidate_id='{Candidate_Id}';"""
            mycursor.execute(query1)
            data2 = mycursor.fetchone()
            mycursor2.execute(query2)
            data4 = mycursor2.fetchone()
            mycursor3.execute(query3)
            data3 = mycursor3.fetchone()
            flag=True
            query5 = f"""Update pvt_prod.infosys_casedownload SET status='download_in_progress',download_complete='1' where candidate_id='{Candidate_Id}';"""
            mycursor4.execute(query5)
            mydb.commit()
            # print(data[0], data2[0], data4[0], data3[0])
            Client_Reference_No = data2[0]
            Candidate_Id = Candidate_Id
            Project_Name = data4[0]
            PO_No = data3[0]

            # conn.commit()
            print("record inserting")
            # package_id8 = package_id()
            l = []
            for files in os.listdir(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf"):
                if 'conso' in files or 'Launchpad_LOA_' in files or 'Launchpad_BVF_' in files or 'Launchpad_FRESHERBVF_' in files or 'SignoFF' in files:
                    case_document = default_case_documents(files, id)
                    l.append(case_document)
            case_document = json.dumps(l, default=str)
            # case_document=str(case_document)
            alternate_no = ''
            alternate_no = (list(alternate_no))
            alternate_no = str(alternate_no)
            print(alternate_no)
            alternate_no = ''.join(alternate_no.replace(",", '').replace("'", "").replace(' ', ''))

            mydb = mysql.connector.connect(
                host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
                port='3306',
                user="sequelstring_app_usr",
                password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
                database="pvt_prod"
            )
            mycursor = mydb.cursor(buffered=True)

            from datetime import datetime, date
            # print(id)

            timer = int('7200')
            query = 'Select * from infosys_case;'
            mycursor.execute(query)
            data = mycursor.fetchone()
            # print(data)
            num_fields = len(mycursor.description)
            # field_names = [i[0] for i in mycursor.description]
            today = date.today()
            case = {}
            case = str(case)
            now = datetime.now()
            time = now.strftime("%H:%M:%S")

            current_time = datetime.now()
            u = 'unassigned'

            qu = """insert into infosys_case values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            values = (current_time, current_time, str(id).replace("-", ""), u, time,data_jaf, case, '', '', None, None, None, '','', '', timer, None, case_document, Client_Reference_No,Candidate_Id, Project_Name, PO_No, full_name,first_name, middle_name, last_name, Phone_no, Date_Of_Birth, '', father_name, alternate_no, 'Bangalore', today,Gender)
            print(values)
            # breakpoint()
            mycursor.execute(qu, values)
            mydb.commit()
            print('record')
            query5 = f"""Update infosys_downloades set extraxted_data='{str(data_jaf)}',final_status='done',remarks='Process Succed',endtime='{str(current_time)}' where cid='{Candidate_Id}'"""
            cursor.execute(query5)
            conn.commit()
        else:
            second_page = pages[1].extract_tables()
            second_page_ = pages[1].extract_text()

            Date_Of_Join = first_page[1][2][-4]
            Candidate_Id = first_page[1][3][3]
            Uan_No = first_page[1][5][3]
            full_name = first_page[1][9][0]

            first, *middle, last = full_name.split()
            first_name = "{first}".format(first=first)
            print(first_name)

            middle_name = "{middle}".format(middle=" ".join(middle))
            print(middle_name)
            last_name = "{last}".format(last=last)
            print(last_name)
            father_name = first_page[1][11][0]
            Gender = first_page[1][11][-2]
            Date_Of_Birth = first_page[1][11][-4]
            Date_Of_Birth=datetime.datetime.strptime(Date_Of_Birth, '%d-%b-%y').strftime('%d-%m-%y')
            Date_Of_Birth = datetime.datetime.strptime(Date_Of_Birth, '%d-%m-%y').strftime('%Y-%m-%d')
            Nationality = first_page[1][11][-6]
            edu_details = education_details('')
            emp_details = emplyoment_details('')
            print(edu_details)
            print(emp_details)

            current_address = []
            permanent_address = []
            Address_details = []

            phone_no = 'N/A'
            current_add = 'N/A'
            current_pos = 'N/A'
            current_scope = 'N/A'
            permanent_add = 'N/A'
            permanent_pos = 'N/A'
            permanent_scope = 'N/A'
            previous_add = 'N/A'
            previous_pos = 'N/A'
            previous_scope = 'N/A'
            criminal_add = 'N/A'
            criminal_pos = 'N/A'
            criminal_scope = 'N/A'
            Indian_add = 'N/A'
            Indian_pos = 'N/A'
            Indian_scope = 'N/A'
            Phone_no = None
            edu_details_ = []
            if edu_details != []:
                import datetime

                for item in edu_details:
                    import datetime
                    if re.search('\d+\-\w{3}\-\d+', edu_details[1][3].strip()):
                        Date_em1 = datetime.datetime.strptime(edu_details[1][3].strip(), '%d-%b-%y').strftime('%Y')
                    else:
                        Date_em1 = datetime.datetime.strptime(edu_details[1][3].strip(), '%b-%y').strftime('%Y')

                    temp_data = {'College_Institute_University, Location':item[0].title()+ ' '+'/' +' '+ item[1].title(),
                                 'Roll No_Registration No_Enrollment No.': item[5].replace(r'\n','').lstrip(r"\n"),
                                 'Year of Passing':Date_em1,'Period of the course (Date From : To) ': item[2].replace(r'\n','').lstrip(r"\n")  + '-' + item[3].replace(r'\n','').lstrip(r"\n") ,
                                 'Qualification': item[4].replace(r'\n','').replace(r'\n','').lstrip(r"\n"),
                                 'Mode of Qualification': item[-1].replace(r'\n','').lstrip(r"\n")}
                    temp_data = {x: v.replace('\n', '') for x, v in temp_data.items()}
                    edu_details_.append(temp_data)
            print(edu_details_)
            # edu_details_ = json.dumps(edu_details_)
            emp_details_ = []
            emp_details_cu=[]
            emp_details_pri=[]
            if emp_details != []:
                for item in emp_details:
                    import datetime
                    from datetime import date
                    if re.search('\d+\-\w{3}\-\d+', item[5].strip()):
                        Date_em1 = datetime.datetime.strptime(item[5].strip(), '%d-%b-%y').strftime('%B %d, %Y')
                        Date_em2 = datetime.datetime.strptime(item[6].strip(), '%d-%b-%y').strftime('%B %d, %Y')
                    else:
                        Date_em1 = datetime.datetime.strptime(item[5].strip(), '%b-%y').strftime('%B, %Y')
                        Date_em2 = datetime.datetime.strptime(item[6].strip(), '%b-%y').strftime('%B, %Y')
                    temp_data = {'Period of Employement': Date_em1 +' ' + 'To' + ' '+ Date_em2, 'Employer Name':item[0].replace(r'Employee Code/No:',''),
                                 'Designation': item[2].replace(r'\n',''), 'Employee Code': item[4].replace(r'Employee Code/No:',''),
                                 'Date of Joining': item[5].replace(r'\n',''), 'Date of Leaving': item[6].replace(r'\n',''), 'UAN': Uan_No,
                                 'Reason for leaving': item[-1].replace(r'\n','')}
                    if re.search('\d+\-\w{3}\-\d+', item[5].strip()):
                        persent_date = datetime.datetime.strptime(f'{date.today()}', '%Y-%m-%d').strftime('%Y-%m-%d')
                        date_em1 = datetime.datetime.strptime(item[6].strip(), '%d-%b-%y').strftime('%Y-%m-%d')
                    else:
                        date_em1 = datetime.datetime.strptime(item[6].strip(), '%b-%y').strftime('%Y-%m')
                        persent_date = datetime.datetime.strptime(f'{date.today()}', '%Y-%m-%d').strftime('%Y-%m')
                    if date_em1 >= persent_date:
                        current_emp = temp_data
                        print(current_emp)
                        pri = '{}'
                        emp_details_cu.append(current_emp)
                        emp_details_pri.append(pri)
                    else:
                        pri = temp_data
                        print(pri)
                        print('yes')
                        current_emp = '{}'
                        emp_details_cu.append(current_emp)
                        emp_details_pri.append(pri)

                    # print(emp_details_)
            current_addresses = {"current_address": current_address}
            permanent_addresss = {"resident_address": permanent_address}
            print(Date_Of_Join, Candidate_Id, Uan_No, full_name, father_name, Gender, Date_Of_Birth,Nationality)
            data_jaf = {"Current Employment History": emp_details_cu,"Previous Employment History": emp_details_pri, "Education_details": [edu_details_[1],edu_details_[1]],"others": [], "Address_details": {"current_address": {}, "residence_address": {}}}
            data_jaf = json.dumps(data_jaf)
            print(data_jaf)
            # breakpoint()
            mydb = mysql.connector.connect(
                    host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
                    port='3306',
                    user="sequelstring_app_usr",
                    password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
                    database="pvt_prod"
            )
            flag = True
            mycursor = mydb.cursor(buffered=True)
            mycursor2 = mydb.cursor(buffered=True)
            mycursor3 = mydb.cursor(buffered=True)
            mycursor4 = mydb.cursor(buffered=True)
            mycursor5 = mydb.cursor(buffered=True)
            query = f"""SELECT candidate_id From pvt_prod.infosys_casedownload where candidate_id='{Candidate_Id}';"""
            query1 = f"""SELECT client_reference_no From pvt_prod.infosys_casedownload where candidate_id='{Candidate_Id}';"""
            query2 = f"""SELECT project_name From pvt_prod.infosys_casedownload where candidate_id='{Candidate_Id}';"""
            query3 = f"""SELECT po_no From pvt_prod.infosys_casedownload where candidate_id='{Candidate_Id}';"""
            mycursor.execute(query1)
            data2 = mycursor.fetchone()
            mycursor2.execute(query2)
            data4 = mycursor2.fetchone()
            mycursor3.execute(query3)
            data3 = mycursor3.fetchone()
            flag = True
            query5 = f"""Update pvt_prod.infosys_casedownload SET status='download_progress',download_complete='1' where candidate_id='{Candidate_Id}';"""
            mycursor4.execute(query5)
            mydb.commit()
            # print(data[0], data2[0], data4[0], data3[0])
            Client_Reference_No = data2[0]
            Candidate_Id = Candidate_Id
            Project_Name = data4[0]
            PO_No = data3[0]

            l = []
            for files in os.listdir(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf"):
                if 'conso' in files or 'Launchpad_LOA_' in files or 'Launchpad_BVF_' in files or 'Launchpad_FRESHERBVF_'in files or 'SignoFF' in files:
                    case_document = default_case_documents(files,'8f4187f9-49c2-4116-87a7-a78145e99103')
                    l.append(case_document)
            case_document = json.dumps(l,default=str)
            print(case_document)
            alternate_no=''
            alternate_no = (list(alternate_no))
            alternate_no = str(alternate_no)
            print(alternate_no)
            alternate_no = ''.join(alternate_no.replace(",", '').replace("'", "").replace(' ', ''))
            mydb = mysql.connector.connect(
                host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
                port='3306',
                user="sequelstring_app_usr",
                password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
                database="pvt_prod"
            )
            mycursor = mydb.cursor(buffered=True)
            # mycursor.execute("Show tables;")
            # myresult = mycursor.fetchall()
            # for x in myresult:
            #     print(x)
            from datetime import datetime, date
            # print(id)
            timer = int('7200')
            query = 'Select * from infosys_case;'
            mycursor.execute(query)
            data = mycursor.fetchone()
            # print(data)
            num_fields = len(mycursor.description)
            # field_names = [i[0] for i in mycursor.description]
            today = date.today()

            case = {}
            case=str(case)
            now = datetime.now()
            time = now.strftime("%H:%M:%S")
            package_id=''
            current_time = datetime.now()
            u='unassigned'
            qu = """insert into infosys_case values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            values = (current_time,current_time,str(id).replace("-", ""),u,time,data_jaf,case,'',package_id,None,None,None,'','','',timer,None,case_document,Client_Reference_No,Candidate_Id,Project_Name,PO_No,full_name,first_name,middle_name,last_name,Phone_no,Date_Of_Birth,'',father_name,alternate_no,'Bangalore',today,Gender)
            print(values)
            mycursor.execute(qu, values)
            mydb.commit()
            print('record')

            query5 = f"""Update infosys_downloades set extraxted_data='{str(data_jaf)}',final_status='done',remarks='Process Succed',endtime='{str(current_time)}' where cid='{Candidate_Id}'"""
            cursor.execute(query5)
            conn.commit()
            print('recorded')
def email_send(email_data, to_addr):
    from_addr = 'accenture.bot@authbridge.com'
    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    smtp.starttls()
    smtp.login('accenture.bot@authbridge.com', 'Auth@1234')
    smtp.sendmail(from_addr, to_addr, email_data)
    smtp.quit()



# #
# if __name__ == "__main__":
# 	for data in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/New folder'):
# 		if data.endswith('.zip'):
# 			data2 = (r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/New folder/%s')%data
# 			pdf_path = unzip(data2)
# time.sleep(2)
# os.remove(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/New folder//'+data)
# time.sleep(5)
# if __name__ == "__main__":
#     for data1 in os.listdir(r'C:\Users\A sylvia\PycharmProjects\KT\KT\extractedzip'):
#         data3 = (r'C:\Users\A sylvia\PycharmProjects\KT\KT\New folder\%s') % data1
#         if data3.endswith('.jpg'):
#             image1()
#         else:
#
# image1()
# time.sleep(2)
# image3()
# time.sleep(2)
# consopdf()
# time.sleep(2)


# if __name__ == "__main__":
#     for data in os.listdir(r'C:\Users\A sylvia\PycharmProjects\KT\KT\New folder'):
#         if data.endswith('.pdf'):
#             data2 = (r'C:\Users\A sylvia\PycharmProjects\KT\KT\New folder\%s') % data
#             BVF_Extract(data2, data, id1)
            # print(data2)
            # pdf_path = zip(r'C:\Users\A sylvia\PycharmProjects\KT\KT\New folder')
#             # print(pdf_path)
#             with pdfplumber.open(data2) as pdf:
#                 pages = pdf.pages
#                 single_page_text = pages[0].extract_text()
#                 # print(single_page_text)
#                 if 'BACKGROUND VERIFICATION FORM' in single_page_text:
#                     path = os.path.join(r"C:\Users\A sylvia\PycharmProjects\KT\KT\extractedzip\\", data2)
#                     with pdfplumber.open(path) as pdf:
#                         pages1 = pdf.pages
#                         single_page_text1 = pages1[1].extract_text()
#                         if 'Education Qualification (Highest Education)- Please attach copy of Degree and Final year mark' in single_page_text1:
#                             # print(single_page_text1)
#                             path2 = os.path.join(r'C:\Users\A sylvia\PycharmProjects\KT\KT\extractedzip\\', path)
#                             BVF_Extract(path2, data, id1)
# id=uuid4()
# print(id)
# if __name__ == "__main__":
#     for data in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder'):
#         if data.endswith('.pdf'):
#             data2 = (r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder/%s')%data
#             print(data2)
#             BVF_Extract(data2, data,'cbeb8bc7-36b2-4a6d-a34d-8195e4037659')
# #
# id=uuid4()
# convert(id,'1003954324')











































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































# if __name__ == "__main__":
#     for data in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/New folder'):
#         if data.endswith('.pdf'):
#             data2 = (r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/New folder/%s')%data
#             print(data2)
#             # pdf_path = zip(r'C:\Users\A sylvia\PycharmProjects\KT\KT\New folder')
#             # print(pdf_path)
#             BVF_Extract(data2, data,'42543828-1370-49b0-82cf-42a9b24474c9')
# # #
# time.sleep(2)
# convert(id1)
# package_id()


# default_case_documents()
# while True:
#     login_infsoys()
