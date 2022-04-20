import openpyxl
from selenium import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import shutil
import datetime
#import psycopg2
import os
import smtplib

import mysql.connector

import psycopg2
from uuid import uuid4
from zipBGV import image1,image3,consopdf,convert,unzip,BVF_Extract
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
conn = psycopg2.connect(host='localhost', database='postgres', user='postgres', password='1234', port=5432)
print("Connected")
cursor = conn.cursor()


def email_sendattch(toaddr, body, sub):
    fromaddr = "accenture.bot@authbridge.com"

    # instance of MIMEMultipart
    msg = MIMEMultipart()

    # storing the senders email address
    msg['From'] = fromaddr

    # storing the receivers email address
    msg['To'] = toaddr

    # storing the subject
    msg['Subject'] = sub

    # string to store the body of the mail
    body = body

    # attach the body with the msg instance
    msg.attach(MIMEText(body, 'plain'))

    # open the file to be sent
    filename = "ss.png"
    attachment = open("/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf/ss.png", "rb")

    # instance of MIMEBase and named as p
    p = MIMEBase('application', 'octet-stream')

    # To change the payload into encoded form
    p.set_payload((attachment).read())

    # encode into base64
    encoders.encode_base64(p)

    p.add_header('Content-Disposition', "attachment; filename= %s" % filename)

    # attach the instance 'p' to instance 'msg'
    msg.attach(p)

    # creates SMTP session
    s = smtplib.SMTP('smtp.gmail.com', 587)

    # start TLS for security
    s.starttls()

    # Authentication
    s.login(fromaddr, "Auth@12345")

    # Converts the Multipart msg into a string
    text = msg.as_string()

    # sending the mail
    s.sendmail(fromaddr, toaddr, text)

    # terminating the session
    s.quit()
import mysql.connector

mydb = mysql.connector.connect(
    host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
    port='3306',
    user="sequelstring_app_usr",
    password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
    database="pvt_prod"
)

mycursor = mydb.cursor(buffered=True)
LOGIN_COUNT = 0
LOGIN_LIMIT = 6
download_folder = r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/New folder" #download path for zip folder

chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('--headless')
prefs = {"profile.default_content_setting_values.notifications" : 2,
'download.default_directory' :download_folder, # Set own Download path
"download.prompt_for_download": False, # Do not ask for download at runtime
"download.directory_upgrade": True, # Also needed to suppress download prompt
"plugins.plugins_disabled": ["Chrome PDF Viewer"], # Disable this plugin
"plugins.always_open_pdf_externally": True }
chrome_options.add_experimental_option('prefs', prefs)


path = r"/home/ubuntu/Downloads/chromedriver"   # crome driver path
driver = webdriver.Chrome(options = chrome_options, executable_path=os.path.join(os.getcwd(),'Utility Files',path))
driver.maximize_window()
def email_sends(email_data, to_addr):
    from_addr = 'accenture.bot@authbridge.com'
    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    smtp.starttls()
    smtp.login('accenture.bot@authbridge.com', 'Auth@12345')
    smtp.sendmail(from_addr, to_addr, email_data)
    smtp.quit()

def JAFF_Application(id1,candidate_id):
    flag = 0
    flaag = 0
    import mysql.connector
    

    mydb = mysql.connector.connect(

        host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
        port='3306',
        user="sequelstring_app_usr",
        password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
        database="pvt_prod"
    )

    mycursor = mydb.cursor(buffered=True)
    if __name__ == "__main__":
        for files in os.listdir(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf"):
            if 'Launchpad_BVF_' in files or 'Launchpad_FRESHERBVF_' in files:
                flaag = 1
                shutil.copy(f"//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//extractedpdf//{files}",
                f"//home//ubuntu//PycharmProjects//Authbridge//infosis (2)//pdf_folder//{files}")
            elif 'Launchpad_LOA_' in files:
                flag=1
        if flag == 0:
            email_sends(f"This {candidate_id} candiadate DOEST NOT have LOA", "naveenm@checkfootprints.com")
            query4 = f"""Update pvt_prod.infosys_casedownload SET status='download_failed' where candidate_id='{candidate_id}';"""
            mycursor.execute(query4)
            mydb.commit()
            query6 = f"""Update infosys_downloades SET loa_status='No',remarks='Doesnt have LOA' where cid='{candidate_id}'"""
            cursor.execute(query6)
            conn.commit()
        if flaag == 0:
            email_sends(f"This {candidate_id} candiadate DOEST NOT have jaff", "naveenm@checkfootprints.com")
            query4 = f"""Update pvt_prod.infosys_casedownload SET status='download_failed' where candidate_id='{candidate_id}';"""
            mycursor.execute(query4)
            mydb.commit()
            query6 = f"""Update infosys_downloades SET jaff_status='No',remarks='Doesnt have jaff' where cid='{candidate_id}'"""
            cursor.execute(query6)
            conn.commit()

        if flaag == 0 and flag == 0:
            email_sends(f"This {candidate_id} candiadate DOEST NOT have jaff and LOA", "naveenm@checkfootprints.com")
            query4 = f"""Update pvt_prod.infosys_casedownload SET status='download_failed' where candidate_id='{candidate_id}';"""
            mycursor.execute(query4)
            mydb.commit()
            query6 = f"""Update infosys_downloades SET loa_status='No',jaff_status='No',remarks='Doesnt have LOA,jaff' where cid='{candidate_id}'"""
            cursor.execute(query6)
            conn.commit()
        for data in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder'):
            if '.pdf' in data:
                data2 = (r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder/%s')%data
                BVF_Extract(data2, data, id1)
            else:
                pass
        time.sleep(2)
        for data3 in os.listdir(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder"):
            os.remove(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder//"+data3)


def countfiles(candidate_id):
    k=[]
    for files in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf'):
        k.append(files)
    query = f"""Update infosys_downloades SET total_document='{str(len(k))}' where cid='{candidate_id}'"""
    cursor.execute(query)
    conn.commit()
def unzip_main(candidate_id):
    flaag=0
    if __name__ == "__main__":
        for data in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/New folder'):
            if data.endswith('.zip'):
                flaag=1
                data2 = (r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/New folder/%s') % data
                pdf_path = unzip(data2)
        if flaag==0:
            email_sends(f"This {candidate_id} candiadate DOEST NOT have Zip", "naveenm@checkfootprints.com")
            query5 = f"""Update infosys_downloades set zip_received='No',jaff_status='No',loa_status='No',final_status='Failed',remarks='Candidate doest have zip' where cid='{candidate_id}';"""
            cursor.execute(query5)
            conn.commit()
            query6 = f"""Update pvt_prod.infosys_casedownload SET status='download_failed' where candidate_id='{candidate_id}';"""
            mycursor.execute(query6)
            mydb.commit()
    time.sleep(2)
    for data2 in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/New folder'):
        os.remove(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/New folder//' + data2)
def get_captcha2(browser, path):
    from PIL import Image
    from python_anticaptcha import AnticaptchaClient, ImageToTextTask
    browser.save_screenshot(path)
    image = Image.open(path)
    width, height = image.size
    print(width, height)
    ###### Frontend ##########
    left = 550
    top = height / 2.1
    right = 900
    bottom = 3 * height / 4.2
    image = image.crop((left, top, right, bottom))
    image.save(path)
    api_key = 'b2b510888e9324fdf42975d81168b15a'  #sonu id
    captcha_fp = open(path, 'rb')
    client = AnticaptchaClient(api_key)
    task = ImageToTextTask(captcha_fp)
    job = client.createTask(task)
    job.join()
    time.sleep(3)
    # print(job.get_captcha_text())
    result = job.get_captcha_text()

    return result


def main_login():
    global LOGIN_COUNT
    LOGIN_COUNT += 1
    if LOGIN_COUNT > LOGIN_LIMIT:
        return 'Captcha didnt take call not correct'
    time.sleep(2)
    driver.get('https://careers.infosys.com/PlacementPortal/Aspx/BGC/BGCPortalLogin.aspx')
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'txtUserName'))).send_keys('prerna.mathur@checkfootprints.com')
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'txtPassword'))).send_keys('Footprints@4444')
    while True:
        try:
            val = get_captcha2(driver, r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/captcha/captcha.png")
            if 'Invalid captcha' in driver.page_source:
                raise Exception
            break
        except Exception as e:
            print(e)
            print("captcha try again")
            time.sleep(2)
    # val = get_captcha2(browser, os.path.join(os.getcwd(),'Utility Files','captcha.png'))
    time.sleep(2)
    print(val)
    if val != "error":
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'txtLoginCaptcha'))).send_keys(val)
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'btnLogin'))).click()
        except:
            pass
    time.sleep(10)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/form/div[6]/div/div/div/div[2]/div[3]')))
    except:
        driver.close()
        time.sleep(2)
        return main_login()





    import mysql.connector

    mydb = mysql.connector.connect(
        host="ab-mum-prod-acs-vault-crawling.ctlr02ymq4f3.ap-south-1.rds.amazonaws.com",
        port='3306',
        user="sequelstring_app_usr",
        password="5eF410-ef973A-49a5Rl-c2o690-084081-a2ac3d",
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
    query1="""select cid from infosys_downloades where remarks='Candidate id not found'"""
    cursor.execute(query1)
    data1=cursor.fetchall()
    c = []
    for i in data:
        # print(i[0])
        c.append(i[0])

    for candidate_id in range(0, len(c)):
        try:
            time.sleep(2)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'txtFilterCandidateId'))).send_keys(c[candidate_id])
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'btnFilterResult'))).click()
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, 'grdViewcheckDetails_lnkbtnDownloadRelatedDoments_0'))).click()
            timestamp = datetime.datetime.today()
            query = """insert into infosys_downloades values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            values = (c[candidate_id], 'yes', 'yes', 'yes', 'yes', '', '', '', '', '', '', '', '', str(timestamp))
            cursor.execute(query, values)
            conn.commit()
            driver.save_screenshot(r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf/ss.png")
            try:
                No_files=WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="divPageMessages"]/p'))).text
                print(No_files)
                email_sendattch('naveenm@checkfootprints.com','Hello sir,Please find below attachment of No files Found','Infosys case downlaod')
            except:
                pass
            # breakpoint()
            condition = True
            while condition:
                for file in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/New folder'):
                    if file.endswith('.zip'):
                        print(file)
                        condition = False
                        break
                    # if No_files =='No files found.':
                    #     condition = False
                    #     break


        ######run Extraction and all backend funtionallity#########//*[@id="Label1"]
            time.sleep(2)
            id = uuid4()
            id1 = id
            # time.sleep(1)
            print(unzip_main(c[candidate_id]))
            # time.sleep(1)
            countfiles(c[candidate_id])
            # time.sleep(1)
            print(image1())
            # time.sleep(1)
            print(image3())
            # time.sleep(1)
            print(consopdf())
            # time.sleep(1)
            print(convert(id1, c[candidate_id]))
            # time.sleep(1)
            print(JAFF_Application(id1, c[candidate_id]))
            # time.sleep(1)


                # query = f"""Upadate infosys_download SET status='Fail' reason_id='Candidate doesnt have jaff' where candidate_id='{c[candidate_id]}'"""
                # values = (c[candidate_id],'DOESNOT HAVE JAFF','Fail',timestmaps)
                # cursor.execute(query)
                # conn.commit()
                # query = f"""Upadate infosys_download SET status='Fail' reason_id='Candidate doesnt have LOA' where candidate_id='{c[candidate_id]}'"""
                # values = (c[candidate_id], 'DOEST NOT have LOA', 'Fail', timestmaps)
                # cursor.execute(query)
                # conn.commit()
            time.sleep(2)
            for data5 in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf'):
                os.remove(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf//' + data5)
            driver.get('https://careers.infosys.com/PlacementPortal/ASPX/BGC/BGCPortalLanding.aspx')
        except:
            email_sends(f"infosys Portal is down or data not Found {c[candidate_id]}", "naveenm@checkfootprints.com")
            driver.get('https://careers.infosys.com/PlacementPortal/ASPX/BGC/BGCPortalLanding.aspx')
            query5 = f"""Update infosys_downloades set portal_status='No',zip_received='NO',jaff_status='No',loa_status='No',remarks='Candidate id not found' where cid='{c[candidate_id]}';"""
            cursor.execute(query5)
            conn.commit()
            query6 = f"""Update pvt_prod.infosys_casedownload SET status='download_failed' where candidate_id='{c[candidate_id]}';"""
            mycursor4.execute(query6)
            mydb.commit()
            for data5 in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf'):
                os.remove(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf//' + data5)
            time.sleep(2)
            for data2 in os.listdir(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder'):
                if data2.endswith('.pdf') or data2.endswith('.csv'):
                    os.remove(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/pdf_folder//' + data2)
                else:
                    pass

########### NO JAF IN ZIP##############
main_login()



