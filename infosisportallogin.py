from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver import ActionChains
from selenium.webdriver.support import expected_conditions as EC
import time
import shutil
import datetime
#import psycopg2
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from datetime import date
import mysql.connector
from bs4 import BeautifulSoup
from urllib import request
import urllib
import psycopg2

def get_new_users_data(timestamp):
    conn= psycopg2.connect(database='postgres', user='postgres',password='1234',host='localhost',port='5432')
    cursor=conn.cursor()
    db_data = list(cursor.execute(f"select * from infoys_1 where timestamps='{timestamp}'"))
    print("Total Rows:", len(db_data))
    return db_data, cursor, conn

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
driver.get('https://careers.infosys.com/PlacementPortal/Aspx/BGC/BGCPortalLogin.aspx')
driver.maximize_window()


def get_captcha2(browser, path):
    from PIL import Image
    from python_anticaptcha import AnticaptchaClient, ImageToTextTask
    browser.save_screenshot(path)
    image = Image.open(path)
    width, height = image.size
    print(width, height)
    ###### Frontend ##########
    left = 670
    top = height / 1.9
    right = 900
    bottom = 3 * height / 5.1
    image = image.crop((left, top, right, bottom))
    image.save(path)
    # input('INPUT')
    # api_key = 'fdfba5ab51dd4fb2bbc64bca5698a797'   #pranav id
    # api_key = '4002e504ff779fd7254eb63ee978cb46'   #sumesh id
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


try:
    # User & Password
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'txtUserName'))).send_keys('prerna.mathur@checkfootprints.com')
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'txtPassword'))).send_keys('Footprints@4444')
    time.sleep(25)
    while True:
        try:
            # val = get_captcha2(driver, r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/captcha.png")
            val = get_captcha2(driver, r"/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/infosis/captcha/captcha.png")
            if 'Invalid captcha' in driver.page_source:
                raise Exception
            break
        except Exception as e:
            print(e)
            print("captcha try again")
            time.sleep(10)
    # val = get_captcha2(browser, os.path.join(os.getcwd(),'Utility Files','captcha.png'))
    time.sleep(2)
    print(val)
    if val != "error":
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'txtLoginCaptcha'))).send_keys(val)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'btnLogin'))).click()
        print("Database connecting........")
        time.sleep(10)
    import mysql.connector

    mydb = mysql.connector.connect(
        host="10.160.15.229",
        port='3306',
        user="usr_python_app",
        password="usrsdfsIdd1144",
        database="pvt_uat"
    )

    mycursor = mydb.cursor(buffered=True)
    mycursor2 = mydb.cursor(buffered=True)
    mycursor3 = mydb.cursor(buffered=True)
    mycursor4 = mydb.cursor(buffered=True)
    query = """SELECT candidate_id From pvt_uat.infosys_casedownload where download_complete='0';"""
    query1 = """SELECT client_reference_no From pvt_uat.infosys_casedownload;"""
    query2 = """SELECT project_name From pvt_uat.infosys_casedownload;"""
    query3 = """SELECT po_no From pvt_uat.infosys_casedownload;"""

    mycursor.execute(query)
    mycursor2.execute(query1)
    mycursor3.execute(query2)
    mycursor4.execute(query3)
    data = mycursor.fetchall()
    data2 = mycursor2.fetchall()
    data3 = mycursor3.fetchall()
    data4 = mycursor4.fetchall()
    print(data, data2, data4, data3)
    c = []
    for i in data:
        # print(i[0])
        c.append(i[0])
        time.sleep(25)
    for candidate_id in range(0, len(c)):
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'txtFilterCandidateId'))).send_keys(c[candidate_id])
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'btnFilterResult'))).click()
        # All download
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, 'grdViewcheckDetails_lnkbtnDownloadRelatedDoments_0'))).click()
        driver.save_screenshot(r'/home/ubuntu/PycharmProjects/Authbridge/infosis (2)/extractedpdf/ss.png')
        time.sleep(350)
        driver.get('https://careers.infosys.com/PlacementPortal/ASPX/BGC/BGCPortalLanding.aspx')


except:
    pass


