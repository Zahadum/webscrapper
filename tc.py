import pymsteams
import configparser
from datetime import datetime
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
import mysql.connector
from mysql.connector import Error
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

config = configparser.ConfigParser()
config.read('config.env')
host = config.get('MySQL','host')
database = config.get('MySQL','database')
user = config.get('MySQL','user')
password = config.get('MySQL','password')
hook =  config.get('MySQL','teamswebhook')

caps = DesiredCapabilities().CHROME
caps["pageLoadStrategy"] = "eager"  #  interactive

current_dateTime = datetime.now()

class Person:
  def __init__(self, name, link):
    def getIdFromLink():
        link_array = self.link.split('?id=')
        return str(link_array[1])
    self.name = name
    self.link = link
    self.tc_id = getIdFromLink()

person_array = []
options = Options()
options.add_argument('--ignore-certificate-errors')
options.add_argument("disable-quic")
options.add_argument('headless')
#driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options) 
driver = webdriver.Chrome(desired_capabilities=caps, service=Service(ChromeDriverManager().install()))

#driver = webdriver.Firefox(capabilities=caps, service=Service(GeckoDriverManager().install()))
driver.get('https://www.legacy.com/ca/obituaries/timescolonist/browse')
myTeamsMessage = pymsteams.connectorcard(hook)
myTeamsMessage.text("running...1")
myTeamsMessage.send()

def __scroll_down_page(driver,speed=16):
    myTeamsMessage.text("running...1x")
    myTeamsMessage.send()
    current_scroll_position, new_height= 0, 1
    while current_scroll_position <= new_height:
        current_scroll_position += speed
        driver.execute_script("window.scrollTo(0, {});".format(current_scroll_position))
        new_height = driver.execute_script("return document.body.scrollHeight")


__scroll_down_page(driver)
blog_titles = driver.find_elements(By.CSS_SELECTOR, '[class^="Box-sc-ucqo0b-0 dacPsq"]')
inMemoryClassName = '[class^="Box-sc-ucqo0b-0 Text-sc-8i5r1a-0 efFzny"]'
nameClassName = '[class^="Box-sc-ucqo0b-0 lbRkvc"]'
linkClassName = '[class^="Box-sc-ucqo0b-0 Link-sc-1u14kdb-0 PersonCard___StyledLink2-sc-1opqadm-6 glRqTu dEdZYg hzcvQu"]'
#names = driver.find_elements(By.CSS_SELECTOR, nameClassName)

for title in blog_titles:
    inMemory = ''
    inMemories = title.find_elements(By.CSS_SELECTOR, inMemoryClassName)
    names = title.find_elements(By.CSS_SELECTOR, nameClassName)
    links = title.find_elements(By.CSS_SELECTOR, linkClassName)
    for inMemory in inMemories:
        inMemory=inMemory.text
    for name in names:
        name=name.text
    for link in links:
        link=link.get_attribute('href')
    if inMemory != 'IN MEMORIAM':
        p1 = Person(name,link)
        person_array.append(p1)
    else:
        print(name)
driver.quit() # closing the browser


def writeScrappedDataToMySQL(scrappedData):
    if len(scrappedData) == 0:
        return 
    try:
        connection = mysql.connector.connect(host=host,
                                            database=database,
                                            user=user,
                                            password=password)
        
        inserting_array = []

        for row in scrappedData:
            inserting_row = []
            inserting_row.append(row.tc_id)
            inserting_row.append(row.name)
            inserting_row.append(row.link)
            inserting_row.append('Time Colonist')
            inserting_row.append(current_dateTime)
            inserting_row.append(current_dateTime)
            inserting_array.append(inserting_row)
        
        sql_Query = "INSERT INTO obituaries(tc_id,name,link,published_by,created_at,updated_at) VALUES (%s, %s, %s, %s, %s, %s)"
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.executemany(sql_Query,inserting_array)
            connection.commit()
            print(cursor.rowcount, "rows inserted.")

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def getDuplicateRecordId(new_records):
    try:
        connection = mysql.connector.connect(host=host,
                                            database=database,
                                            user=user,
                                            password=password)
        placeholders = ', '.join(['%s'] * len(new_records))
        sql_Query = "SELECT tc_id FROM obituaries WHERE tc_id IN ({})".format(placeholders)
        if connection.is_connected():
            print(new_records)
            cursor = connection.cursor()
            cursor.execute(sql_Query,new_records)
            rows = cursor.fetchall()
            if len(rows)>0:
                text_arrays = []
                for row in rows:
                    text_array = [str(column) for column in row]
                    text_arrays.append(text_array)
                return text_arrays
            else:
                return []

    except Error as e:
        print("Error while connecting to MySQL", e)
        print(cursor._executed)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def removeDuplicateRecords(recordsFromTimeColonistWebsite,duplicateRecords): 
    new_records = []

    for record in recordsFromTimeColonistWebsite:
        duplicate_id_array = []
        for recordFromDuplicateList in duplicateRecords:
            for col in recordFromDuplicateList:
                duplicate_id_array.append(col)
        if str(record.tc_id) in duplicate_id_array:
            print('existed')
        else:
            new_records.append(record)
    return new_records
def scrapIndividualObituary(record):
    driverSub = webdriver.Chrome(desired_capabilities=caps, service=Service(ChromeDriverManager().install()))
    driverSub.get(record.link)    
    content_array = driverSub.find_elements(By.CSS_SELECTOR, '[class^="Paragraph-sc-osiab4-0 ObituaryText___StyledParagraph-sc-12f7zd1-0"]')
    born_died_box_array = driverSub.find_elements(By.CSS_SELECTOR, '[class^="Box-sc-ucqo0b-0 Flex-sc-d1l2vy-0 eqTuri"]')
    born_died_css_class = '[class^="Box-sc-ucqo0b-0 Text-sc-8i5r1a-0 fNnNbH gacyGL"]'
    obituary_bornValue = ''
    obituary_diedValue = ''
    for born_died_box in born_died_box_array:
        elements = born_died_box.find_elements(By.CSS_SELECTOR,born_died_css_class)
        count = 0
        for element in elements:
            if count == 0:
                obituary_bornValue = element.text
            elif count == 1:
                obituary_diedValue = element.text
            count = count + 1
            
    obituary_content = ''
    for content in content_array:
        isUWMentioned = 0
        obituary_content = content.text
        searchContext = content.text.lower()
        if(searchContext.find('uw')>=0 or searchContext.find('united way')>=0 or searchContext.find('united way southern vancouver island')>=0 or searchContext.find('united way greater victoria')>=0):
            isUWMentioned = 1
    driverSub.quit() # closing the browser
    updateObituaryRecord(obituary_bornValue,obituary_diedValue,obituary_content,record.tc_id,isUWMentioned)

def updateObituaryRecord(born,died,content,tc_id,isUWMentioned):
    try:
        connection = mysql.connector.connect(host=host,
                                            database=database,
                                            user=user,
                                            password=password)
      
        sql_Query = "UPDATE obituaries SET born=%s, died=%s, description=%s, is_uw_mentioned=%s WHERE tc_id=%s"
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(sql_Query,[born,died,content,isUWMentioned,tc_id])
            connection.commit()
            print(cursor.rowcount, "rows updated.")

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

myTeamsMessage.text("running...2")
myTeamsMessage.send()
tc_id_array = []
for person in person_array:
    tc_id_array.append(person.tc_id)
duplicate_records = getDuplicateRecordId(tc_id_array)

person_array_final = removeDuplicateRecords(person_array,duplicate_records)
myTeamsMessage.text("running...3")
myTeamsMessage.send()

writeScrappedDataToMySQL(person_array_final)

for person in person_array_final:
    scrapIndividualObituary(person)
myTeamsMessage.text("running...4 completed!")
myTeamsMessage.send()