
from itertools import count
import utils
import time
import csv
import resource
from os import getenv
from os import scandir
from os import getcwd
from os import remove
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient
from glob import glob
from multiprocessing import Pool

from polyglot.detect import Detector
from polyglot.detect.base import logger as polyglot_logger
from polyglot.detect.base import UnknownLanguage

polyglot_logger.setLevel("ERROR")
load_dotenv()
ENVIRONMENT = getenv("ENVIRONMENT")
REMOVE_FILES_AT_END = True if (getenv("REMOVE_FILES_AT_END") == "true") else False
MAX_ROWS = int(getenv("MAX_ROWS"))
COUNTRIES = []
LANGUAGES = []
LOGFILES = []
DATA = []

def get_memory():
    with open('/proc/meminfo', 'r') as mem:
        free_memory = 0
        for i in mem:
            sline = i.split()
            if str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                free_memory += int(sline[1])
    return free_memory

def memory_limit():
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (get_memory() * 1024 / 2, hard))

def incrementCountry(code):
    index = next((i for i, item in enumerate(COUNTRIES) if item["code"] == code), None)
    if(index == None):
        COUNTRIES.append({'code': code, 'name': '', 'quantity': 1})
    else:
        COUNTRIES[index]['quantity'] += 1

def incrementLanguage(lang, code):
    index = next((i for i, item in enumerate(LANGUAGES) if item["code"] == code), None)
    if(index == None):
        LANGUAGES.append({'code': code, 'name': lang, 'quantity': 1})
    else:
        LANGUAGES[index]['quantity'] += 1

def log(file, rows, time):
    LOGFILES.append({'file': file, 'rows': rows, 'time': time})
    
def insertData(date, ip, domain, country, sender, subject, language, confidence):
    DATA.append({'date': date, 'ip': ip, 'domain': domain, 'country': country, 'sender': sender, 'subject':subject, 'language': language, 'confidence': confidence, 'translate_subject': '' })

def cleanLists():
    COUNTRIES.clear()
    LANGUAGES.clear()
    LOGFILES.clear()
    DATA.clear()

def save_processed_data(db):
    dataTable = db.processed_data
    dataTable.insert_many(DATA)
    DATA.clear()

def main():
    subfolders = [f.path for f in scandir(getcwd() +'/Feeds/') if f.is_dir()]

    if(ENVIRONMENT == "dev"):
        client = MongoClient()
    elif(ENVIRONMENT == "prod"):
        client = MongoClient(username="root", password="12345", authSource="admin")

    for folder in subfolders:
        f = folder.split("/")[6]
        list_csvs = glob(f"./Feeds/{f}/*.csv", recursive = True)
        db = client[f]        
        for file in list_csvs:
            process(file, db)
            if(REMOVE_FILES_AT_END):
                remove(file)
        filesTable = db.log_files
        countriesTable = db.countries
        languagesTable = db.languages

        countriesTable.insert_many(COUNTRIES)
        languagesTable.insert_many(LANGUAGES)
        filesTable.insert_many(LOGFILES)
        save_processed_data(db)
        cleanLists()
    client.close()

""" HEADERS
# CSIRTAmericas Observation time [0],
# SMTP Timestamp [1] = Send Date From SMTP Server,
# Source IP [2],
# ASN [3] = Autonomous System Number,
# AS Name [4] = , 
# Source CC [5] = Country where the email was sent, 
# Source Registry [6] = Registro de Direcciones IP,
# Domain [7]
# Mail From [8], Subject [9], Attachment Hashes [10], Attachment Types [11], Email URLs [12], Email Headers [13],
# Taxonomy [14] = Taxonomic Category from CSIRTAMERICAS,
# Provider [15]
"""
def process(file, db):
    with open(file) as csv_file:
        start_time_file = time.time()
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        try:
            for row in csv_reader:
                if line_count == 0:
                    line_count += 1
                else:
                    #print(f'\t FROM: {row[8]} / IP: {row[2]} / SUBJECT: {row[9]}')
                    clean_date      = row[1]
                    clean_ip        = row[2]
                    clean_country   = row[5]
                    clean_domain    = row[7] if (len(row) == 16) else "" 
                    clean_sender    = row[8] if (len(row) == 16) else ""
                    clean_subject   = utils.remove_bad_chars(row[9]) if (len(row) == 16) else utils.remove_bad_chars(row[7])
                    incrementCountry(clean_country)
                    detector = Detector(clean_subject, quiet=True)
                    incrementLanguage(detector.language.name, detector.language.code)
                    insertData(clean_date, clean_ip, clean_domain, clean_country, clean_sender, clean_subject, detector.language.name, detector.language.confidence)
                    line_count += 1
                    if(len(DATA) == MAX_ROWS):
                        save_processed_data(db)
        except csv.Error:
            print(f"An exception occurred at line {csv_reader.line_num} in file {file}")
        file_time = (time.time() - start_time_file) * 1000
        log(file, line_count, file_time)
        
start_time = time.time()
main()
print("--- %s seconds ---" % (time.time() - start_time))