import time

import utils
from pymongo import MongoClient

from polyglot.detect import Detector
from polyglot.utils import pretty_list
from polyglot.detect.base import logger as polyglot_logger
from polyglot.detect.base import UnknownLanguage

import numba as nb

from glob import glob
import csv
#import pandas

polyglot_logger.setLevel("ERROR")

def incrementCountry(countriesTable, code):
    country = countriesTable.find_one({"code": code})
    if(country == None):
        countriesTable.insert_one({"code": code, "name": "", "quantity": 1})
    else:
        quantity = country["quantity"]
        quantity = quantity + 1
        countriesTable.update_one({"_id": country["_id"]}, { "$set": { "quantity": quantity } })

def incrementLanguage(languagesTable, lang, code):
    language = languagesTable.find_one({"code": code})
    if(language == None):
        languagesTable.insert_one({"name": lang, "code": code, "quantity": 1})
    else:
        quantity = language["quantity"]
        quantity = quantity + 1
        languagesTable.update_one({"_id": language['_id']}, { "$set": {"quantity": quantity } })

def log(filesTable, file, rows, time):
    filesTable.insert_one({'file': file, 'rows': rows, 'time': time})


def insertData(dataTable, date, ip, domain, country, sender, subject, language, confidence):
    dataTable.insert_one({'date': date, 'ip': ip, 'domain': domain, 'country': country, 'sender': sender, 'subject':subject, 'language': language, 'confidence': confidence, 'translate_subject': '' })

#@nb.njit
def main():
    list_csvs = glob("./Feeds/**/*.csv",recursive = True)
    for file in list_csvs:
        process(file)
        #print(f'\t{file}')


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
def process(file):
    client = MongoClient()
    db = client.spam_relay2 if (file.find("spam_relay2") != -1) else client.spam_relay4
    countriesTable = db.countries
    languagesTable = db.languages
    dataTable = db.processed_data
    filesTable = db.log_files

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
                    clean_subject   = utils.remove_bad_chars(row[7])
                    incrementCountry(countriesTable, clean_country)
                    #clean_subject = utils.remove_bad_chars(row[9])
                    detector = Detector(clean_subject, quiet=True)
                    #print(f'\t {detector.language.name} - {detector.language.confidence}')
                    incrementLanguage(languagesTable, detector.language.name, detector.language.code)
                    #insertData(dataTable, row[1], row[2], row[7], row[5], row[8], clean_subject, detector.language.name, detector.language.confidence)
                    insertData(dataTable, clean_date, clean_ip, "", clean_country, "", clean_subject, detector.language.name, detector.language.confidence)
                    line_count += 1
        except csv.Error:
            print(f"An exception occurred at line {csv_reader.line_num} in file {file}")
        file_time = (time.time() - start_time_file) * 1000
        log(filesTable, file, line_count, file_time)
        client.close()
        #print(f'\tProcessed {line_count} lines.')
    
    #print("\t--- %s ms ---" % (file_time))
    #print()
    
main()