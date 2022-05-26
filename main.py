# -*- coding: utf-8 -*-
# usr/.local/bin
import time

import utils
import constants
import database

from polyglot.detect import Detector
from polyglot.utils import pretty_list
from polyglot.detect.base import logger as polyglot_logger
from polyglot.detect.base import UnknownLanguage
from os import listdir
from os.path import isfile, join

import csv
import pandas
import sys

polyglot_logger.setLevel("ERROR")

start_time_global = time.time()

def main():
    args = sys.argv
    if(len(args) > 2):
        if(args[1] == "db"):
            connection = database.connect()
            if(args[2] == "createtables"):
                print("Crating Tables")
                database.dropTable(connection, 'data')
                database.createTable(connection, 'data', ['date TEXT', 'ip TEXT', 'domain TEXT', 'country TEXT', 'sender TEXT', 'subject TEXT', 'language TEXT', 'confidence NUMERIC', 'translate_subject TEXT' ])
                database.dropTable(connection, 'languages')
                database.createTable(connection, 'languages', ['language TEXT', 'quantity INT'])
                database.dropTable(connection, 'countries')
                database.createTable(connection, 'countries', ['code TEXT', 'country TEXT', 'quantity INT'])
            if(args[2] == "loaddata"):
                database.truncateTable(connection, 'languages')
                loadLanguages(connection)
                database.truncateTable(connection, 'countries')
                loadCountries(connection)
            connection.close()
    
    if(args[1] == "process"):
        preProcess()
   
def loadLanguages(connection):
    LanguagesCleaned = list(dict.fromkeys(constants.LANGUAGES))
    for i in LanguagesCleaned:
        database.insert(connection, 'languages', [f"\"{i}\"", "'0'"])

def loadCountries(connection):
    for i in constants.COUNTRIES:
        database.insert(connection, 'countries', [f"'{i}'", f"\"{constants.COUNTRIES[i]}\"", "'0'"])

def incrementCountry(connection, code):
    country = database.searchInTable(connection, 'countries', 'code', code)
    country_name = constants.COUNTRIES.get(code) or ""
    if(country is None):
        database.insert(connection, 'countries', [f"'{code}'", f"\"{country_name}\"", "'1'"])
    else:
        quantity = int(country['quantity']) + 1
        database.update(connection, 'countries', {'quantity': f'{quantity}'}, {'code': code})

def incrementLanguage(connection, lang):
    language = database.searchInTable(connection, 'languages', 'language', lang)
    if(language is None):
        database.insert(connection, 'languages', [f"\"{lang}\"", "'1'"])
    else:
        quantity = int(language['quantity']) + 1
        database.update(connection, 'languages', {'quantity': f'{quantity}'}, {'language': lang})

def insertData(connection, date, ip, domain, country, sender, subject, language, confidence, translate_subject):
    database.insert(connection, 'data', [f"\"{date}\"", f"\"{ip}\"", f"\"{domain}\"", f"\"{country}\"", f"\"{sender}\"", f"\"{subject}\"", f"\"{language}\"", f"\"{confidence}\"", f"\"{translate_subject}\""])

def preProcess():
    subdirectories = utils.getPathFolders(constants.PATH)

    for i in subdirectories:
        print(f'Folder: {i}')
        fullpath = f'{constants.PATH}/{i}'
        files = [f for f in listdir(fullpath) if isfile(join(fullpath, f))]
        for file in files:
            print(f"\tReading: {file}")
            filepath = f'{fullpath}/{file}'
            process(filepath)
            #print(f'\t{file}')

#df = pandas.read_csv('./Feeds/20220325-spam_account-AR.csv', engine="pyarrow")
#print(df)


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
    
    with open(file) as csv_file:
        connection = database.connect()
        start_time_file = time.time()

        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        try:
            for row in csv_reader:
                if line_count == 0:
                    # print(f'Column names are {", ".join(row)}')
                    line_count += 1
                else:
                    #print(f'\t FROM: {row[8]} / IP: {row[2]} / SUBJECT: {row[9]}.')
                    incrementCountry(connection, row[5])
                    detector = Detector(utils.remove_bad_chars(row[9]), quiet=True)
                    #print(detector.language)
                    incrementLanguage(connection, detector.language.name)
                    insertData(connection, row[1], row[2], row[7], row[5], row[8], utils.remove_bad_chars(row[9]).replace("\"", "\'"), detector.language.name, detector.language.confidence, '')
                    #print(f'\t', detector.language, '\n')
                    line_count += 1
            database.disconnect(connection)
        except csv.Error:
            database.disconnect(connection)
            print(f"An exception occurred at line {csv_reader.line_num} in file {file}")
        

        
        print(f'\tProcessed {line_count} lines.')
    
    print("\t--- %s ms ---" % ((time.time() - start_time_file) * 1000))
    print()
    

main()
print("--- %s ms ---" % ((time.time() - start_time_global) * 1000))
print()