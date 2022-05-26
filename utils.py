# -*- coding: utf-8 -*-
# usr/.local/bin

import constants

from os import listdir

def getPathFolders(PATH):
    return listdir(PATH)

def remove_bad_chars(text):
    return constants.RE_BAD_CHARS.sub("", text)

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d