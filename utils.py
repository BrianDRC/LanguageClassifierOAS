import constants

from os import listdir

def getPathFolders(PATH):
    return listdir(PATH)

def remove_bad_chars(text):
    return constants.RE_BAD_CHARS.sub("", text)
