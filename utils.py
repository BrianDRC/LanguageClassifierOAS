import constants

def remove_bad_chars(text):
    return constants.RE_BAD_CHARS.sub("", text)
