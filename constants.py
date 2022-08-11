import regex
from dotenv import load_dotenv
from os import getenv

load_dotenv()

RE_BAD_CHARS = regex.compile(r"[\p{Cc}\p{Cs}]+")
ENVIRONMENT = getenv("ENVIRONMENT")
REMOVE_FILES_AT_END = True if (getenv("REMOVE_FILES_AT_END") == "true") else False
MAX_ROWS = int(getenv("MAX_ROWS"))
COUNTRIES = []
LANGUAGES = []
LOGFILES = []
DATA = []