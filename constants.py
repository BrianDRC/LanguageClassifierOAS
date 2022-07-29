import regex

RE_BAD_CHARS = regex.compile(r"[\p{Cc}\p{Cs}]+")