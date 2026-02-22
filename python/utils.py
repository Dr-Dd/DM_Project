from constants import known_types, known_attributes
import re

def pg_null(v):
    if v is None or v == "\\N":
        return None
    return v

def type_split(word):
    type_pattern = re.compile("|".join(sorted(known_types, key=len, reverse=True)))
    return type_pattern.findall(word)

def attribute_split(word):
    attribute_pattern = re.compile("|".join(sorted(known_attributes, key=len, reverse=True)))
    return attribute_pattern.findall(word)
