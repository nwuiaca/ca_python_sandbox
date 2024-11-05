""" FUNCSTR.PY *****************************************************************
Python string functions
Copyright (c) AB Janse van Rensburg
5 Apr 2018
"""

import re
import string
import hashlib


""" Short description of functions *********************************************
exclude_markup(a) Remove all <> pairs from a string, except if <> > 20
include(a,b) Return a with only characters contained in b
"""

# Function EXCLUDE_MARKUP Remove all <> combinations
def exclude_markup(a):

    c = "<"
    while c in a:
        b = a.find("<")
        e = a.find(">")
        if b > 0 and e < 0:
            d = "<"
        elif e - b > 20:
            d = "<"
            a = a.replace(d,"")
            d = ">"
        else:
            d = a[b:e+1]
        a = a.replace(d," ")

    return a
    
# Function INCLUDE Return a with only characters contained in b
def include(a,b=" abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"):
    
    """ Parameters
    a = Source string
    b = Characters to test
    """
    
    c = ""
    for i in range(0, len(a)):
        if a[i] in b:
            c+= a[i]

    return c

# Function to test if string is blank
def isBlank(myString):
    return not (myString and myString.strip())

# Function to test if string is not blank
def isNotBlank(myString):
    return bool(myString and myString.strip())


# Function to return a cleaned paragraph
def clean_paragraph(paragraph: str = '', words_to_remove: list = [], allow_characters: str = 'b'):
    # Remove any special characters or punctuation and convert to upper case
    if allow_characters == 'a':
        cleaned_paragraph = [char.upper() for char in paragraph if char.isalpha() or char.isspace()]
    elif allow_characters == 'n':
        cleaned_paragraph = [char.upper() for char in paragraph if char.isnumeric() or char.isspace()]
    else:
        cleaned_paragraph = [char.upper() for char in paragraph if char.isalnum() or char.isspace()]
    cleaned_paragraph = ''.join(cleaned_paragraph)
    # Remove words in the word list
    for word in words_to_remove:
        cleaned_paragraph = cleaned_paragraph.replace(word, '')
    # Replace multiple spaces with a single space.
    cleaned_paragraph = re.sub(' +', ' ', cleaned_paragraph)
    # Trim blanks
    cleaned_paragraph = cleaned_paragraph.strip()
    return cleaned_paragraph


# Function to build a word list from a paragraph
def build_word_list(paragraph):
    # Remove any special characters or punctuation and convert to upper case.
    cleaned_paragraph = ''.join(char.upper() for char in paragraph if char.isalnum() or char.isspace())
    # Split the paragraph into individual words based on spaces
    words = cleaned_paragraph.split()
    return words


# Function that generates an MD5 hash from input data
def generate_md5_hash(data):
    md5_hash = hashlib.md5(data.encode('utf-8')).hexdigest()
    return md5_hash
   