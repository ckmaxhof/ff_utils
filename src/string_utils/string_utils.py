import constants

from collections import Counter
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import numpy as np
import re
import string

### Text Cleaning
patterns_item = [
    re.compile(r"\((\d+)\)"),
    re.compile(r"\((\d+pc)\)"),
    re.compile(r"\((\d+ pc)\)"),
    re.compile(r"\((\d+ pieces)\)"),
    re.compile(r"^\d+ ")
]

patterns_drink = [
    re.compile(r"\d+ml"),
    re.compile(r"\d+cl")
]

patterns_weight = [
    re.compile(r"\d+g"),
    re.compile(r"\d+mg"),
    re.compile(r"\d+kg"),
]

def clean_item_string_from_count(name, patterns=patterns_item):

    str_clean = re.sub(patterns[0], '', name)
    str_clean = re.sub(patterns[1], '', str_clean)
    str_clean = re.sub(patterns[2], '', str_clean)
    str_clean = re.sub(patterns[3], '', str_clean)
    str_clean = re.sub(patterns[4], '', str_clean)
    
    return str_clean.strip()

def get_item_count_from_string(name, patterns=patterns_item):
        
    item_counts = []
    item_counts.append(''.join(patterns[0].findall(name)))
    item_counts.append(''.join(patterns[1].findall(name)).replace('pc',''))
    item_counts.append(''.join(patterns[2].findall(name)).replace(' pc',''))
    item_counts.append(''.join(patterns[3].findall(name)).replace(' pieces',''))
    item_counts.append(''.join(patterns[4].findall(name)).strip())
        
    res = [x for x in item_counts if x != '']
    
    if res:
        return np.int64(res[0])
    else:
        return np.nan
    
def clean_item_string_from_drink(name, patterns=patterns_drink):

    str_clean = re.sub(patterns[0], '', name)
    str_clean = re.sub(patterns[1], '', str_clean)
    
    return str_clean.strip()

def get_drink_qty_from_string(name, patterns=patterns_drink):
    
    item_counts = []
    item_counts.append(''.join(patterns[0].findall(name)))
    item_counts.append(''.join(patterns[1].findall(name)))
    
    res = [x for x in item_counts if x != '']
    
    if res:
        return res[0]
    else:
        return None
    
def clean_item_string_from_weight(name, patterns=patterns_weight):

    str_clean = re.sub(patterns[0], '', name)
    str_clean = re.sub(patterns[1], '', str_clean)
    str_clean = re.sub(patterns[2], '', str_clean)
    
    return str_clean.strip()

def get_weight_from_string(name, patterns=patterns_weight):
    
    item_counts = []
    item_counts.append(''.join(patterns[0].findall(name)))
    item_counts.append(''.join(patterns[1].findall(name)))
    item_counts.append(''.join(patterns[2].findall(name)))
    
    res = [x for x in item_counts if x != '']
    
    if res:
        return res[0]
    else:
        return None

def remove_punctuation_digits_from_string(name):
    
    return name.translate(str.maketrans('','', string.punctuation)).translate(str.maketrans('','', string.digits))

def to_lower_string(name):
    
    return name.lower()

def remove_stopwords(name, stopwords=constants.stopwords):
    new_name = []
    for word in name.split(' '):
        if word.lower() not in stopwords and word != '':
            new_name.append(word.lower())
            
    return ' '.join(new_name)

def remove_stopwords_new(name, stopwords=constants.stopwords):
    return ' '.join([word for word in name.split(' ') if word not in stopwords and word != ''])

def remove_stopwords_new_new(name, stopwords=constants.stopwords):
    stopwords_dict = Counter(stopwords)
    return ' '.join([word.lower() for word in name.split(' ') if word.lower() not in stopwords_dict and word != ''])

def stem_sentence(name):
    porter = PorterStemmer()
    token_words = word_tokenize(name)
    stem_sentence = [porter.stem(word) if word not in constants.ignore_stem_words else word for word in token_words]
    return ' '.join(stem_sentence)

def string_to_clean_name(name):
    s = remove_punctuation_digits_from_string(name)
    s = to_lower_string(s)
    s = stem_sentence(s)
    return s

# NLP
def cos_similarity(v1, v2): # manually creating function to perform cos_similarity
    dot_product = np.dot(v1, v2)
    l2_norm = (np.sqrt(sum(np.square(v1))) * np.sqrt(sum(np.square(v2))))
    similarity = dot_product / l2_norm     
    return similarity

def get_search_term_query(search_term):
    l = [string_to_clean_name(x) for x in search_term.split(' ')]
    
    string_match = ''
    for st in l:
        string_match += '''
            AND (
                clean_name LIKE '%%{}%%'
                )
        '''.format(st)
        
    return string_match