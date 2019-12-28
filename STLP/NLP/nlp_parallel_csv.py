#!/bin/env python
import spacy
import urllib.request
from bs4 import BeautifulSoup
from bs4.element import Comment
import urllib.request
import re
import time
from nltk.corpus import stopwords
import multiprocessing as mp
# import warnings
# warnings.filterwarnings('ignore')
import csv
from collections import OrderedDict

html1 = ''
html2 = ''
t1 = ''
t2 = ''
t1w = ''
t2w = ''
t1t = ''
t2t = ''
kw1 = ''
desc1 = ''
kw2 = ''
desc2 = ''
title1 = ''
title2 = ''
doc1_segments = []
doc2_segments = []
n_doc1_segments = 0
n_doc2_segments = 0
ct0 = 0
pt = time.perf_counter()
doc_segments = OrderedDict()


def read_dictionary():
    """
    Read the dictionary and stop words
    :return: None
    """
    global ct0, nlp, sr
    ct0 = time.perf_counter()
    print("Starting to read dictionary and stopwords")
    nlp = spacy.load("en_core_web_md")
    sr = stopwords.words('english')
    # et = time.perf_counter() - ct0
    # print("Finished reading: {:0.2f} sec".format(et))


# ------Functions
def Lemmatise(text):
    global nlp, sr
    # Implementing lemmatization
    p = re.compile('[a-zA-Z]')
    lt = []
    lem = nlp(text)
    # finding lemma for each word
    for word in lem:
        if p.findall(str(word)):
            lemma = word.lemma_.lower()
            if len(lemma) > 3:
                if str(lemma) not in sr:
                    lt.append(lemma)
    return lt


def GetDoc1and2(i, j, keys_list):
    global doc_segments
    doc1 = doc_segments[keys_list[i]]
    doc2 = doc_segments[keys_list[j]]
    print(doc1, " | vs. | ", doc2)


def get_jaccard_sim(str1, str2):
    a = set(str1.split())
    b = set(str2.split())
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c)), len(a), len(b), len(c)


def par_CS(m, n, pairs, w, C_1D, kl, doc_segments):
    global nlp, sr
    read_dictionary()
    # testCS()
    # global doc_segments
    # print(w, len(keys_list))
    for k in range(m, n):
        pair = pairs[k]
        i_j = pair.split(',')
        i = int(i_j[0])
        j = int(i_j[1])
        # print(i_j, keys_list[i], "|", keys_list[j])
        doc1 = Lemmatise(doc_segments[kl[i]])
        # doc1s = list(set(doc1))
        doc1sstr = " ".join(doc1)
        doc1str = " ".join(doc1)
        # print(doc1str)
        doc2 = Lemmatise(doc_segments[kl[j]])
        # doc2s = list(set(doc2))
        doc2sstr = " ".join(doc2)
        doc2str = " ".join(doc2)
        doc1nlp = nlp(doc1str)
        doc2nlp = nlp(doc2str)
        sim = doc1nlp.similarity(doc2nlp)
        s = round(sim, 2)
        jsim, a, b, c = get_jaccard_sim(doc1sstr, doc2sstr)
        js = round(jsim, 2)
        print(i, j, s, js)
        # if s > 0.93:
        #     jsim, a, b, c = get_jaccard_sim(doc1sstr, doc2sstr)
        #     js = round(jsim, 2)
        #     print(i_j, s, "****", js, a, b, c)
        #     print(doc1)
        #     print(doc2)
        # else:
        #     jsim, a, b, c = get_jaccard_sim(doc1sstr, doc2sstr)
        #     js = round(jsim, 2)
        #     print(i_j, s, "----", js, a, b, c)
        # GetDoc1and2(i, j, keys_list)
        # print(j, keys_list[j])


def elapsedTime(n):
    global pt
    ct = time.perf_counter()
    et = ct - pt
    print("Elapsed:", n, ":", et, " sec.")
    pt = ct


def par_compare(kl):
    """
    Function to compare Gavin Mackay's data in the CSV files
    This function sets up multi-processing.
    STEPS
        1. Create an array of the pairs of comparisons. This is in the form of a list as below, where the numbers are the indices of the URLs...
            ['0,1', '0,2', '0,3', '0,4', '1,2', '1,3', '1,4', '2,3', '2,4', '3,4']
        2. Create a flat array that works across multiple processes.
            It will hold the results from every sub-process and finally compiles them into one list
        3. Find the number of CPUs on the system.
            These many workers will be launched.
        4. Determine the chunk_size based on the length of pairs and num_workers
            Each worker will be given these many pairs to process. For example, if the number of pairs is 64 and
            there are 8 workers then each worker will get 8 pairs to process.
    :return:
    """
    n_keys = len(kl)
    n_keys = 100
    print(n_keys)
    pairs = []
    n_pairs = (n_keys * (n_keys+1) / 2) - n_keys
    print("Making the pairs:", n_pairs)
    for i in range(n_keys):
        # print(i)
        for j in range(i + 1, n_keys):
            k = str(i) + ',' + str(j)
            pairs.append(k)
    elapsedTime(161)
    # print(len(pairs))
    C_1D = mp.RawArray('d', len(pairs))  # flat version of matrix C. 'd' = number
    num_workers = mp.cpu_count()
    chunk_size = len(pairs) // num_workers + 1
    n_chunks = len(pairs) // chunk_size

    workers = []
    # Each worker gets a subset of the pairs.
    # e.g. 8 workers and 64 pairs:
    #   worker 1: From pairs[0] to pairs[7]. i.e. b = 0; e = 8
    #   worker 2: from pairs[8] to pairs[15], b = 8; e = 16, and so on.
    for w in range(n_chunks):
        b = w * chunk_size
        e = b + chunk_size
        workers.append(mp.Process(target=par_CS, args=(b, e, pairs, w, C_1D, kl, doc_segments)))
    try:
        if e:
            r = len(pairs) - e
        if r > 0:
            w += 1
            workers.append(mp.Process(target=par_CS, args=(e, len(pairs), pairs, w, C_1D, kl, doc_segments)))
    except:
        pass

    # print("num_workers,workers:", num_workers, workers)
    for w in workers:
        w.start()
    # Wait for all processes to finish
    for w in workers:
        w.join()


def read_csv():
    """
    The CSV file has source filenames (column 4) and keyword phrases (column 3)
    e.g.
    KeyPhraseLocation	KeyPhraseLocation@type	PartitionKey	RowKey	SourceFileName
    body | Edm.String | https:||qsaqutpoc...200109.doc| training program initiative | 11599293_Meeting Report - RTI Training 200109.DOC

    Several rows of keyphrases for the same source file. These are appended, with spaces, into a long text string
    and added to a global OrderedDict, 'doc_segments', using the source file name as the key. OrderedDict is used
    instead of normal dict so that the order of filenames in the CSV is maintained in the data structure. It helps
    to manually check the results.

    The Cosine Similarity will be calculated on these key phrases between pairs of source files.

    :return:
    """
    global doc_segments
    csv_filename = "Files/stlprecordassociationkeyphrases.typed.csv"
    with open(csv_filename, encoding="utf8") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            # Skip the header row
            if line_count == 0:
                line_count += 1
            else:
                line_count += 1
                try:
                    # This will give an error if the key is not yet defined.
                    # Subsequent values are appended
                    doc_segments[row[4]] = doc_segments[row[4]] + ' ' + row[3]
                except Exception as e:
                    # The first value is assigned to the key
                    doc_segments[row[4]] = row[3]


# - Functions - End -----------------------------------------------------------------
# the main:
if __name__ == '__main__':
    """
    The function, 'par_compare()', does the following:
    1. Create an array of 'pairs' of all required comparisons as e.g.
        ['0,1', '0,2', '0,3', '0,4', '1,2', '1,3', '1,4', '2,3', '2,4', '3,4', ...]
    2. Split the pairs array into chunks based on the number of CPUs used.
    """
    # Read the CSV files and create a dict of doc_segments, where the keys are the filenames
    # and the values are the key phrases concatenated together.
    read_csv()

    # Take the keys into an array
    keys_list = list(doc_segments.keys())
    par_compare(keys_list)
    et1 = time.perf_counter() - ct0
    print("Total Time: {:0.2f} sec".format(et1))
