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
from inspect import getframeinfo, stack

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
cst = 0.93 # Threshold for CS score
js0 = 0.19 # Threshold of JS
js1 = 0.15 # Low JS
js2 = 0.23 # High JS
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
    js = float(len(c) / (len(a)+len(b)-len(c)))
    js = round(js, 2)
    return js, len(a), len(b), len(c)


def classify(c, j):
    if c > cst:
        if j >= js1: return 'P' # P: Positive. CS > 0.93 and JS >= 0.15
        if j < js1: return 'FP' # FP: False Positive. CS > 0.93 and JS < 0.15
    if c <= cst:
        if j <= js0: return 'N'
        if j > js2: return 'FN'
        if j > js1: return 'UN'
    return ''


def par_CS(m, n, pairs, w, kl, segs, lock):
    global nlp, sr
    read_dictionary()
    with open("Files/nlp_parallel_csv_results.txt", "a", encoding="utf8") as f:
        for k in range(m, n):
            pair = pairs[k]
            i_j = pair.split(',')
            i = int(i_j[0])
            j = int(i_j[1])
            doc1 = Lemmatise(segs[kl[i]])
            doc1sstr = " ".join(doc1)
            doc1str = " ".join(doc1)
            doc2 = Lemmatise(segs[kl[j]])
            doc2sstr = " ".join(doc2)
            doc2str = " ".join(doc2)
            doc1nlp = nlp(doc1str)
            doc2nlp = nlp(doc2str)
            sim = doc1nlp.similarity(doc2nlp)
            cs = round(sim, 2)
            js, a, b, c = get_jaccard_sim(doc1sstr, doc2sstr)
            lock.acquire()
            # How to classify:
            # P: Positive. CS > 0.93 and JS > 0.19
            # FP: False Positive. CS > 0.93 and JS < 0.15
            # UP: Undecided Positive. CS > 0.93 and JS between 0.15 and 0.19
            # UP: Undecided Positive. CS <= 0.93 and JS > 0.23

            # N: Negative. CS <= 0.93 and JS <= 0.19
            # FN: False Negative. CS <= 0.93 and JS > 0.23
            # UN: Undecided Negative. CS <= 0.93 and JS between 0.15 and 0.19
            # UN: Undecided Negative. CS > 0.93 and JS < 0.15
            cf = classify(cs, js)
            if cs > 0.93:
                print("{}\t{}_{}\t{}\t{}\t{}\t{}\t{}\t{}".format(w, i, j, cs, a, b, c, js, cf))
                f.write("{}\t{}_{}\t{}\t{}\t{}\t{}\t{}\t\t{}\t{}\t{}\t{}\t{}\n".format(w, i, j, cs, a, b, c, js, cf, kl[i], kl[j], doc1sstr, doc2sstr))
            else:
                print("{}\t{}_{}\t{}\t{}\t{}\t{}\t{}\t{}".format(w, i, j, cs, a, b, c, js, cf))
                f.write("{}\t{}_{}\t{}\t{}\t{}\t{}\t{}\t\t{}\t{}\t{}\t{}\t{}\n".format(w, i, j, cs, a, b, c, js, cf, kl[i], kl[j], doc1sstr, doc2sstr))
            lock.release()


def elapsedTime(text):
    caller = getframeinfo(stack()[1][0])
    global pt
    ct = time.perf_counter()
    et = round((ct - pt), 2)
    print("Line {}: Time for {}: {} sec.".format(caller.lineno, text, et))
    pt = ct


def par_compare(kl, nc):
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
    pairs = []
    n_pairs = int((nc * (nc+1) / 2) - nc)
    print("No. of files: {}. Total pairs: {}".format(nc, n_pairs))
    for i in range(nc):
        # print(i)
        for j in range(i + 1, nc):
            k = str(i) + ',' + str(j)
            pairs.append(k)
    elapsedTime("Creating Pairs")
    # C_1D = mp.RawArray('d', len(pairs))  # flat version of matrix C. 'd' = number
    num_workers = mp.cpu_count()
    chunk_size = len(pairs) // num_workers + 1
    n_chunks = len(pairs) // chunk_size
    lock = mp.Lock()
    workers = []
    # Each worker gets a subset of the pairs.
    # e.g. 8 workers and 64 pairs:
    #   worker 1: From pairs[0] to pairs[7]. i.e. b = 0; e = 8
    #   worker 2: from pairs[8] to pairs[15], b = 8; e = 16, and so on.
    with open("Files/nlp_parallel_csv_results.txt", "w") as f:
        f.write("CPU\ti_j\tCS\twords_i\twords_j\tintersect\tJS\tReal\tFile1\tFile2\tDoc1\tDoc2\n")
    for w in range(n_chunks):
        b = w * chunk_size
        e = b + chunk_size
        workers.append(mp.Process(target=par_CS, args=(b, e, pairs, w, kl, doc_segments, lock)))
    try:
        if e:
            r = len(pairs) - e
        if r > 0:
            w += 1
            workers.append(mp.Process(target=par_CS, args=(e, len(pairs), pairs, w, kl, doc_segments, lock)))
    except:
        pass

    elapsedTime("Creating Workers")
    # print("num_workers,workers:", num_workers, workers)
    print("i-j\ts\ta\tb\tc\tjs")
    for w in workers:
        w.start()
    elapsedTime("Starting Workers")
    # Wait for all processes to finish
    for w in workers:
        w.join()
    elapsedTime("Running Workers")


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
                    doc_segments[row[4]] = doc_segments[row[4]] + ' ' + row[3] + ' ' + row[4]
                    # doc_segments[row[4]] = doc_segments[row[4]] + ' ' + row[3]
                except Exception as e:
                    # The first value is assigned to the key
                    doc_segments[row[4]] = row[3] + ' ' + row[4]
                    # doc_segments[row[4]] = row[3]


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
    # n_compare = len(kl)
    n_compare = 200

    # Take the keys into an array
    keys_list = list(doc_segments.keys())
    par_compare(keys_list, n_compare)
    et1 = time.perf_counter() - ct0
    print("Total Time: {:0.2f} sec".format(et1))
