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
import sys

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
ct0 = 0  # Start time
pt = time.perf_counter()
doc_segments = OrderedDict()
skip_pair = 0  # Skip a pair if its members are in either 'group' or 'member' list.


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


# def get_jaccard_sim(str1, str2):  # Not Used
#     a = set(str1.split())
#     b = set(str2.split())
#     c = a.intersection(b)
#     js = float(len(c) / (len(a)+len(b)-len(c)))
#     js = round(js, 2)
#     return js, len(a), len(b), len(c)


# def classify(c, j):  # Not Used
#     if c > cst:
#         if j >= js1: return 'P' # P: Positive. CS > 0.93 and JS >= 0.15
#         if j < js1: return 'FP' # FP: False Positive. CS > 0.93 and JS < 0.15
#     if c <= cst:
#         if j <= js0: return 'N'
#         if j > js2: return 'FN'
#         if j > js1: return 'UN'
#     return ''


def par_CS(m, n, pairs, w, kl, segs, lock, ngid, i_array, j_array, ij_array):
    global nlp, sr
    m_n = int(n-m)
    read_dictionary()
    print(w, ':', m_n, pairs[m:n])
    with open("Files/nlp_parallel_csv_results.txt", "a", encoding="utf8") as f:
        for k in range(m, n):
            pair = pairs[k]
            m_n -= 1  # Decrement the count for info on progress
            i_j = pair.split(',')
            i = int(i_j[0])
            j = int(i_j[1])

            # The second file is compared only if it has not been positive with another.
            # i.e. if 0 is positive with 1 and 2, then 1 and will not be compared each other.
            # Instead, 0 will be added to a group and 1 and 2 will be its members.
            if skip_pair:
                if i is not 0 and i in j_array:
                    continue
                if j is not 0 and j in j_array:
                    continue
            doc1 = Lemmatise(segs[kl[i]])
            # doc1sstr = " ".join(doc1)
            doc1str = " ".join(doc1)
            doc2 = Lemmatise(segs[kl[j]])
            # doc2sstr = " ".join(doc2)
            doc2str = " ".join(doc2)
            doc1nlp = nlp(doc1str)
            doc2nlp = nlp(doc2str)
            sim = doc1nlp.similarity(doc2nlp)
            cs = round(sim, 2)
            # js, a, b, c = get_jaccard_sim(doc1sstr, doc2sstr)
            lock.acquire()

            if cs > cst:
                if i not in i_array:
                    for kk in range(ngid):
                        if not i_array[kk]:
                            i_array[kk] = i
                            break
                ij = float(str(i) + "." + str(j))
                if j not in j_array:
                    for kk in range(ngid):
                        if not ij_array[kk]:
                            ij_array[kk] = ij
                            break
                if j not in j_array:
                    for kk in range(ngid):
                        if not j_array[kk]:
                            j_array[kk] = j
                            break
                print("{}: {} : {}_{}\t{}\t ******\t{}\t{}".format(w, m_n, i, j, cs, kl[i], kl[j]))
                f.write("{}_{}\t{}\t{}\t{}\n".format(i, j, cs, kl[i], kl[j]))
            else:
                # print("{}: {} : {}_{}\t{}\t{}\t{}".format(w, m_n, i, j, cs, kl[i], kl[j]))
                print("{}: {} : {}_{}\t{}".format(w, m_n, i, j, cs))
                f.write("{}_{}\t{}\t{}\t{}\n".format(i, j, cs, kl[i], kl[j]))
                pass
            lock.release()


def elapsedTime(text):
    caller = getframeinfo(stack()[1][0])
    global pt
    ct = time.perf_counter()
    et = round((ct - pt), 2)
    print("Line {}: Time for {}: {} sec.".format(caller.lineno, text, et))
    pt = ct


def p(s1='', s2=''):
    """
    Print the line number and sent text strings. Useful for debugging.
    :param s1: If empty, print "Debug marker"
    :param s2: Can be empty
    :return: None
    """
    if s1 is '':
        s1 = 'Debug marker'
    caller = getframeinfo(stack()[1][0])
    print("Line {}:".format(caller.lineno), end='')
    print(s1, s2)


def par_compare_groups(kl, nb, ne):
    group_ids = []
    member_ids = []
    file1 = []
    file2 = []
    csv_filename = "Files/nlp_parallel_csv_groups.csv"
    with open(csv_filename, encoding="utf8") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            # Skip the header row
            if line_count == 0:
                # Add a blank line as first item.
                # It is required for i and j not to be 0 in 'par_CS' and adding to i_array and j_array will work.
                # doc_segments['______'] = ''
                line_count += 1
            else:
                line_count += 1
                try:
                    group_ids.append(row[0])
                    member_ids.append(row[1].strip())
                    file1.append(row[2])
                    file2.append(row[3])
                except Exception as e:
                    pass
    # print(group_ids)
    # print(member_ids)
    # print(file1)
    # print(file2)
    pairs = []
    nc = ne - nb  # Total number of lines
    for i in range(len(group_ids)):
        ig = group_ids[i]
        for j in range(nb, ne):
            k = str(ig) + ',' + str(j)
            pairs.append(k)

    print("pairs:", len(pairs))
    elapsedTime("Creating Pairs")
    i_array = mp.RawArray('i', nc)  # flat version of matrix C. 'i' = integer, 'd' = double, 'f' = float
    j_array = mp.RawArray('i', nc)
    ij_array = mp.RawArray('d', nc)
    # pairIDs = mp.RawArray('f', nc)
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
        f.write("i_j\tCS\tFile1\tFile2\n")
    for w in range(n_chunks):
        b = w * chunk_size
        e = b + chunk_size
        workers.append(mp.Process(target=par_CS, args=(b, e, pairs, w, kl, doc_segments, lock, nc, i_array, j_array, ij_array)))
    try:
        if e:
            r = len(pairs) - e
        if r > 0:
            w += 1
            workers.append(mp.Process(target=par_CS, args=(e, len(pairs), pairs, w, kl, doc_segments, lock, nc, i_array, j_array, ij_array)))
    except:
        pass

    elapsedTime("Creating Workers")
    # print("i-j\ts\ta\tb\tc\tjs")
    for w in workers:
        w.start()
    elapsedTime("Starting Workers")
    # Wait for all processes to finish
    for w in workers:
        w.join()
    elapsedTime("Running Workers")



def par_compare(kl, nb, ne):
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
    global i_array, j_array, ij_array
    pairs = []
    nc = ne - nb  # Total number of lines
    n_pairs = int((nc * (nc+1) / 2) - nc)
    print("No. of files: {}. Total pairs: {}".format(nc, n_pairs))

    # Ignore the first line in kl
    for i in range(nb+1, ne+1):
        # print(i)
        for j in range(i + 1, ne+1):
            k = str(i) + ',' + str(j)
            pairs.append(k)
    elapsedTime("Creating Pairs")
    i_array = mp.RawArray('i', nc)  # flat version of matrix C. 'i' = integer, 'd' = double, 'f' = float
    j_array = mp.RawArray('i', nc)
    ij_array = mp.RawArray('d', nc)
    # pairIDs = mp.RawArray('f', nc)
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
        f.write("i_j\tCS\tFile1\tFile2\n")
    for w in range(n_chunks):
        b = w * chunk_size
        e = b + chunk_size
        workers.append(mp.Process(target=par_CS, args=(b, e, pairs, w, kl, doc_segments, lock, nc, i_array, j_array, ij_array)))
    try:
        if e:
            r = len(pairs) - e
        if r > 0:
            w += 1
            workers.append(mp.Process(target=par_CS, args=(e, len(pairs), pairs, w, kl, doc_segments, lock, nc, i_array, j_array, ij_array)))
    except:
        pass

    elapsedTime("Creating Workers")
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
                # Add a blank line as first item.
                # It is required for i and j not to be 0 in 'par_CS' and adding to i_array and j_array will work.
                doc_segments['______'] = ''
                line_count += 1
            else:
                line_count += 1
                try:
                    # This will give an error if the key is not yet defined. If so, the except block will add a new key.
                    # Subsequent values are appended
                    doc_segments[row[4]] = doc_segments[row[4]] + ' ' + row[3] + ' ' + row[4]
                except Exception as e:
                    # The first value is assigned to the key
                    doc_segments[row[4]] = row[3] + ' ' + row[4]
    elapsedTime("Reading the CSV")


def count_ungrouped_items(i, j, nb, ne):
    n = 0
    # nc = ne - nb  # Total number of lines
    print(i[:])
    print(j[:])
    for k in range(nb, ne):
        if k not in i and k not in j:
            n += 1
            print(n, k)

# - Functions - End -----------------------------------------------------------------
# the main:
if __name__ == '__main__':
    """
    The function, 'par_compare()', does the following:
    1. Create an array of 'pairs' of all required comparisons as e.g.
        ['0,1', '0,2', '0,3', '0,4', '1,2', '1,3', '1,4', '2,3', '2,4', '3,4', ...]
    2. Split the pairs array into chunks based on the number of CPUs used.
    """
    # Temporary test functions. Will exit after calling the function
    # sys.ext()
    # ------------------------
    # Read the CSV files and create a dict of doc_segments, where the keys are the filenames
    # and the values are the key phrases concatenated together.
    read_csv()
    # Take the keys into an array
    keys_list = list(doc_segments.keys())

    # nc = len(keys_list)
    nb = 400
    ne = 420

    # Mode of operation: pair_wise = compare all pairs to get groups
    # group_wise: compare linearly with existing groups
    pair_wise = True
    group_wise = False
    if pair_wise:
        # group_wise = False
        # Do pairwise comparison of  nb to ne lines in the CSV file
        par_compare(keys_list, nb, ne)
        print(i_array[:])
        print(j_array[:])
        print(ij_array[:])
        # Make into groups
        with open("Files/nlp_parallel_csv_groups.csv", "w", encoding="utf8") as f:
            f.write("Groups\tMembers\n")
            iva = [i for i in i_array]
            iva.sort()
            for i in range(len(iva)):
                iv = iva[i]
                fl2 = ""  # List of member files
                if iv:
                    print("{}:\t".format(iv), end='')
                    f.write("{}\t".format(iv))
                    for j in range(len(ij_array)):
                        jv = str(ij_array[j]).split(".")
                        if jv[1] and int(jv[0]) == iv:
                            print("{} ".format(jv[1]), end='')
                            f.write("{} ".format(jv[1]))
                            fl2 += keys_list[int(jv[1])] + ";"
                    print("")
                    f.write("\t\t\t{}\t{}\n".format(keys_list[iv], fl2)) # Write the group file and members
        count_ungrouped_items(i_array, j_array, nb, ne)  # Count and list the lines not included in any group

    elif group_wise:
        # pair_wise = False
        # Do one-to-one comparision of n0 to n_compare lines against the reference group
        par_compare_groups(keys_list, nb, ne)

    et1 = time.perf_counter() - ct0
    print("Total Time: {:0.2f} sec".format(et1))
