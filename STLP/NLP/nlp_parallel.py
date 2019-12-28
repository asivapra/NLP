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


def CountWords(t):
    global counts
    from collections import Counter
    counts = Counter(t)


def ReadUrls(i, j):
    """
    This function reads two URLs and compare them.
    :param i: URL 1
    :param j: URL 2
    :return: Nothing is returned
    """
    url1 = urls[i]
    url2 = urls[j]
    global html1, html2, t1, t2, t1w, t2w, t1t, t2t, kw1, desc1, kw2, desc2, title1, title2
    t1 = []
    t2 = []

    # For speed, the contents fetched from the URLs are stored in files.
    # Only if the files do not exist will the URLs be called again.
    # TODO: The stored files must be purged at the end of the run to avoid mistakes.
    file1 = "Files/url" + str(i) + ".html"
    file2 = "Files/url" + str(j) + ".html"
    err = 0
    # Read the first URL or its stored file content
    try:
        with open(file1, "rb") as f:
            file_content = bytes(f.read())
            text1, title1, kw1, desc1 = text_from_html(file_content)
            # Take the lemmas of words within the text in pages. The 'text1' comes from the <body> and omits title
            t1 = Lemmatise(text1)
            # Take the lemmas from the title of the page. This is NOT used at present.
            t1t = Lemmatise(str(title1))
    except Exception as e:
        print(e)
        err = 1
    if err:
        # If stored file does not exist, then get the content from the URL and write it out in stored file.
        # Since the same URL is used in many comparisons, faster to store it and read from it for subsequent calls.
        response = urllib.request.urlopen(url1)
        html1 = response.read()
        with open(file1, 'wb') as f:
            f.write(html1)
        text1, title1, kw1, desc1 = text_from_html(html1)
        t1 = Lemmatise(text1)
        t1t = Lemmatise(str(title1))
    err = 0

    # Read the seconds URL or its stored file content
    try:
        with open(file2, "rb") as f:
            file_content = bytes(f.read())
            text2, title2, kw2, desc2 = text_from_html(file_content)
            t2 = Lemmatise(text2)
            t2t = Lemmatise(str(title2))
    except Exception as e:
        print(e)
        err = 1
    if err:
        response = urllib.request.urlopen(url2)
        html2 = response.read()
        with open(file2, 'wb') as f:
            f.write(html2)
        text2, title2, kw2, desc2 = text_from_html(html2)
        t2 = Lemmatise(text2)
        t2t = Lemmatise(str(title2))

    # Make a list of words and their counts. We will take the top 10 for further analysis
    # The word counts are made from the lemmas of 'text1'. The most popular words are compared between the URLs
    CountWords(t1)
    t1w = []
    for key, val in counts.items():
        item = str(val) + ':' + str(key.lower())
        t1w.append(item)
    t1w.sort(key=lambda f_name: int(f_name.split(':')[0]), reverse=True)
    CountWords(t2)
    t2w = []
    for key, val in counts.items():
        item = str(val) + ':' + str(key.lower())
        t2w.append(item)
    t2w.sort(key=lambda f_name: int(f_name.split(':')[0]), reverse=True)


def GetDocSegments():
    global doc1_segments, doc2_segments, n_doc1_segments, n_doc2_segments
    # Split and save the text as 100 word segments
    doc1_segments = []
    doc2_segments = []
    text = ''
    segment_size = 100
    # Split t1 and t2 into text of segment_size words
    l1 = len(t1)
    n = int(l1 / segment_size)
    for i in range(n):
        b = i * segment_size
        e = b + segment_size
        for j in range(b, e):
            text += t1[j] + ' '
        doc1_segments.append(text)
        text = ''

    l2 = len(t2)
    n = int(l2 / segment_size)
    for i in range(n):
        b = i * segment_size
        e = b + segment_size
        for j in range(b, e):
            text += t2[j] + ' '
        doc2_segments.append(text)
        text = ''

    n_doc1_segments = len(doc1_segments)
    n_doc2_segments = len(doc2_segments)


def CalculateSimilarity():
    """
    The logic of this function is confusing, but it works!
    Must try to recollect the logic. DO NOT CHANGE IT NOW.
    :return: avg
    """
    global nlp, sr
    sims = []
    k = n_urls
    if n_doc1_segments < k:
        k = n_doc1_segments
    if n_doc2_segments < k:
        k = n_doc2_segments
    if k == 0:
        return 0
    for i in range(k):
        for j in range(k):
            doc1 = nlp(doc1_segments[i])
            doc2 = nlp(doc2_segments[j])
            sim = doc1.similarity(doc2)
            sims.append(sim)
    sims.sort(reverse=True)
    tot = 0.00
    for i in range(k):
        tot += sims[i]
    avg = tot / k
    return avg


def tag_visible(element):
    # Only the visible text is to be retrieved
    if element.parent.name in ['a', 'link', 'style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def text_from_html(body):
    """
    The URL page content is processed to take the text from visible parts only.
    BeautifulSoup parses the page content
    :param body: The page content of the URL, including <head> and <body>
    :return: returns the visible text, page title, keywords and description. Of these, only the 'text' is used.
    """
    # Beautiful Soup is a Python library for pulling data out of HTML and XML files.
    # Here it parses the <body></body> content into a 'soup' data structure.
    soup = BeautifulSoup(body, 'html.parser')
    keywords = ''
    description = ''
    title = soup.title.string
    for tag in soup.find_all("meta"):
        if tag.get("name", None) == "og:title":
            title = tag.get("content", None)
            pass
        if tag.get("name", None) == "keywords":
            keywords = tag.get("content", None)
            pass

        if tag.get("name", None) == "description":
            description = tag.get("content", None)
            pass

    texts = soup.findAll(text=True)
    # Omit the text from these HTML elements: ['a', 'link', 'style', 'script', 'head', 'title', 'meta', '[document]']
    visible_texts = filter(tag_visible, texts)
    return u" ".join(t.strip() for t in visible_texts), title, keywords, description


def par_ReadUrls(m, n, pairs, w, C_1D):
    """
    :param m: start of the range of pairs to be processed
    :param n: end of the range
    :param pairs: the list that holds the pairs of URLs to be compared.
                e.g. ['0,1', '0,2', '0,3', '0,4', '1,2', '1,3', '1,4', '2,3', '2,4', '3,4']
    :param w: the processor ID (0 to n)
    :param C_1D: flat array to hold the values. It is accessible across the processes
    :return: no value is returned
    Method:
        1. Each worker gets a range of pairs to process. These are done sequentially
        2. Split the pairs[k] into two indices. e.g. '0,1' to 0 and 1. These are the two URLs to be compared
        3. Send these to 'ReadUrls' to get the file contents
        4. Call 'GetDocSegments' to split the content into smaller chunks
        5. Call 'CalculateSimilarity' to calculate the cosine similarity
        6. Record the value in C_1D array.
    """
    read_dictionary()
    ct1 = time.perf_counter()
    for k in range(m, n):
        pair = pairs[k]
        i_j = pair.split(',')
        i = int(i_j[0])
        j = int(i_j[1])
        ReadUrls(i, j)
        GetDocSegments()
        sc = CalculateSimilarity()
        s = round(sc, 2)
        C_1D[k] = s
    ct2 = time.perf_counter()
    et1 = ct2 - ct1
    print("Worker {} Finished comparisons: {:0.2f} sec".format(w, et1))


def par_compare():
    """
    This function sets up multi-processing.
    STEPS
        1. Create an array of the pairs of comparisons. This is in the form of a list as below, where the numbers are the indices of the URLs...
            ['0,1', '0,2', '0,3', '0,4', '1,2', '1,3', '1,4', '2,3', '2,4', '3,4']
        2. Create a flat array that works across multiple processes.
            It will hold the results from every sub-process and finally compiles them into one list
        3. Find the number of CPUs on the system.
            These many workers will be launched.
        4. Determine the chunk_size based on the length of pairs and num_workers
            Each worker will be given these many pairs to process. For example, if the number of pairs is 64 and there are 8 workers
            then each worker will get 8 pairs to process.

    """
    pairs = []
    for i in range(n_urls):
        for j in range(i + 1, n_urls):
            k = str(i) + ',' + str(j)
            pairs.append(k)
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
        workers.append(mp.Process(target=par_ReadUrls, args=(b, e, pairs, w, C_1D)))
    try:
        if e:
            r = len(pairs) - e
        if r > 0:
            w += 1
            workers.append(mp.Process(target=par_ReadUrls, args=(e, len(pairs), pairs, w, C_1D)))
    except:
        pass

    print("num_workers,workers:", num_workers, workers)
    # As many processes are started as there are workers
    # num_workers,workers: 8 [<Process(Process-1, initial)>, <Process(Process-2, initial)>,...]
    for w in workers:
        w.start()
    # Wait for all processes to finish
    for w in workers:
        w.join()
    k = 0

    # nn = n_urls
    # Initialise a list of n_urls*n_urls with 0s to hold the cosine similarity scores.
    matrix = [[0] * n_urls for i in range(n_urls)]

    # Print the top row values as we populate the matrix
    print("#,", end='')
    for i in range(n_urls):
        print("{},".format(i), end='')

        # C_1D is a 1-dimensional array that holds the values. It gets the values from all workers together
        # Split it into rows and columns in matrix
        for j in range(i+1, n_urls):
            matrix[i][j] = C_1D[k]
            k += 1
    print('')
    for i in range(n_urls):
        print("{},".format(i),end='')
        for j in range(n_urls):
            print("{},".format(matrix[i][j]), end='')
        print('')

    # Write the results into a file. This will be used to compare against a reference matrix for expected matches
    with open('Files/matrix.txt', 'w') as f:
        for i in range(n_urls):
            for j in range(n_urls):
                f.write("{},".format(str(matrix[i][j])))
            f.write("\n")

    # Read in the ref_table for expected matches
    ref_table = [[0] * 30 for i in range(30)]
    j = 0
    with open('ref_table.txt', 'r') as f:
        lines = f.readlines()
        for i in range(len(lines)):
            lines[i].rstrip('\n\r')
            la = lines[i].split('\t')
            ref_table[j] = la
            j += 1
    # print(ref_table)


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


def testCS():
    global nlp, sr
    s1 = ['activity', 'appropriate', 'capacity', 'building', 'strategy', 'assessment', 'framework', 'assessment',
     'framework', 'implementation', 'baseline', 'datum', 'capability', 'maturity', 'level', 'change', 'management',
     'objective', 'compliance', 'performance', 'model', 'compliance', 'performance', 'monitoring', 'consultation',
     'brief', 'diverse', 'recordkeeping', 'function', 'document', 'control', 'sheet', 'information', 'management',
     'policy', 'information', 'recordkeepe', 'training', 'project', 'training', 'parameter', 'mccauley', 'level',
     'risk', 'assessment', 'framework', 'mccauley', 'phone', 'national', 'information', 'management', 'skill', 'summit',
     'outcome', 'delivery', 'delay', 'policy', 'officer', 'potential', 'user', 'group', 'principal', 'policy',
     'officer', 'project', 'plan', 'project', 'sponsor', 'public', 'sector', 'queensland', 'public', 'queensland',
     'public', 'authority', 'queensland', 'state', 'archives', 'queensland', 'state', 'archives&|39', 'recordkeepe',
     'assessment', 'framework', 'recordkeeping', 'assessment', 'framework', 'development', 'recordkeeping',
     'assessment', 'framework', 'implementation', 'recordkeepe', 'assessment', 'framework', 'implementation', 'plan',
     'recordkeeping', 'assessment', 'framework', 'outcome', 'recordkeeping', 'assessment', 'framework', 'reference',
     'group', 'recordkeepe', 'awareness', 'program', 'recordkeepe', 'compliance', 'performance', 'model', 'recordkeepe',
     'training', 'research', 'unit', 'version', 'history']
    s2 = ['activity', 'appropriate', 'capacity', 'building', 'strategy', 'assessment', 'activity', 'assessment', 'framework',
     'assessment', 'framework', 'implementation', 'assessment', 'framework', 'sustainability', 'baseline', 'datum',
     'briefing', 'note', 'prepare', 'capability', 'maturity', 'level', 'change', 'management', 'objective',
     'compliance', 'performance', 'model', 'compliance', 'performance', 'monitoring', 'consultation', 'brief', 'deputy',
     'director', 'diverse', 'recordkeeping', 'function', 'document', 'control', 'sheet', 'financial', 'implication',
     'implement', 'compliance', 'performance', 'monitor', 'information', 'management', 'policy', 'information',
     'recordkeepe', 'training', 'project', 'focus', 'area', 'training', 'parameter', 'mccauley', 'level', 'risk',
     'assessment', 'framework', 'mccauley', 'phone', 'national', 'information', 'management', 'skill', 'summit',
     'ongoing', 'administrative', 'outcome', 'delivery', 'delay', 'performance', 'model', 'peer', 'policy', 'officer',
     'principal', 'policy', 'officer', 'project', 'manager', 'project', 'plan', 'project', 'sponsor', 'queensland',
     'public', 'queensland', 'public', 'authorities&|39', 'queensland', 'public', 'authority', 'queensland', 'state',
     'archives', 'queensland', 'state', 'archives&|39', 'recordkeepe', 'assessment', 'framework', 'recordkeeping',
     'assessment', 'framework', 'development', 'recordkeeping', 'assessment', 'framework', 'implementation',
     'recordkeepe', 'assessment', 'framework', 'implementation', 'plan', 'recordkeeping', 'assessment', 'framework',
     'outcome', 'recordkeeping', 'assessment', 'framework', 'reference', 'group', 'recordkeepe', 'awareness', 'program',
     'recordkeepe', 'compliance', 'performance', 'model', 'research', 'unit', 'state', 'archivist', 'assessment',
     'blah1', 'blah2', 'blah3', 'blah4', 'blah5', 'blah6', 'blah7', 'blah8', 'blah9', 'blah10']
    doc1str = " ".join(s1)
    doc2str = " ".join(s2)
    doc1nlp = nlp(doc1str)
    doc2nlp = nlp(doc2str)
    sim = doc1nlp.similarity(doc2nlp)
    s = round(sim, 2)
    print(s)


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


def par_compare_GM_data(kl):
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
    elapsedTime(452)
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
            if line_count == 0:
                # print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                line_count += 1
                try:
                    # This will give an error if the key is not yet defined.
                    # Subsequent values are appended
                    doc_segments[row[4]] = doc_segments[row[4]] + ' ' + row[3]
                except Exception as e:
                    # print(e)
                    # The first value is assigned to the key
                    doc_segments[row[4]] = row[3]


# - Functions - End -----------------------------------------------------------------
# Globals - Begin


urls = [
    "https://www.ands.org.au/working-with-data/metadata/geospatial-data-and-metadata",
    "https://www.ands.org.au/guides/geospatial",
    "https://www.safe.com/what-is/spatial-data/",
    "https://gisgeography.com/what-is-geodata-geospatial-data/",
    "https://www.mathworks.com/help/map/what-is-geospatial-data.html",
    "https://www.veris.com.au/our-services/geospatial-data-management/",

    "https://searchbusinessanalytics.techtarget.com/definition/natural-language-processing-NLP",
    "https://algorithmia.com/blog/introduction-natural-language-processing-nlp",
    "https://www.coursera.org/learn/language-processing",
    "https://en.wikipedia.org/wiki/Natural_language_processing",
    "https://www.forbes.com/sites/bernardmarr/2019/06/03/5-amazing-examples-of-natural-language-processing-nlp-in-practice/",
    "https://www.sas.com/en_au/insights/analytics/what-is-natural-language-processing-nlp.html",

    "https://aiatsis.gov.au/explore/articles/indigenous-australian-languages",
    "https://en.wikipedia.org/wiki/Australian_Aboriginal_languages",
    "https://www.commonground.org.au/learn/indigenous-languages-avoiding-a-silent-future",
    "http://theconversation.com/the-state-of-australias-indigenous-languages-and-how-we-can-help-people-speak-them-more-often-109662",
    "https://www.clc.org.au/articles/info/aboriginal-languages/",
    "https://www.britannica.com/topic/Australian-Aboriginal-languages",

    "https://en.wikipedia.org/wiki/Human_genome",
    "https://en.wikipedia.org/wiki/Human_Genome_Project",
    "https://www.nature.com/scitable/topicpage/dna-sequencing-technologies-key-to-the-human-828/",
    "https://www.britannica.com/event/Human-Genome-Project/Advances-based-on-the-HGP",
    "https://www.genome.gov/human-genome-project/Completion-FAQ",
    "https://www.yourgenome.org/stories/how-is-the-completed-human-genome-sequence-being-used",

    "https://en.wikipedia.org/wiki/Machine_learning",
    "https://www.sas.com/en_au/insights/analytics/machine-learning.html",
    "https://en.wikipedia.org/wiki/London",
    "https://en.wikipedia.org/wiki/England",
    "https://en.wikipedia.org/wiki/Titanic_(1997_film)",
    "https://en.wikipedia.org/wiki/Titanic_(1953_film)",
]
n_urls = len(urls)
n_urls = 3
# Globals - End
#
# -----------------------------------------------------------------------------------------------------------------
# Call the functions
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
    par_compare_GM_data(keys_list)
    et1 = time.perf_counter() - ct0
    print("Total Time: {:0.2f} sec".format(et1))
