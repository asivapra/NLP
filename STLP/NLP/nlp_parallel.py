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
import warnings
# warnings.filterwarnings('ignore')
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

ct0 = time.perf_counter()
print("14:Starting to read dictionary and stopwords")
nlp = spacy.load("en_core_web_md")
sr = stopwords.words('english')
et = time.perf_counter() - ct0
print("18:Finished reading: {:0.2f} sec".format(et))


def read_words():
    """
    This function is not used
    :return:
    """
    global nlp, sr
    print("Reading in words and stopwords")
    # Load only once
    try:
        if nlp:
            pass
    except:
        nlp = spacy.load("en_core_web_md")
        pass

    # Load only once
    try:
        if sr[0]:
            pass
    except:
        sr = stopwords.words('english')
        pass
    print("Finished reading in words and stopwords")


# ------Functions
def tokenize(text):
    t0 = []
    # Split the text into words separated by any of these chars
    p = re.compile('[-_\", {};:=?\[\]\(\)\'.]')

    # Get the word list into an array, t
    t = p.split(text)
    lt = len(t)

    # Add the 4+ chars words into an array, t1. Discard the smaller words
    for i in range(lt):
        w = t[i]
        lw = len(w)
        if lw > 3:
            t0.append(w)
    return t0


def Lemmatise(text):
    global nlp,sr
    # Implementing lemmatization
    p = re.compile('[a-zA-Z]')
    lt = []
    lem = nlp(text)
    # finding lemma for each word
    for word in lem:
        if (p.findall(str(word))):
            lemma = word.lemma_.lower()
            if (len(lemma) > 3):
                if str(lemma) not in (sr):
                    lt.append(lemma)
    return lt


def CountWords(t):
    global counts
    from collections import Counter
    counts = Counter(t)


def get_and_write_urls(nurls):
    # get all URLs and write to a local file. Once only
    for i in range(nurls):
        url = urls[i]
        file = "Files/url" + str(i) + ".html"
        print(file)
        response = urllib.request.urlopen(url)
        html = response.read()
        with open(file, 'wb') as f:
            f.write(html)


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
            filecontent = bytes(f.read())
            text1, title1, kw1, desc1 = text_from_html(filecontent,file1)
            print(file1, text1)
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
        # print('Reading from URL:')
        response = urllib.request.urlopen(url1)
        html1 = response.read()
        with open(file1, 'wb') as f:
            f.write(html1)
        text1, title1, kw1, desc1 = text_from_html(html1,url1)
        t1 = Lemmatise(text1)
        t1t = Lemmatise(str(title1))
    err = 0

    # Read the seconds URL or its stored file content
    try:
        with open(file2, "rb") as f:
            filecontent = bytes(f.read())
            text2, title2, kw2, desc2 = text_from_html(filecontent,file2)
            print(file2, text2)
            t2 = Lemmatise(text2)
            t2t = Lemmatise(str(title2))
            # print('Reading from file 2:', title2)
    except Exception as e:
        print(e)
        err = 1
    if err:
        response = urllib.request.urlopen(url2)
        html2 = response.read()
        with open(file2, 'wb') as f:
             f.write(html2)
        text2, title2, kw2, desc2 = text_from_html(html2,url2)
        t2 = Lemmatise(text2)
        t2t = Lemmatise(str(title2))

    # Make a list of words and their counts. We will take the top 10 for further analysis
    # The word counts are made from the lemmas of 'text1'. The most popular words are compared between the URLs
    CountWords(t1)
    t1w = []
    for key, val in counts.items():
        item = str(val) + ':' + str(key.lower())
        t1w.append(item)
    t1w.sort(key=lambda fname: int(fname.split(':')[0]), reverse=True)
    CountWords(t2)
    t2w = []
    for key, val in counts.items():
        item = str(val) + ':' + str(key.lower())
        t2w.append(item)
    t2w.sort(key=lambda fname: int(fname.split(':')[0]), reverse=True)


def GetDocSegments():
    global doc1_segs, doc2_segs, l, m
    # Split and save the text as 100 word segments
    doc1_segs = []
    doc2_segs = []
    text = ''
    # Split t1 and t2 into text of 100 words
    l = len(t1)
    n = int(l / 100)
    for i in range(n):
        b = i * 100
        e = b + 100
        for j in range(b, e):
            text += t1[j] + ' '
        doc1_segs.append(text)
        text = ''

    l = len(t2)
    n = int(l / 100)
    for i in range(n):
        b = i * 100
        e = b + 100
        for j in range(b, e):
            text += t2[j] + ' '
        doc2_segs.append(text)
        text = ''

    l = len(doc1_segs)
    m = len(doc2_segs)


def CalculateSimilarity(i, j, nurls):
#    sim = 0.00
    sims = []
    tot = 0.00
    k = nurls
    if (l < k):
        k = l
    if (m < k):
        k = m
    if (k == 0):
        return 0
    for i in range(k):
        for j in range(k):
            doc1 = nlp(doc1_segs[i])
            doc2 = nlp(doc2_segs[j])
            sim = doc1.similarity(doc2)
            sims.append(sim)
    sims.sort(reverse=True)
    for i in range(k):
        tot += sims[i]
    avg = tot / k
    return avg


def CalculateWordFrequency():
    tw1f = []
    tw2f = []
    # ------------t1--------------
#    lw = len(t1w)
    tot = 0
    # Count the total number of words
    for i in range(0, 3):
        fields = t1w[i].split(":")
        tot += int(fields[0])
    # Take the percentage of top 10 words in t1w
    for i in range(0, 3):
        fields = t1w[i].split(":")
        c = int(fields[0])
        w = fields[1]
        f = str(round(float(c / tot * 100), 2))
        fw = f + ":" + w
        tw1f.append(fw)
    # ------------t2--------------
#    lw = len(t2w)
    tot = 0
    # Count the total number of words
    for i in range(0, 3):
        fields = t2w[i].split(":")
        tot += int(fields[0])
    for i in range(0, 3):
        fields = t2w[i].split(":")
        c = int(fields[0])
        w = fields[1]
        f = str(round(float(c / tot * 100), 2))
#        fw = f + ":" + w
        tw2f.append(w)
    # Compare the two to get the word frequency
    lw1 = len(tw1f)
    lw2 = len(tw2f)
    mf = 0.00
    tf = 0.00
    for i in range(lw2):
        w2 = tw2f[i]
        tf = 0.00
        for j in range(lw1):
            fields = tw1f[j].split(":")
            f1 = float(fields[0])
            w1 = fields[1]
            tf += f1
            if w1 == w2:
                mf += f1
    freq = round(mf / tf, 2)
    return freq


def tag_visible(element):
    # Only the visible text is to be retrieved
    if element.parent.name in ['a', 'link', 'style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def text_from_html(body,src):
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


def SimilarityScore(t1, t2):
    doc1 = nlp(t1)
    doc2 = nlp(t2)
    sim = doc1.similarity(doc2)
    return sim


def Domain_match(url1, url2):
    ldv = 0.00
    f1 = url1.split('/')
    d1 = f1[2].replace('www.', '')
    f2 = url2.split('/')
    d2 = f2[2].replace('www.', '')
    if d1 == d2:
        ldv = 1.0
    return ldv


def OneComparison():
    # Read the HTML contents and lemmatise into two arrays, 't1' and 't2'
    i = 0
    j = 2
    ReadUrls(i, j)
#    sk = 0.00
    st = SimilarityScore(str(title1), str(title2))
    sk = SimilarityScore(kw1, kw2)
    sd = SimilarityScore(desc1, desc2)
    print(title1, '|', kw1, '|', desc1, '|', st, '|', sk, '|', sd, '\n')
    print(title2, '|', kw2, '|', desc2, '|', st, '|', sk, '|', sd, '\n')


def one_pair(i, j):
    print("i,j:",i,j)
    ReadUrls(i, j)
    st = SimilarityScore(str(title1), str(title2))
    sk = SimilarityScore(kw1, kw2)
    sd = SimilarityScore(desc1, desc2)
    dv = Domain_match(urls[i], urls[j])
    GetDocSegments()
    sc = CalculateSimilarity(i, j, nurls)
    s = round(sc, 2)
    matrix[i][j] = s
    print(i, j, s)


def MultipleComparisons():
    global o
    h = "#,"
    o = ""
    for i in range(nurls):
        h += str(i) + ','

    def AddTabs(i):
        global o
        for j in range(i):
            o += ','

    for i in range(nurls):
        o = str(i)
        AddTabs(i + 1)
        for j in range(i, nurls):
            ReadUrls(i, j)
            st = SimilarityScore(str(title1), str(title2))
            sk = SimilarityScore(kw1, kw2)
            sd = SimilarityScore(desc1, desc2)
            #        tv = Title_Match(t1t, t2t) # If any word in titles match, tf=0.1
            dv = Domain_match(urls[i], urls[j])
            GetDocSegments()
            sc = CalculateSimilarity(i, j, nurls)
            wf = CalculateWordFrequency()
            #        s = round((sc + wf + tv + dv),2)
            #        s = round((sc + wf + dv + st + sk + sd),2)
            s = round(sc, 2)
            o += str(s) + ','
        #    o += '\n'
        print(o)


def sequential():
    """
    This is to do the calculations sequentially. It is NOT being used anymore and its functionality may be broken.
    :return:
    """
    for i in range(nurls):
        for j in range(i + 1, nurls):
            one_pair(i, j)
    print(matrix)

    # Print the matrix out for Excel as e.g...
    # #,1,2,
    # 0,0.97,0.95,
    # 1,,0.96,
    # 2,,,,

    print("{},".format('#'), end='')
    for i in range(1, nurls):
        print("{},".format(i), end='')
    print('')
    for i in range(nurls):
        print("{}".format(i), end='')
        for j in range(nurls):
            v = str(matrix[i][j])
            if v == ' ':
                v = ''
            print("{},".format(v), end='')
        print('')


def par_ReadUrls(m,n,pairs,w,C_1D):
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

    ct1 = time.perf_counter()
    for k in range(m,n):
        pair = pairs[k]
        i_j = pair.split(',')
        i = int(i_j[0])
        j = int(i_j[1])
        ReadUrls(i, j)
        GetDocSegments()
        sc = CalculateSimilarity(i, j, nurls)
        s = round(sc, 2)
        C_1D[k] = s
    et1 = time.perf_counter() - ct1 - ct0
    print("419:Worker{} Finished comparisons: {:0.2f} sec".format(w,et1))


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
    for i in range(nurls):
        for j in range(i + 1, nurls):
            k = str(i) + ',' + str(j)
            pairs.append(k)
    print(len(pairs))
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

    nn = nurls
    # Initialise a list of nurls*nurls with 0s to hold the cosine similarity scores.
    matrix = [[0] * nurls for i in range(nn)]

    # Print the top row values as we populate the matrix
    print("#,", end='')
    for i in range(nn):
        print("{},".format(i),end='')

        # C_1D is a 1-dimensional array that holds the values. It gets the values from all workers together
        # Split it into rows and columns in matrix
        for j in range(i+1,nn):
            matrix[i][j] = C_1D[k]
            k += 1
    print('')
    for i in range(nn):
        print("{},".format(i),end='')
        for j in range(nn):
            print("{},".format(matrix[i][j]), end='')
        print('')

    # Write the results into a file. This will be used to compare against a reference matrix for expected matches
    with open('Files/matrix.txt', 'w') as f:
        for i in range(nurls):
            for j in range(nurls):
                f.write("{},".format(str(matrix[i][j])))
            f.write("\n")

    # Read in the ref_table for expected matches
    ref_table = [[0] * 30 for i in range(30)]
    j = 0
    with open('ref_table.txt','r') as f:
        lines = f.readlines()
        for i in range(len(lines)):
            lines[i].rstrip('\n\r')
            la = lines[i].split('\t')
            print(len(la))
            ref_table[j] = la
            j += 1
    print(ref_table)
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
nurls = len(urls)
nurls = 3
# Globals - End
#
# -----------------------------------------------------------------------------------------------------------------
# Call the functions
#read_words()
# OneComparison()
# MultipleComparisons()
#sequential()
# get_and_write_urls(nurls)
if __name__ == '__main__':
    """
    The function, 'par_compare()', does the following:
    1. Create an array of 'pairs' of all required comparisons as e.g.
        ['0,1', '0,2', '0,3', '0,4', '1,2', '1,3', '1,4', '2,3', '2,4', '3,4', ...]
    2. Split the pairs array into chunks based on the number of CPUs used.
    """
    par_compare()
    etime = time.perf_counter() - ct0
    print("Line 574:Total Time: {:0.2f} sec".format(etime))
