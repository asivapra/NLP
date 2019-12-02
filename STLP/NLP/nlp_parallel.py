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
warnings.filterwarnings('ignore')
ct0 = time.perf_counter()
print("14:Starting to read dictionary and stopwords")
nlp = spacy.load("en_core_web_md")
sr = stopwords.words('english')
et = time.perf_counter() - ct0
print("18:Finished reading: {:0.2f} sec".format(et))


def read_words():
    global nlp, sr
    print("Reading in words and stopwords")
    # Load only once
    try:
        if (nlp):
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
    # finding lemma for each wordx
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


def ReadUrls(i, j):
    url1 = urls[i]
    url2 = urls[j]
    global html1, html2, t1, t2, t1w, t2w, t1t, t2t, kw1, desc1, kw2, desc2, title1, title2
    t1 = []
    t2 = []
    response = urllib.request.urlopen(url1)
    html1 = response.read()
    response = urllib.request.urlopen(url2)
    html2 = response.read()
    text1, title1, kw1, desc1 = text_from_html(html1)
    t1 = Lemmatise(text1)
    t1t = Lemmatise(str(title1))
    text2, title2, kw2, desc2 = text_from_html(html2)
    t2 = Lemmatise(text2)
    t2t = Lemmatise(str(title2))

    # Make a list of words and their counts. We will take the top 10 for further analysis
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


def text_from_html(body):
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
#    print(matrix)
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
    ct1 = time.perf_counter()
    # print("360:Worker{} Starting to process the comparisons:{},{}".format(w,m,n))
    for k in range(m,n):
        pair = pairs[k]
        i_j = pair.split(',')
        i = int(i_j[0])
        j = int(i_j[1])
        ReadUrls(i, j)
        st = SimilarityScore(str(title1), str(title2))
        sk = SimilarityScore(kw1, kw2)
        sd = SimilarityScore(desc1, desc2)
        dv = Domain_match(urls[i], urls[j])
        GetDocSegments()
        sc = CalculateSimilarity(i, j, nurls)
        s = round(sc, 2)
        C_1D[k] = s
    et1 = time.perf_counter() - ct1 - ct0
    print("379:Worker{} Finished comparisons: {:0.2f} sec".format(w,et1))


def par_compare():
    pairs = []
    for i in range(nurls):
        for j in range(i + 1, nurls):
            k = str(i) + ',' + str(j)
            pairs.append(k)
    C_1D = mp.RawArray('d', len(pairs))  # flat version of matrix C. 'd' = number
    num_workers = mp.cpu_count()
    chunk_size = len(pairs) // num_workers + 1
    n_chunks = len(pairs) // chunk_size

    workers = []
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

    for w in workers:
        w.start()
    for w in workers:
        w.join()
    k = 0

    nn = nurls
    matrix = [[0] * nurls for i in range(nn)]
    for i in range(nn):
        for j in range(i+1,nn):
            matrix[i][j] = C_1D[k]
            k += 1
        print('')
    print(matrix)
    for i in range(nn):
        print("{},".format(i),end='')
        for j in range(nn):
            print("{},".format(matrix[i][j]), end='')
        print('')
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
nurls = 6
# Globals - End
#
# -----------------------------------------------------------------------------------------------------------------
# Call the functions
#read_words()
# OneComparison()
# MultipleComparisons()
#sequential()

if __name__ == '__main__':
    par_compare()
