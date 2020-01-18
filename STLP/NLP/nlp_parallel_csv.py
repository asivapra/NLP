#!/usr/local/anaconda3/bin/python
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
from decimal import *
import sys
import collections
import string

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
groups = OrderedDict()
skip_pair = 1  # Skip a pair if its members are in either 'group' or 'member' list.
csv_lines = []
groups_and_members = []
super_group_lines = []


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
    print("Finished reading dictionary and stopwords")
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
    """
    The function that does the pair-wise comparison for CS score
    Called as: workers.append(mp.Process(target=par_CS, args=(b, e, pairs, w, kl, doc_segments, lock, nc, i_array, j_array, ij_array)))

    :param m: starting number of pairs[] to be processed
    :param n: ending number of of pairs[] to be processed
    :param pairs: the pairs[] list as e.g. '1,2', '1,3', etc.
    :param w: the worker number
    :param kl: keys_list of the doc_segments
    :param segs: doc_segments
    :param lock: lock to prevent race conditions
    :param ngid: number of lines
    :param i_array: to store the group IDs
    :param j_array: to store the member IDs
    :param ij_array: to store the group.member IDs
    :return:
    """
    global nlp, sr
    m_n = int(n-m)
    read_dictionary()
    # print(w, ':', m_n, pairs[m:n])
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

            if cs > cst:
                if i not in i_array:
                    for kk in range(ngid):
                        if not i_array[kk]:
                            i_array[kk] = i
                            break
                # ij = Decimal(str(i) + "." + str(j))
                if j not in j_array:
                    for kk in range(0, ngid*2, 2):
                        if not ij_array[kk]:
                            ij_array[kk] = i
                            ij_array[kk+1] = j
                            break
                if j not in j_array:  # Not needed ?
                    for kk in range(ngid):
                        if not j_array[kk]:
                            j_array[kk] = j
                            break
                lock.acquire()
                print("{}: {} : {}_{}\t{}\t ******".format(w, m_n, i, j, cs))
                f.write("{}_{}\t{}\t{}\t{}\n".format(i, j, cs, kl[i], kl[j]))
                lock.release()
            else:
                lock.acquire()
                # print("{}: {} : {}_{}\t{}".format(w, m_n, i, j, cs))
                f.write("{}_{}\t{}\t{}\t{}\n".format(i, j, cs, kl[i], kl[j]))
                lock.release()
                pass


def elapsedTime(text):
    caller = getframeinfo(stack()[1][0])
    global pt
    ct = time.perf_counter()
    et = round((ct - pt), 2)
    print("Line {}: Time for {}: {} sec.".format(caller.lineno, text, et))
    pt = ct


def p(*args):
    """
    Print the line number and sent text strings. Useful for debugging.
    :param s1: If empty, print "Debug marker"
    :param s2: Can be empty
    :return: None
    """
    text = ''
    # if s1 is '':
    #     s1 = 'Debug marker'
    caller = getframeinfo(stack()[1][0])
    print("Line {}:".format(caller.lineno), end='')
    for x in args:
        print(x, end='')
    # print(s1, s2)
    print('')

def write_out_csv_groups():
    # i_array = [485, 558, 581, 532, 487, 561, 546, 563, 494, 590, 537, 552, 568, 556, 541, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    # ij_array = [485, 617, 558, 572, 581, 586, 532, 549, 532, 551, 487, 577, 581, 628, 561, 562, 532, 611, 561, 589, 546, 555, 563, 564, 494, 516, 590, 624, 494, 558, 537, 621, 552, 570, 568, 585, 552, 613, 556, 581, 541, 598, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    # i_array = [1123, 935, 780, 646, 939, 1130, 786, 942, 1132, 1134, 546, 650, 1139, 376, 652, 793, 1150, 654, 1154, 948, 549, 45,
    #  797, 1165, 381, 552, 1174, 661, 1178, 958, 556, 558, 1200, 666, 561, 54, 1205, 1208, 966, 1211, 800, 1212, 563,
    #  669, 55, 802, 1234, 804, 805, 1236, 673, 403, 1252, 976, 1255, 58, 1259, 568, 981, 1272, 231, 1285, 680, 63, 816,
    #  1302, 419, 571, 64, 995, 420, 822, 1005, 824, 1332, 827, 69, 1333, 590, 1356, 833, 1006, 1364, 1008, 707, 1374,
    #  1009, 72, 837, 1010, 1391, 709, 439, 1413, 73, 1420, 711, 75, 1440, 1015, 838, 1481, 448, 1532, 78, 1562, 1042,
    #  1580, 1612, 593, 731, 1046, 595, 244, 736, 1048, 739, 1049, 1050, 82, 751, 1074, 844, 754, 853, 757, 603, 97, 760,
    #  1103, 1106, 1107, 854, 1108, 763, 1109, 764, 1112, 1114, 610, 487, 769, 773, 616, 494, 875, 621, 628, 277, 635,
    #  519, 299, 300, 145, 526, 532, 537, 319, 539, 176, 541, 181, 340, 191, 348]
    # ij_array = [1123, 1495, 935, 941, 935, 996, 935, 1001, 935, 1002, 935, 1034, 935, 1036, 935, 1087, 780, 1012, 935, 1147, 935,
    #  1149, 935, 1155, 935, 1156, 935, 1163, 935, 1164, 935, 1166, 780, 1093, 935, 1254, 935, 1263, 935, 1266, 935, 1295,
    #  935, 1298, 646, 755, 646, 758, 780, 1277, 935, 1352, 935, 1365, 935, 1366, 935, 1368, 935, 1414, 935, 1430, 935,
    #  1437, 935, 1445, 935, 1475, 780, 1525, 780, 1554, 935, 1537, 935, 1569, 935, 1589, 939, 1025, 939, 1271, 939, 1463,
    #  939, 1504, 939, 1512, 646, 1322, 1130, 1291, 786, 787, 646, 1540, 786, 868, 942, 1358, 1132, 1327, 786, 1217, 786,
    #  1250, 1134, 1135, 1134, 1136, 546, 555, 546, 646, 546, 780, 546, 836, 546, 960, 650, 878, 1139, 1142, 1139, 1167,
    #  376, 825, 376, 883, 376, 954, 652, 653, 652, 655, 376, 1467, 793, 794, 793, 896, 793, 935, 793, 989, 1150, 1206,
    #  793, 1067, 793, 1094, 793, 1186, 793, 1199, 793, 1227, 793, 1228, 1150, 1527, 793, 1241, 793, 1262, 654, 682, 793,
    #  1336, 793, 1397, 793, 1400, 793, 1447, 793, 1486, 793, 1559, 793, 1560, 793, 1561, 793, 1630, 1154, 1184, 1154,
    #  1191, 1154, 1204, 1154, 1305, 948, 1419, 1154, 1372, 1154, 1406, 1154, 1426, 948, 1599, 948, 1626, 549, 551, 549,
    #  598, 549, 611, 549, 703, 549, 778, 45, 1171, 654, 1593, 549, 1232, 549, 1330, 797, 846, 797, 848, 797, 1085, 1165,
    #  1190, 381, 952, 552, 570, 1174, 1181, 552, 613, 552, 617, 552, 670, 552, 723, 661, 745, 661, 747, 661, 749, 661,
    #  750, 661, 845, 552, 1189, 552, 1235, 1178, 1203, 1178, 1222, 958, 1061, 552, 1479, 552, 1606, 552, 1609, 556, 581,
    #  556, 586, 556, 630, 556, 631, 556, 633, 556, 721, 958, 1544, 958, 1546, 556, 753, 556, 756, 556, 835, 556, 985,
    #  556, 1464, 556, 1488, 556, 1489, 556, 1581, 556, 1582, 556, 1598, 558, 572, 1200, 1398, 666, 683, 666, 766, 666,
    #  772, 666, 812, 666, 975, 666, 1029, 561, 562, 561, 589, 54, 535, 666, 1316, 666, 1318, 666, 1319, 666, 1320, 1205,
    #  1207, 54, 727, 1208, 1215, 966, 1480, 966, 1575, 54, 1200, 54, 1369, 1211, 1231, 1211, 1485, 800, 980, 1212, 1224,
    #  800, 1052, 563, 564, 800, 1118, 800, 1124, 669, 793, 669, 803, 669, 830, 669, 859, 55, 1293, 802, 1466, 802, 1471,
    #  802, 1503, 802, 1558, 802, 1587, 55, 1498, 669, 1123, 669, 1130, 669, 1174, 1234, 1312, 804, 1551, 669, 1470, 669,
    #  1519, 805, 902, 1236, 1597, 673, 694, 403, 612, 1252, 1274, 403, 858, 403, 873, 976, 1032, 1255, 1269, 1255, 1396,
    #  58, 1317, 673, 1629, 1259, 1260, 568, 585, 981, 1044, 981, 1051, 568, 698, 1272, 1346, 231, 819, 1285, 1450, 680,
    #  695, 63, 958, 816, 817, 680, 1095, 680, 1096, 680, 1098, 63, 1058, 1302, 1339, 419, 1563, 571, 1468, 571, 1516, 64,
    #  518, 995, 1478, 420, 1154, 822, 823, 822, 917, 1005, 1078, 824, 948, 1332, 1436, 1005, 1201, 827, 887, 69, 1290,
    #  1333, 1465, 590, 624, 1356, 1457, 833, 866, 1006, 1007, 1364, 1449, 1364, 1515, 1364, 1600, 1008, 1023, 707, 1238,
    #  1374, 1405, 1374, 1441, 1374, 1491, 1009, 1024, 1009, 1059, 72, 1531, 72, 1535, 837, 939, 1010, 1017, 1391, 1617,
    #  1010, 1022, 709, 964, 439, 556, 1413, 1493, 73, 1172, 1420, 1460, 1420, 1505, 711, 729, 75, 720, 75, 726, 1440,
    #  1595, 1015, 1494, 838, 946, 1481, 1536, 448, 860, 838, 1047, 1532, 1534, 78, 666, 838, 1120, 1562, 1564, 1562,
    #  1565, 1562, 1570, 1042, 1116, 1580, 1628, 1612, 1613, 593, 842, 731, 732, 731, 734, 1046, 1053, 595, 718, 244, 605,
    #  736, 874, 1048, 1121, 838, 1325, 244, 728, 739, 742, 739, 743, 739, 744, 1049, 1122, 1050, 1105, 1050, 1566, 82,
    #  1357, 244, 1226, 751, 879, 751, 882, 751, 906, 1074, 1075, 1074, 1079, 1074, 1081, 1074, 1234, 844, 931, 844, 932,
    #  844, 945, 754, 981, 844, 1364, 844, 1373, 853, 861, 757, 898, 603, 1394, 757, 1117, 97, 645, 760, 820, 1103, 1132,
    #  760, 1349, 1106, 1127, 1107, 1128, 854, 1010, 1108, 1129, 763, 1348, 1109, 1110, 1109, 1111, 1109, 1134, 764, 765,
    #  1112, 1579, 1114, 1138, 610, 673, 487, 577, 769, 1433, 773, 774, 773, 967, 616, 669, 494, 516, 494, 558, 875, 876,
    #  621, 711, 628, 629, 628, 638, 628, 751, 628, 1257, 277, 816, 635, 636, 635, 640, 635, 641, 635, 642, 635, 905, 519,
    #  1091, 299, 610, 300, 616, 145, 628, 526, 1021, 526, 1043, 532, 549, 532, 862, 537, 621, 319, 1332, 539, 769, 176,
    #  796, 539, 1539, 541, 767, 541, 1030, 181, 809, 340, 531, 191, 1151, 348, 540, 348, 543]
    i_array = [1684, 1685, 1686, 1687, 343, 529, 1692, 1693, 775, 1004, 1698, 1429, 347, 1432, 1699, 1014, 218, 791, 219, 367,
     1474, 1709, 1501, 799, 1726, 380, 1730, 1738, 1742, 76, 1522, 1767, 1768, 1771, 591, 1780, 1099, 1790, 1796, 1811,
     1817, 1556, 1819, 101, 1584, 855, 1602, 246, 1858, 1636, 869, 1641, 1909, 1928, 1932, 1937, 135, 1664, 1967, 1666,
     1977, 1983, 1668, 1992, 1997, 2027, 470, 471, 267, 146, 676, 488, 681, 521]
    ij_array = [1684, 1868, 1684, 1872, 1684, 1879, 1684, 1893, 1684, 1894, 1684, 1895, 1684, 1896, 1684, 1927, 1684, 1934, 1684,
     1936, 1684, 2000, 1684, 2003, 1684, 2014, 1684, 2033, 1685, 1690, 1685, 1694, 1685, 1731, 1685, 1875, 1686, 1688,
     1686, 1716, 1686, 1717, 1686, 1926, 1687, 1718, 343, 2055, 529, 1776, 1687, 2008, 529, 1814, 1692, 1697, 1692,
     1723, 1692, 1725, 1692, 1728, 1692, 1753, 1692, 1762, 1692, 1773, 1693, 2001, 775, 1687, 1004, 1752, 1004, 1919,
     1698, 1764, 1698, 1783, 1698, 1837, 1698, 1856, 1429, 1973, 1698, 1881, 1698, 1987, 1698, 2018, 1698, 2023, 347,
     2061, 1432, 1662, 1432, 1663, 1699, 1713, 1699, 1748, 1432, 1869, 1699, 1940, 1014, 1659, 218, 1972, 791, 2056,
     219, 1632, 219, 1643, 219, 1670, 219, 1677, 367, 1698, 219, 2012, 1474, 1633, 1709, 2034, 1474, 1777, 1501, 1874,
     799, 1857, 1726, 1741, 1726, 1743, 1726, 1795, 1726, 1890, 380, 1759, 1730, 1735, 380, 1813, 380, 2051, 1738, 1877,
     1738, 1878, 1742, 1754, 76, 1959, 1522, 1651, 1767, 1916, 1768, 1799, 1768, 2020, 1771, 1772, 591, 1792, 1780,
     1781, 1780, 1782, 1099, 1913, 1099, 1915, 1790, 1958, 1796, 1797, 1796, 1798, 1796, 1891, 1811, 1859, 1817, 1950,
     1556, 1929, 1819, 1923, 101, 2032, 1819, 2059, 1584, 1661, 1584, 1665, 1584, 1672, 1584, 1676, 1584, 1684, 855,
     1811, 1602, 1989, 246, 1786, 1858, 1912, 1636, 1637, 869, 1867, 869, 1914, 1641, 2010, 1909, 2042, 1928, 1935,
     1932, 1933, 1937, 1938, 135, 1817, 1664, 1673, 135, 1999, 1664, 1674, 1967, 1968, 1967, 1969, 1666, 1682, 1666,
     1685, 1666, 1730, 1977, 1980, 1983, 1984, 1983, 1985, 1668, 1686, 1992, 1996, 1997, 1998, 2027, 2031, 470, 1715,
     471, 1932, 267, 1964, 146, 1705, 676, 1658, 488, 1747, 681, 1820, 521, 1866]
    i_array = [70, 1, 135, 145, 769, 1074, 1103, 775, 590, 1109, 11, 78, 1123, 439, 1178, 1205, 275, 1236, 610, 1259, 621, 1302,
     277, 471, 181, 1364, 824, 1440, 494, 1580, 299, 837, 676, 680, 312, 532, 54, 537, 539, 844, 754, 319, 854, 337, 63,
     363]
    ij_array = [70, 1684, 70, 1698, 1, 1391, 135, 1817, 145, 628, 769, 2027, 1074, 1234, 1103, 1132, 775, 1687, 590, 1154, 1109,
     1134, 11, 793, 78, 666, 1123, 1130, 439, 556, 439, 1666, 1178, 1983, 1205, 1686, 275, 552, 275, 616, 275, 669,
     1236, 1768, 275, 966, 610, 673, 275, 1150, 1259, 1685, 621, 711, 1302, 1726, 277, 816, 471, 1932, 181, 646, 1364,
     1811, 181, 1738, 824, 948, 1440, 1928, 494, 558, 1580, 1767, 299, 610, 837, 939, 676, 780, 680, 1780, 312, 680,
     532, 549, 54, 1200, 532, 1709, 537, 621, 539, 769, 844, 935, 844, 1364, 754, 981, 319, 1332, 854, 1010, 337, 676,
     63, 958, 363, 590]

    iva = [i for i in i_array]
    iva.sort()
    for i in range(len(iva)):
        iv = iva[i]
        if iv:
            print("{}:\t".format(iv), end='')
            # f.write("{}\t".format(iv))
            for j in range(0, len(ij_array), 2):
                ij = ij_array[j]
                if ij == 0: break
                if iv is ij:
                    jv = ij_array[j + 1]
                    # f.write("{} ".format(jv))
                    print("{} ".format(jv), end='')
            # f.write("\n")
            print("")


def rewrite_groups_and_members():
    """
    Reads the 'i_array' and 'ij_array' to add member IDs to teh reference groups
    i_array:  [11, 131, 217, 0, 0, 0]
    ij_array: [11, 518, 131, 517, 131, 520, 217, 515, 0, 0, 0, 0]
    The ij_array is twice as long as i_array and holds pairs of integers as i,j
    Nested iteration of i_array and ij_array will get the j values (member ID) corresponding to i (group ID)

    :return:
    """
    global i_array, j_array, ij_array, group_ids, member_ids
    # group_ids = [552, 966, 1200]
    # member_ids = ['570 613 617 670 723 1189 1235 1479 1606 1609', '1480 1575', '1398']
    # ij_array = [552, 3007, 966, 3005, 966, 3006, 552, 3025, 552, 3044, 1200, 3010]
    print(len(group_ids))
    print(group_ids)
    print(member_ids)
    print(ij_array[:])
    # p(len(ij_array))
    with open("Files/groups_and_members.txt", "w", encoding="utf8") as f:
        f.write("Groups\tMembers\n")
        for i in range(len(group_ids)):
            iv = int(group_ids[i])
            jv = member_ids[i]
            # print("A. {} : {}".format(iv, jv))
            for j in range(0, len(ij_array), 2):
                ij = ij_array[j]
                # print("B. {} : {}".format(iv, ij))
                if ij == 0: break
                if iv is ij:
                    # print("C. {} : {}".format(iv, jv))
                    jv += ' ' + str(ij_array[j+1])
                    member_ids[i] = jv
                    # print("iv, jv", iv, jv)
            print("{}\t{}".format(iv, jv))
            f.write("{}\t{}\n".format(iv, jv))
    # print(member_ids)


def par_compare_list(kl, nb, ne):
    """
    Read the Files/groups_and_members.txt to make a list of all un-grouped numbers.
    Then pair-wise compare these ungrouped docs.
    :return:
    """
    global i_array, j_array, ij_array
    ig = []  # Groups
    gn = []  # Grouped numbers
    ugn = [] # Un-grouped numbers
    csv_filename = "Files/groups_and_members.txt"
    with open(csv_filename, encoding="utf8") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\t')
        for row in csv_reader:
            ml = row[1].split()
            ig.append(int(row[0]))
            gn.append(int(row[0]))
            for i in ml:
                gn.append(int(i))
        ig.sort()
        gn.sort()
    compare_ig = True  # To compare the group IDs each other
    # List the items to be compared pair-wise
    nb = 0
    # ne = len(ugn)
    ne = 289
    nc = ne - nb  # Total number of lines
    if not compare_ig:
        print(len(ugn), ":", ugn)
        for i in range(1, ne):
            if i not in gn:
                ugn.append(i)
    else:
        print(len(ig), ":", ig)
        for i in ig:
            ugn.append(i)
    # List the duplicates
    for item, count in collections.Counter(ugn).items():
        if count > 1:
            p(count, item)
    # sys.exit(0)
    pairs = []
    for i in range(nb, ne):
        ig = ugn[i]
        for j in range(i + 1, ne):
            jg = ugn[j]
            k = str(ig) + ',' + str(jg)
            pairs.append(k)
    p(len(pairs))
    et = round((len(pairs) * 0.02) / 3600, 2)
    p(et, "Hours")
    # sys.exit(0)
    elapsedTime("Creating Pairs")
    i_array = mp.RawArray('i', nc)  # flat version of matrix C. 'i' = integer, 'd' = double, 'f' = float
    j_array = mp.RawArray('i', nc)
    ij_array = mp.RawArray('i', nc*2)
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



def par_compare_groups(kl, nb, ne):
    global i_array, j_array, ij_array, group_ids, member_ids
    group_ids = []
    member_ids = []
    file1 = []
    file2 = []
    csv_filename = "Files/groups_and_members.txt"
    with open(csv_filename, encoding="utf8") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\t')
        line_count = 0
        for row in csv_reader:
            # Skip the header row
            if line_count == 0:
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
    # p(pairs)
    print("pairs:", len(pairs))
    elapsedTime("Creating Pairs")
    i_array = mp.RawArray('i', nc)  # flat version of matrix C. 'i' = integer, 'd' = double, 'f' = float
    j_array = mp.RawArray('i', nc)
    ij_array = mp.RawArray('i', nc*2)
    # pairIDs = mp.RawArray('f', nc)
    num_workers = mp.cpu_count()
    chunk_size = len(pairs) // num_workers + 1
    n_chunks = len(pairs) // chunk_size
    lock = mp.Lock()
    workers = []
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
    for w in workers:
        w.start()
    elapsedTime("Starting Workers")
    # Wait for all processes to finish
    for w in workers:
        w.join()
    elapsedTime("Running Workers")


def par_compare(kl, nb, ne):
    """
    Function to compare Gavin Mackay's data in the CSV files This function sets up multi-processing.
    STEPS
        1. Create an array of the pairs of comparisons.
        This is in the form of a list as below, where the numbers are the indices of the URLs...
            ['1,2', '1,3', '1,4', '2,3', '2,4', '3,4']
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
    ij_array = mp.RawArray('i', nc*2)
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

    Several rows of key_phrases for the same source file. These are appended, with spaces, into a long text string
    and added to a global OrderedDict, 'doc_segments', using the source file name as the key. OrderedDict is used
    instead of normal dict so that the order of filenames in the CSV is maintained in the data structure. It helps
    to manually check the results.

    The Cosine Similarity will be calculated on these key phrases between pairs of source files.

    :return:
    """
    global doc_segments
    read_csv_file = True
    write_processed_file = True
    if read_csv_file:
        csv_filename = "Files/stlprecordassociationkeyphrases.typed.csv"
        with open(csv_filename, encoding="utf8") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                filename = ''.join(filter(lambda x: x in string.printable, row[4]))
                key_phrases = ''.join(filter(lambda x: x in string.printable, row[3]))
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
                        # key_phrases = ''.join(filter(lambda x: x in string.printable, row[3]))

                        doc_segments[filename] += key_phrases + ' '
                    except Exception as e:
                        # key_phrases = ''.join(filter(lambda x: x in string.printable, row[3]))
                        # The first value is assigned to the key
                        doc_segments[filename] = filename + ' '  # Add the filename to the key_phrases
                        doc_segments[filename] += key_phrases + ' '
        elapsedTime("Reading the CSV")
    if write_processed_file:
        processed_file = "Files/processed_file.txt"
        keys_list = list(doc_segments.keys())
        k = 0
        with open(processed_file, "w", encoding="utf8") as f:
            for key in keys_list:
                line = key + "\t" + doc_segments[key] + "\n"
                # print(line)
                f.write(line)
                k += 1
            text = "writing " + str(k) + " lines to " + processed_file
        elapsedTime(text)


def count_ungrouped_items(i, j, nb, ne):
    n = 0
    # nc = ne - nb  # Total number of lines
    print(i[:])
    print(j[:])
    for k in range(nb, ne):
        if k not in i and k not in j:
            n += 1
            print(n, k)


def merge_groups_ids():
    ig = []  # Groups
    gn = []  # Grouped numbers
    csv_filename = "Files/groups_and_members.txt"
    with open(csv_filename, encoding="utf8") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\t')
        for row in csv_reader:
            ml = row[1].split()
            ig.append(int(row[0]))
            gn.append(int(row[0]))
            for i in ml:
                gn.append(int(i))
        ig.sort()
        gn.sort()
    with open("Files/nlp_parallel_csv_results.txt", "r") as f:
        lines = f.readlines()
    p(lines)


def remove_from_members(i, j):
    global csv_lines, groups_and_members, super_group_lines
    print(len(groups_and_members), i, j, ' ', end='')
    for k in range(1, len(groups_and_members)):
        fields = groups_and_members[k].split('\t')
        if int(fields[0]) is i:
            # p(fields[0], i)
            jv = fields[1]
            # p(jv)
            jvr = str(j) + ' '  # Internal item with a space after
            if jvr in jv:
                jv = jv.replace(jvr, '')
            else:
                jvr = str(j)  # The last item; no space after
                jv = jv.replace(jvr, '')

            # p(jv)
            newline = str(i) + '\t' + jv
            # p(newline)
            groups_and_members[k] = newline
            break


def match_original_group(i, j):
    global csv_lines, groups_and_members
    gl = csv_lines[i].split('\t')
    gm = csv_lines[j].split('\t')
    doc1 = Lemmatise(gl[1])
    doc1str = " ".join(doc1)
    doc1nlp = nlp(doc1str)
    doc2 = Lemmatise(gm[1])
    doc2str = " ".join(doc2)
    doc2nlp = nlp(doc2str)
    sim = doc1nlp.similarity(doc2nlp)
    cs = round(sim, 2)
    print("Checking again:", end='')
    stars = '--------- OK'
    if cs < 0.94:
        remove_from_members(i, j)
        stars = 'xxxxxxxx Not OK'
        print(cs, stars)
    else:
        print(i, j, cs, stars)


def read_group_data():
    global csv_lines, groups_and_members, super_group_lines
    infile = "Files/processed_file.txt"
    with open(infile, encoding="utf8") as f:
        csv_lines = f.readlines()

    infile = "Files/groups_and_members.txt"
    with open(infile, encoding="utf8") as f:
        groups_and_members = f.readlines()

    infile = "Files/super_groups.txt"
    with open(infile, encoding="utf8") as f:
        super_group_lines = f.readlines()


def consolidate_matches():
    """
    Process the output from 'inter_group_matching' to combine groups and members.

    - Read col1 for primary group.
    - Add all in col2 as members for this primary group.
    - Iterate through the col2 members and take their col2 lists and add.
    - Iterate col2 of primary group with col2 of ALL and take their primary groups.
    - Sort and remove duplicates.
    :return:
    """
    infile = "Files/inter_group_matches_positives.txt"
    with open(infile, encoding="utf8") as f:
        inter_group_matches_positives = f.readlines()
        lg = len(inter_group_matches_positives)
        # p(inter_group_matches_positives)
        fields = inter_group_matches_positives[1].split('\t')
        cg = int(fields[0])  # Current col1 group
        groups[cg] = ''
        for k in range (1, lg):
            fields = inter_group_matches_positives[k].split('\t')
            a = int(cg)
            b = int(fields[0])
            # p(type(fields[0]), type(cg), cg, ' ', fields[0])
            if a == b:
                # p('OK')
                groups[cg] += fields[1] + ' '  # Add the col2 groups
            else:
                # p(cg, ' ', groups[cg])
                cg = int(fields[0])  # Current col1 group
                groups[cg] = fields[1] + ' '
        p(groups)
        keys = groups.keys()
        # for key in keys:
        #     print(groups[key])

        # Take the members of the col2 groups to col1 group
        for cg in groups.keys():
            # p(cg, ":", groups[cg])
            jv = groups[cg].split()
            for j in jv:
                try:
                    # col2 = groups[int(j)]
                    groups[cg] += groups[int(j)]
                    groups[int(j)] = ''
                    # print(cg, ' ', j, ' ', col2)
                except:
                    pass
            gcg = groups[cg].split()
            gcg = [int(x) for x in gcg]
            gcg = sorted(set(gcg))
            groups[cg] = gcg
            # p(cg, ' ', groups[cg])
            # p(cg, ' ', gcg)
        #     pass
        empty = []
        for cg in groups.keys():
            jv = groups[cg]
            # p(cg, jv)
            if len(jv) == 0:
                empty.append(cg)
        gk = list(groups.keys())
        lg = len(gk)
        p(lg, gk)
        for e in empty:
            del groups[e]
        p(len(groups.keys()))
        gk = list(groups.keys())
        lg = len(gk)
        p(lg, gk)
        for i in range(lg):
            key = gk[i]
            p(i, ' ', key)
            p(i, ' ', key, ' ', groups[gk[key]])
            for j in groups[gk[key]]:
                pass



def inter_group_matching():
    """
    Test a group against the 'super_groups.txt' for all other groups.

    If any match, then add the group ID and all its members to the other matching group ID.
    :return:
    """
    global csv_lines, groups_and_members, super_group_lines
    read_group_data()
    read_dictionary()
    k0 = 0
    for n in range(0, len(super_group_lines)):
        fields = super_group_lines[n].split('\t')
        i = int(fields[0])
        key_phrases = fields[3].strip()
        doc1 = Lemmatise(key_phrases)
        doc1str = " ".join(doc1)
        doc1nlp = nlp(doc1str)
        k2 = 0
        for k in range(n+1, len(super_group_lines)):
            fields = super_group_lines[k].split('\t')
            j = int(fields[0])
            fields = csv_lines[j].split('\t')
            doc2 = Lemmatise(fields[1].strip())
            doc2str = " ".join(doc2)
            doc2nlp = nlp(doc2str)
            sim = doc1nlp.similarity(doc2nlp)
            cs = round(sim, 2)
            if cs > 0.93:
                print(i, ':', j, ':', cs, '**********')
                k2 += 1
                k0 += 1
            else:
                print(i, ':', j, ':', cs)
        print("Matches: Total: {}; This group: {}\n".format(k0, k2))

            # break


def intra_group_matching():
    """
    Test a group's members against its own super group key_phrases.

    From 'super_groups.txt' take the key_phrases for a group and match against the key_phrases from
    'processed_files.txt' for its members.
    e.g. for group id=1 take the key_phrases from row 1 in 'super_groups.txt'

    Then, take the key_phrases for its members ['15', '66', '77', ... ] from 'processed_files.txt'

    The expectation is that all members must score above threshold. Those which do not score above
    are matched again by taking the key_phrases for BOTH the group and member ID from 'processed_files.txt'.

    If still below threshold, then delete the member ID from 'groups_and_members.txt'
    :return:
    """
    global csv_lines, groups_and_members, super_group_lines
    read_group_data()
    read_dictionary()
    # infile = "Files/processed_file.txt"
    # with open(infile, encoding="utf8") as f:
    #     csv_lines = f.readlines()
    #
    # infile = "Files/groups_and_members.txt"
    # with open(infile, encoding="utf8") as f:
    #     groups_and_members = f.readlines()
    #
    # infile = "Files/super_groups.txt"
    # with open(infile, encoding="utf8") as f:
    #     super_group_lines = f.readlines()
        # for line in lines:
    for n in range (0, len(super_group_lines)):
        fields = super_group_lines[n].split('\t')
        i = int(fields[0])
        jv = fields[1].split()
        jvl = len(jv)
        if jvl > 10:
            jvl = 10
        key_phrases = fields[3].strip()
        print(i, jv[:jvl])
        doc1 = Lemmatise(key_phrases)
        doc1str = " ".join(doc1)
        doc1nlp = nlp(doc1str)
        for j in jv:
            fields = csv_lines[int(j)].split('\t')
            doc2 = Lemmatise(fields[1].strip())
            doc2str = " ".join(doc2)
            doc2nlp = nlp(doc2str)
            sim = doc1nlp.similarity(doc2nlp)
            cs = round(sim, 2)
            if cs < 0.94:
                match_original_group(i, int(j))
            else:
                pass
    outfile = "Files/groups_and_members.txt"
    with open(outfile, 'w', encoding="utf8") as f:
        for line in groups_and_members:
            f.write(line)

def test_super_group_0():
    read_dictionary()
    infile = "Files/super_groups.txt"
    with open(infile, encoding="utf8") as f:
        lines = f.readlines()
        line_count = 0
        for line in lines:
            fields = line.split('\t')
            # Skip the header row
            if line_count == 0:
                doc1 = Lemmatise(fields[1].strip())
                p(fields[0])
                line_count += 1
            else:
                doc2 = Lemmatise(fields[1].strip())
                p(fields[0])
                line_count += 1
                # break
                doc1str = " ".join(doc1)
                doc1nlp = nlp(doc1str)
                doc2str = " ".join(doc2)
                doc2nlp = nlp(doc2str)
                sim = doc1nlp.similarity(doc2nlp)
                cs = round(sim, 2)
                p(cs)


def sort_groups_and_members():
    infile = "Files/groups_and_members.txt"
    with open(infile, encoding="utf8") as f:
        groups_and_members = f.readlines()
        lg = len(groups_and_members)
        for n in range(1, lg):
            fields = groups_and_members[n].strip().split('\t')
            jv = list(map(int, fields[1].split())) # split a string array at spaces and convert into an int array
            jv = sorted(set(jv))  # Sort and remove duplicates
            fields[1] = ' '.join([str(x) for x in jv]) + '\n'
            groups_and_members[n] = '\t'.join(fields)

    outfile = "Files/groups_and_members.txt"
    with open(outfile, 'w', encoding="utf8") as f:
        for line in groups_and_members:
            f.write("{}".format(line))
    # sys.exit()


def create_super_group():
    """
    To create a master doc, we can try this method:
•	Concatenate the key_phrases for a group and all its members.
•	Remove duplicates or keep the duplicates.
•	Lemmatise or let the program do it on the fly.
•	Compare the group and all its members against this master doc.
•	Compare other groups with this master doc.

    :return:
    """

    # i = 0
    # j = 0
    # key_phrases = ''
    # csv_lines = []

    # Read the input file. This is created by 'def read_csv'
    infile = "Files/processed_file.txt"
    with open(infile, encoding="utf8") as f:
        csv_lines = f.readlines()

    outfile = "Files/super_groups.txt"
    # Create an empty output file
    with open(outfile, "w", encoding="utf8") as f:
        f.write("")

    sort_groups_and_members() #  Sort the group IDs and member IDs in this file

    with open(outfile, "a", encoding="utf8") as fout:
        # Read the groups and members list
        infile = "Files/groups_and_members.txt"
        with open(infile, encoding="utf8") as f:
            csv_rows = csv.reader(f, delimiter='\t')
            line_count = 0
            for row in csv_rows:
                # Skip the header row
                if line_count == 0:
                    line_count += 1
                else:
                    line_count += 1
                    # Get the group number. This is the index of the line in 'csv_lines[]'
                    i = int(row[0].replace(' ', ''))

                    # Get the members' list for the group ID. This is a space-separated string of numbers
                    j = row[1]
                    key_phrases = ''

                    # Get the group file and take its filename
                    gf = csv_lines[i].split('\t')
                    # print(gf[0].strip(), '\t', end='')

                    # Write the group filename in column A
                    fout.write("{}\t{}\t{}\t".format(i, j, gf[0].strip()))

                    # Add the group file's key_phrases
                    key_phrases += gf[1].strip() + ' '

                    # Split the members' IDs and get their key_phrases
                    # print(i, j)
                    jv = j.split()
                    for j in (jv):
                        mf = csv_lines[int(j)].split('\t')
                        key_phrases += mf[1].strip() + ' '
                    key_phrases = ''.join(filter(lambda x: x in string.printable, key_phrases))
                    fout.write("{}\n".format(key_phrases))


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
    # read_csv()
    # create_super_group()
    # intra_group_matching()
    # inter_group_matching()
    consolidate_matches()
    sys.exit()
    # ------------------------
    # Read the CSV files and create a dict of doc_segments, where the keys are the filenames
    # and the values are the key phrases concatenated together.
    # merge_groups_ids()  # The inter-group comparison gave several matches. Combine these in groups_and_members.txt
    # sys.exit(0)
    read_csv()
    # Take the keys into an array
    keys_list = list(doc_segments.keys())

    # nc = len(keys_list)
    nb = 4600
    ne = 4605

    # Mode of operation: pair_wise = compare all pairs to get groups
    # group_wise: compare linearly with existing groups
    pair_wise = False
    group_wise = True
    list_wise = False
    if pair_wise:
        # group_wise = False
        # Do pairwise comparison of  nb to ne lines in the CSV file
        par_compare(keys_list, nb, ne)
        print(i_array[:])
        print(j_array[:])
        print(ij_array[:])
        # Make into groups
        with open("Files/nlp_parallel_csv_groups.tsv", "w", encoding="utf8") as f:
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
        # with open("Files/i_ij_arrays.txt", "a", encoding="utf8") as f:
        ls = [e for i, e in enumerate(i_array) if e != 0]
        print(ls)
        # f.write("{}\n".format(ls[:]))
        ls = [e for i, e in enumerate(ij_array) if e != 0]
        print(ls)
        # f.write("{}\n".format(ls[:]))
        # print(i_array[:])
        # print(j_array[:])
        # print(ij_array[:])
        rewrite_groups_and_members()
    elif list_wise:
        """
        This will compare pair-wise the list and make groups/members list that can be added
        to the total.
        
        The i and ij values will be displayed and/or written out. These must be manually copied
        to the 'write_out_csv_groups' after commenting out the remaining lines in the below block.
        
        The displayed results are then copied and added to the end in Files/groups_and_members.xlsx and
        exported as Files/groups_and_members.txt for further runs. 
        """
        # write_out_csv_groups()
        par_compare_list(keys_list, nb, ne)
        print(i_array[:])
        print(ij_array[:])
        with open("Files/i_ij_arrays.txt", "a", encoding="utf8") as f:
            f.write("{}\n".format(i_array[:]))
            f.write("{}\n".format(ij_array[:]))
        p("-----------------------------------")
        # write_out_csv_groups()

    et1 = time.perf_counter() - ct0
    print("Total Time: {:0.2f} sec".format(et1))
