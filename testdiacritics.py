#!/usr/bin/env python

# This script queries the Faculty Data Repository (FDR) and the Service Directory (SD) for faculty HR data to feed Symplectic Elements.
# For FDR records without email addresses it can query the LDAP Service Directory using the python util package
# from https://intranet.lib.duke.edu/download/python/
# There are few tricky spots in here due to the untrustworthiness of the FDR data, which the FDR people cannot or will not fix.

import cx_Oracle
import logging
import logging.handlers
from os.path import join
from os import getcwd, environ
from sys import exit
from djangoutil.xmlrpc import getServerProxy
from xml.sax.saxutils import escape
import codecs
import io

# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')


environ['NLS_LANG']= 'AMERICAN_AMERICA.AL32UTF8'

# # FDR database specifics
# dbhost = 'fdrprd-db.oit.duke.edu'
# dbport = '1637'
# dbsid = 'FDRPRD'
# dbuser = 'libacct'
# dbpassword = 'uanbahd10'

# retrieve results from Faculty Data Repository (FDR)
def getResults(ora, sql):
    ocur = ora.cursor()
    ocur.execute(sql)
    res = ocur.fetchall()
    ocur.close()
    return res


dbdsn = cx_Oracle.makedsn(dbhost, dbport, dbsid)  # Open the connection to the FDR database
ora = cx_Oracle.connect(dbuser, dbpassword, dbdsn)
sql = "select DUID, NETID, SALUTATION, SURNAME, FIRSTNAME, MIDDLENAME, LEGAL_SURNAME, LEGAL_FIRSTNAME, LEGAL_MIDDLENAME, EMAIL, PRIMARY_VIVO_ORG, PRIMARY_SCHOOL, affiliations from  APT.V_PEOPLE_WITH_AFFILIATIONS where NETID = 'JR152'"
res = getResults(ora, sql)  # Query FDR. data is a list of tuples, 1 tuple per record.
ora.close()

print res

text = u''
for v in res[0]:
    if v:
        print v
        text += (v.encode('utf-8') + u'\n')

with io.open('test.txt','w',encoding='utf8') as f:
    f.write(text)

f.close
