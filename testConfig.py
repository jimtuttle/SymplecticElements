#!/usr/bin/env python

from os import getcwd
from os.path import join
from ConfigParser import SafeConfigParser


# choose dev, test, or prod
usedb = 'dev'


config = SafeConfigParser()
config.read(join(getcwd(), 'fdr.config'))
dbhost = config.get(usedb, 'dbhost')
dbport = config.get(usedb, 'dbport')
dbsid = config.get(usedb, 'dbsid')
dbuser = config.get(usedb, 'dbuser')
dbpassword = config.get(usedb, 'dbpassword')

print dbhost
print dbport
print dbsid
print dbuser
print dbpassword
