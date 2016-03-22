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
from ConfigParser import SafeConfigParser
import codecs
import io
from django.conf import settings
from djangoutil import config
settings.configure(config)
# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
# set to UTF-8 to capture diacritics
environ['NLS_LANG']= 'AMERICAN_AMERICA.AL32UTF8'



# database configuration
usedb = 'test' # choose database: dev, test, or prod
config = SafeConfigParser()
config.read(join(getcwd(), 'fdr.config')) # read config file to gather parameters
dbhost = config.get(usedb, 'dbhost')
dbport = config.get(usedb, 'dbport')
dbsid = config.get(usedb, 'dbsid')
dbuser = config.get(usedb, 'dbuser')
dbpassword = config.get(usedb, 'dbpassword')

useldapforemail = False  # LDAP is slow and hasn't returned significant number of emails. If False, use netid+@duke.edu instead.
sd_file = join(getcwd(), 'libsymel.dat') # Nightly export of Service Directory data
xmlfile = join(getcwd(), 'people.xml') # Output file for Symplectic Elements consumption
affiliationsfile = join(getcwd(), 'affiliations.txt') # Output file for unique affiliations to populate Elements Auto Groups

# instantiate and configure logger
logfile = join(getcwd(), 'hrDataFeeder.log')
logger = logging.getLogger('fdrlogger')
logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=20971520, backupCount=5)       # limit to 6 files of 20 MB or less
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# retrieve results from Faculty Data Repository (FDR)
def getResults(ora, sql):
    ocur = ora.cursor()
    ocur.execute(sql)
    res = ocur.fetchall()
    ocur.close()
    return res

# Take list of dictionaries and build XML elements.  Return string.
def buildXml(list):
    sequence_dict = {1:'Secondary', 2:'Tertiary', 3:'Quaternary', 4:'Quinary', 5:'Senary', 6:'Septenary', 7:'Octonary', 8:'Nonary', 9:'Denary'}
    xml = ''
    for record in list:
        xml += '\t\t<person>\n'
        xml += '\t\t\t<Lastname>%s</Lastname>\n' % (record['surname'])
        xml += '\t\t\t<Firstname>%s</Firstname>\n' % (record['forename'])
        try:
            xml += '\t\t\t<Middlename>%s</Middlename>\n' % (record['middlename'])
        except:
            pass
        xml += '\t\t\t<Email>%s</Email>\n' % (record['email'])  # removing angle brackets in some email fields
        xml += '\t\t\t<Proprietary_ID>%s</Proprietary_ID>\n' % (record['duid'])
        xml += '\t\t\t<Username>%s</Username>\n' % (record['netid'])
        xml += '\t\t\t<PrimaryGroupDescriptor>%s</PrimaryGroupDescriptor>\n' % (escape(record['primary']))
        # this must change in response to addition of school
        if 'secondary' in record:
            if len(record['secondary']) > 0:
                i = 1
                for appointment in record['secondary']:
                    xml += '\t\t\t<%sGroupDescriptor>%s</%sGroupDescriptor>\n' % (sequence_dict[i], escape(appointment.strip()), sequence_dict[i])
                    i += 1
        xml += '\t\t\t<IsAcademic>%s</IsAcademic>\n' % (record['academic'])
        xml += '\t\t\t<LoginAllowed>%s</LoginAllowed>\n' % (record['login'])
        xml += '\t\t\t<AuthenticatingAuthority>%s</AuthenticatingAuthority>\n' % (record['authority'])
        xml += '\t\t</person>\n'
    return xml


# Build list of dictionaries of FDR people. Also return list of Duke Unique IDs.
def buildFdrDict(data, rpcserver, sd_dict_list):
    print 'buildFdrDict'
    fdr_dict_list = []
    # CHANGE THIS.  DROP FDR RECORD WITHOUT NETID, USE NETID as KEY
    netid_list = []
    missing_fdr_email = 0
    missing_email_found_sd = 0
    for record in data:
        drop_record = False
        fdr_dict = {}
        try: # Confusing. FDR forced their names on us. Their PRIMARY_SCHOOL is our primary group, all other groups are secondary for us.
            duid, netid, salutation, surname, forename, middlename, lsurname, lforename, lmiddlename, email, primary, school, secondary,  primary_affiliation = record
        except ValueError:
            logmessage = 'Database view has changed.'
            logger.critical(logmessage)
            exit()
        if not netid: # Some people records do not contain netid. Look in SD file. If not there, log and discard person.
            logmessage = 'Record dropped - No NetID in FDR. %s %s, %s' % (forename, surname, duid)
            logger.critical(logmessage)
            print logmessage
            drop_record = True
            continue
        else:
            pass

            # for person in sd_dict_list:  # Look through SD records
            #     if duid == person['duid']:  # If DUID matches...
            #         print person
            #         netid = person['netid']   # Assign SD netid to person
            #         logmessage = "Found FDR person %s missing netid." % (duid)
            #         logger.info(logmessage)
            #         print logmessage
            #         break
            # else:  # If also no netid in SD, log and set flag to drop this record.
            #     logmessage = "Person %s missing netid in FDR and SD." % (duid)
            #     logger.critical(logmessage)
            #     print logmessage
            #     drop_record = True

        if surname: # If professional name set, use that. Otherwise fall back to legal name.
            fdr_dict['surname'] = surname
            fdr_dict['forename'] = forename
            if middlename: # Many records do not contain middle name.
                fdr_dict['middlename'] = middlename
        else: # Legal name block
            fdr_dict['surname'] = lsurname
            fdr_dict['forename'] = lforename
            if lmiddlename:
                fdr_dict['middlename'] = lmiddlename
        if not email: # Some people do not have email addresses for some reason that I cannot comprehend.
            missing_fdr_email += 1
            if not drop_record:  # If there's no netid, there's no point in continuing with this record.
                for person in sd_dict_list:  # Look through SD records
                    if duid == person['duid']:  # If DUID matches...
                        email = person['email']   # Assign SD netid to person
                        logmessage = "FDR person %s missing email found in Service Directory." % (duid)
                        missing_email_found_sd += 1
                        #print logmessage
                        #logger.info(logmessage)
                        break
            else:
                    email = person['email']
                    email = email.translate(None, "<>") # Remove angle brackets present in some email fields
                #email = netid + "@duke.edu"
        fdr_dict['email'] = email
        fdr_dict['duid'] = duid
        fdr_dict['netid'] = netid
        fdr_dict['primary'] = school
        # Non-primary appointments. Convert double-pipe delimited string to list and add PRIMARY_VIVO_ORG to that.
        secondary_deduped_list = []  # Deduplicate the secondary appointments. Often duplicates.
        if secondary:
            secondary = secondary.strip() # Remove EOL character.
            if '||' in secondary:  # Double pipes indicates concatenated result.
                secondary_list = secondary.split('||')  # Split results into list
                for appt in secondary_list:
                    if (appt not in secondary_deduped_list) and (appt != school):  # Don't want school twice.
                        secondary_deduped_list.append(appt)
            elif secondary != school:  # Single result, dedupe against school.
                secondary_deduped_list.append(secondary)
            if (primary not in secondary_deduped_list) and (primary != school): # Dedupe primary against secondary appts and school.
                secondary_list.append(primary)
            fdr_dict['secondary'] = secondary_deduped_list
        fdr_dict['academic'] = 'Y'
        fdr_dict['login'] = 'Y'
        fdr_dict['authority'] = 'Shibboleth'
        netid_list.append(netid)
        if not drop_record:
            fdr_dict_list.append(fdr_dict)
        else: # Discard this record and log.
            logmessage = 'Record dropped for DUID:%s Forename: %s Surname: %s' % (duid, forename, surname)
            logger.info(logmessage)
    if missing_fdr_email > 0:
        logmessage = '%s FDR records without email addresses' % (missing_fdr_email)
        logger.info(logmessage)
        print '%s people missing FDR email found in SD' % (missing_email_found_sd)
    return fdr_dict_list, netid_list


# Build list of dictionaries of service directory entries after deduplicating people from FDR
def buildSdDict(sd_file):
    sd_dict_list = []
    duplicates = 0
    sd_missing_email = 0
    sd = open(sd_file, 'r')
    print '1'
    for line in sd:
        sd_dict = {}
        duid , netid, surname, forename, email, status = line.split('|')
        sd_dict['duid'] = duid
        sd_dict['netid'] = netid
        sd_dict['surname'] = surname
        sd_dict['forename'] = forename
        sd_dict['primary'] = status.strip() # Remove line break
        sd_dict['academic'] = 'N'
        sd_dict['login'] = 'Y'
        sd_dict['authority'] = 'Shibboleth'
        if email:
            email = email.translate(None, "<>") # Remove angle brackets present in some email fields
            sd_dict['email'] = email
        else:
            sd_dict['email'] = netid + '@duke.edu'
            sd_missing_email += 1
        sd_dict_list.append(sd_dict)
    sd.close()
    logmessage = 'Found %s Service Directory records.' % (len(sd_dict_list) + duplicates)
    logger.info(logmessage)
    logmessage = '%s Service Directory records without email addresses' % (sd_missing_email)
    logger.info(logmessage)
    #logmessage = '%s Service Directory records were duplicates.' % (duplicates)
    #logger.info(logmessage)
    print 'testing" return buildDdDict'
    return sd_dict_list

# Deduplicate the SD people to prevent creating multiple accounts as some will appear in FDR data.
def dedupeSdDictList(sd_dict_list, netid_list):
    duplicates = 0
    sd_dict_list_dedupe = []
    for record in sd_dict_list:
         if record['netid'] not in netid_list: # Deduplicate these records against FDR records.
            sd_dict_list_dedupe.append(record)
            duplicates += 1
    logmessage = "Found %s Service record duplicates." % (duplicates)
    logger.info(logmessage)
    return sd_dict_list_dedupe


# Serialize list of unique affiliations to populate Elements Auto Groups
def getUniqueAffiliations(fdr_dict_list):
    unique_affiliations_list = []
    for dict in fdr_dict_list:
        if 'secondary' in dict:
            for affiliation in dict['secondary']:
                if affiliation not in unique_affiliations_list:
                    unique_affiliations_list.append(affiliation)
    return unique_affiliations_list



if __name__=='__main__':
    try:
        logmessage = "Starting update." # Begin logging
        logger.info(logmessage)
        dbdsn = cx_Oracle.makedsn(dbhost, dbport, dbsid)  # Open the connection to the FDR database
        try:
            ora = cx_Oracle.connect(dbuser, dbpassword, dbdsn)
        except:
            logmessage = 'Database connection error.'
            logger.critical(logmessage)
            exit()
        sql = 'select DUID, NETID, SALUTATION, SURNAME, FIRSTNAME, MIDDLENAME, LEGAL_SURNAME, LEGAL_FIRSTNAME, LEGAL_MIDDLENAME, EMAIL, PRIMARY_VIVO_ORG, PRIMARY_SCHOOL, affiliations, PRIMARY_AFFILIATION from  APT.V_PEOPLE_WITH_AFFILIATIONS'
        data = getResults(ora, sql)  # Query FDR. data is a list of tuples, 1 tuple per record.
        logmessage = 'Found %s FDR faculty.' % (len(data))
        logger.info(logmessage)
        ora.close()
        xml_preabmle = '<?xml version="1.0" encoding="UTF-8" ?>\n<HR_Data>\n'  # Begin the XML string to write to people.xml
        xml_preabmle += '\t<Feed_ID>FDR</Feed_ID>\n'
        xml_preabmle += '\t<people>\n'
        if useldapforemail:
            rpcserver = getServerProxy()  # Open connection to Service Directory
        else:
            rpcserver = False
        sd_dict_list = buildSdDict(sd_file)  #  Build list of attributes about people from Service Directory dump file.
        fdr_dict_list, netid_list = buildFdrDict(data, rpcserver, sd_dict_list)

        unique_affiliations_list = getUniqueAffiliations(fdr_dict_list) # Build list of unique affiliations/appointments for Elements
        netid_list.sort()

        sd_dict_list_dedupe = dedupeSdDictList(sd_dict_list, netid_list)  # Deduplicate Service Directory people so we don't name people twice
        # TESTED TO HERE
        sd_xml = buildXml(sd_dict_list_dedupe)  # Build the XML string from SD people
        print 'testing buildXML sd dict'
        fdr_xml = buildXml(fdr_dict_list)  # Build the XML string for FDR people
        print 'testing buildXML fdr dict'
        xml_postamble =  '\t</people>\n</HR_Data>'
        xml = xml_preabmle + fdr_xml + sd_xml + xml_postamble  # Complete XML string.

        f = open(xmlfile, 'w')  # Serialize the XML string
        f.write(xml)
        f.close()

        af = open(affiliationsfile, 'w') # Serialize the unique affiliations
        unique_affiliations_list.sort
        for affiliation in unique_affiliations_list:
            af.write(affiliation + '\n')
        af.close()
        logmessage = "Update complete."
        print logmessage
        logger.info(logmessage)

    except Exception as e:
        print (e)
        # successful sending of email necessitated disabling McAfee email rule
        import smtplib
        from email.mime.text import MIMEText
        msg = MIMEText('The HR data serialization script has failed on lib-symeldata.')
        sender = 'jjim.tuttle@duke.edu'
        recipient = 'elements@duke.edu'
        msg['Subject'] = 'HR data failed on Elements development'
        msg['From'] = 'jim.tuttle@duke.edu'
        msg['To'] = 'jim.tuttle@duke.edu'
        s = smtplib.SMTP('smtp.duke.edu', '587')
        s.sendmail(sender, [recipient], msg.as_string())
        s.quit()
