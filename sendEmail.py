#!/usr/bin/env python



import smtplib

from email.mime.text import MIMEText
msg = MIMEText('The HR data serialization script has failed on Elements production.')
msg['Subject'] = 'HR data failed on Elements production'
msg['From'] = 'elements@duke.edu'
msg['To'] = 'elements@duke.edu'
s = smtplib.SMTP('smtp.duke.edu', '587')
s.sendmail(sender, [recipient], msg.as_string())
s.quit()
