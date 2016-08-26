# -*- coding: utf-8 -*-
"""
Created on 30.11.2015

subscribed to: eshl.wago.v1.readout.misc.4qs

@author: florian fiebig
@author: kaibin bao
"""

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner

import time
import mysql.connector
from mysql.connector import errorcode

import json
import csv
import os.path
import smtplib
from email.mime.text import MIMEText

from config import *


class payload(object):
    def __init__(self, j):
        if type(j) is str:
            self.__dict__ = json.loads(j)
        else:
            self.__dict__ = j


class OutMysql(ApplicationSession):
    """Logs the state of the four quadrant chopper to the configured MySQL database"""
    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)
        self.connection = mysql.connector.connect(mysqlConfig)
#==============================================================================
# A notification email will be send every errorNotificationInterval hours
#==============================================================================
        self.pfc = 'MISC_Netzanschluss'
        self.errorNotificationEmails = notificationMailAddresses
        self.errorNotificationInterval = 12  # hours'
        self.timestampOld = 0  # need for initial comparison

#=====================is=========================================================
# wagoClampStructure  defined since the readout is published as dict-objects and those are not ordered in Python
#==============================================================================
        self.wagoClampStructure = ['TimestampSYS',
                                   'TimestampPFC',
                                   'shStarr',
                                   'sh4QS',
                                   'speicherSh',
                                   'speicher4QS']

#==============================================================================
# This function creates a table for each clamp on the pfc if it is not already existent
#==============================================================================

    def createTableStmt(self, pfc):
        tables = {}
        tableString1 = 'CREATE TABLE IF NOT EXISTS' + " `{}` ".format(pfc) + '('
        tables['{}'.format(pfc)] = (
        tableString1 +
        " `TimestampSYS` bigint NOT NULL,"
        " `TimestampPFC` bigint NOT NULL,"
        " `shStarr` tinyint,"
        " `sh4QS` tinyint,"
        " `speicherSh` tinyint,"
        " `speicher4QS` tinyint,"
        " PRIMARY KEY (`TimestampSys`)"
        ") ENGINE=MyISAM")

        return tables['{}'.format(pfc)]

#==============================================================================
# The values of each clmap are extracted based on the attribute names defined in wagoClampStructure
#==============================================================================

    def extractClampData(self, readoutDesi, pfc):

        extractedData = {}
        for x in self.wagoClampStructure:
                extractedData[x] = readoutDesi.__getattribute__(x)
        print(extractedData)
        return extractedData

#==============================================================================
# Actual mysql-statement which is executed for every clamp
#==============================================================================

    def prepareInsertStmt(self, pfc):
        sql = {}
        sqlString1 = "INSERT INTO {} ".format(pfc)
        sql = (sqlString1 +
               "(TimestampSYS, TimestampPFC, shStarr, sh4QS, speicherSh, speicher4QS) "
               "VALUES (%(TimestampSYS)s, %(TimestampPFC)s, %(shStarr)s, %(sh4QS)s, %(speicherSh)s, %(speicher4QS)s)")

        return sql

    @inlineCallbacks
    def onJoin(self, details):
        print("session ready")

#==============================================================================
# onWagoReadout is executed everytime something is published to the subscribed domain
#==============================================================================

        def onWagoReadout(readout):
            print('readout received: ' + readout)

            readoutDeserialized = payload(readout)  # Deserialize erste Stufe
            timestampNow = round(time.time())

            try:
                if not self.connection.is_connected():
                    mysql.connector.connect(**self.mysqlConfig)

                cursor = self.connection.cursor()
                cursor.execute(self.createTableStmt(self.pfc))
                cursor.execute(self.prepareInsertStmt(self.pfc), self.extractClampData(readoutDeserialized, self.pfc))
                self.connection.commit()  # one commit per second with all sql-statements
                cursor.close()
                self.connection.close()
#==============================================================================
# Error-Handling
#==============================================================================
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Something is wrong with your user name or password")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    print("Database does not exist")
                else:
                    print(err)
                    print("Logging to CSVs instead")
                    csv_exists = os.path.isfile(self.pfc + '.csv')
                    with open(self.pfc + '_Clamp0' + '.csv', 'a', newline='') as csvfile:
                        fieldnames = self.wagoClampStructure
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        if not csv_exists:
                            writer.writeheader()
                        writer.writerow(self.extractClampData(readoutDeserialized, self.pfc))
#==============================================================================
# An Email is sent immediatly after failure and after that every errorNotificationInterval-hours
#==============================================================================
                    if timestampNow - self.timestampOld > self.errorNotificationInterval * 3600 :
                        self.timestampOld = timestampNow
                        email = open('Emailtext', 'w')
                        email.write('Timestamp: ')
                        email.write(str(timestampNow))
                        email.write('\n')
                        email.write("Errorcode --> " + str(err))
                        email.close()
                        with open('Emailtext', 'r') as fp:
                            msg = MIMEText(fp.read())
                            msg['Subject'] = 'Error Notification Wago-MySQL-Modul MISC 4QS'
                            msg['From'] = notificationMailSender
                            msg['To'] = self.errorNotificationEmails
                            s = smtplib.SMTP(notificationMailServer)
                            s.send_message(msg)
                            s.quit()

        yield self.subscribe(onWagoReadout, u'eshl.wago.v1.readout.misc.4qs')
        print("subscribed to topic 'eshl.wago.v1.readout.misc.4qs'")

if __name__ == '__main__':
    runner = ApplicationRunner(url=wampRouterAddress, realm=wampRouterRealm)
    runner.run(OutMysql, auto_reconnect=True)
