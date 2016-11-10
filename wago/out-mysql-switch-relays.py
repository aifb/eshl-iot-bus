# -*- coding: utf-8 -*-
"""
Created on 30.11.2015

subscribed to: eshl.wago.v1.readout.switch.relays

@author: florian fiebig
@author: kaibin bao
"""

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner

import time
import mysql.connector
from mysql.connector import errorcode

import sys
import csv
import os.path

from config import *


class OutMysql(ApplicationSession):
    """Logs the state of the relays to the configured MySQL database"""
    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)
        self.connection = mysql.connector.connect(**mysqlConfig)
#==============================================================================
# A notification email will be send every errorNotificationInterval hours
#==============================================================================
        self.pfc = 'SWITCH_Relays'
        self.errorNotificationInterval = 12  # hours'
        self.timestampOld = 0  # need for initial comparison

#=====================is=========================================================
# mySQLStructure  defined since the readout is published as dict-objects and those are not ordered in Python
#==============================================================================
        self.mySQLStructure = ['TimestampSYS',
                               'TimestampPFC',
                               'K_001',
                               'K_002',
                               'K_003',
                               'K_004',
                               'K_005',
                               'K_006',
                               'K_007',
                               'K_008',
                               'K_009',
                               'K_010',
                               'K_011',
                               'K_012',
                               'K_013',
                               'K_014',
                               'K_015',
                               'Frei_001',
                               'Frei_002',
                               'Frei_003']

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
        " `K_001` tinyint,"
        " `K_002` tinyint,"
        " `K_003` tinyint,"
        " `K_004` tinyint,"
        " `K_005` tinyint,"
        " `K_006` tinyint,"
        " `K_007` tinyint,"
        " `K_008` tinyint,"
        " `K_009` tinyint,"
        " `K_010` tinyint,"
        " `K_011` tinyint,"
        " `K_012` tinyint,"
        " `K_013` tinyint,"
        " `K_014` tinyint,"
        " `K_015` tinyint,"
        " `Frei_001` tinyint,"
        " `Frei_002` tinyint,"
        " `Frei_003` tinyint,"
        " PRIMARY KEY (`TimestampSys`)"
        ") ENGINE=MyISAM")

        return tables['{}'.format(pfc)]


#==============================================================================
# Actual mysql-statement which is executed for every clamp
#==============================================================================

    def prepareInsertStmt(self, pfc):
        sql = {}
        sqlString1 = "INSERT INTO {} ".format(pfc)
        sql = (sqlString1 +
               "(TimestampSYS, TimestampPFC, K_001, K_002, K_003, K_004, K_005, K_006, K_007, K_008, K_009, K_010, K_011, K_012, K_013, K_014, K_015, Frei_001, Frei_002, Frei_003) "
               "VALUES (%(TimestampSYS)s, %(TimestampPFC)s, %(K_001)s, %(K_002)s, %(K_003)s, %(K_004)s, %(K_005)s, %(K_006)s, %(K_007)s, %(K_008)s, %(K_009)s, %(K_010)s, %(K_011)s, %(K_012)s, %(K_013)s, %(K_014)s, %(K_015)s, %(Frei_001)s, %(Frei_002)s, %(Frei_003)s)")

        return sql

    @inlineCallbacks
    def onJoin(self, details):
        print("session ready")

#==============================================================================
# onWagoReadout is executed everytime something is published to the subscribed domain
#==============================================================================

        def onWagoReadout(readout):

            timestampNow = int(str(time.time())[0:10])

            try:
                if not self.connection.is_connected():
                    self.connection.close()  # This method tries to send a QUIT command and close the socket. It raises no exceptions.
                    self.connection = mysql.connector.connect(**self.mysqlConfig)

                cursor = self.connection.cursor()
                cursor.execute(self.createTableStmt(self.pfc))
                cursor.execute(self.prepareInsertStmt(self.pfc), readout)
                self.connection.commit()  # one commit per second with all sql-statements
                cursor.close()
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
                    with open(self.pfc + '.csv', 'a', newline='') as csvfile:
                        fieldnames = self.mySQLStructure
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        if not csv_exists:
                            writer.writeheader()
                        writer.writerow(readout)
#==============================================================================
# An Email is sent immediatly after failure and after that every errorNotificationInterval-hours
#==============================================================================
                    if timestampNow - self.timestampOld > self.errorNotificationInterval * 3600 :
                        self.timestampOld = timestampNow
                        sys.__stdout__.write('Error Notification Wago-MySQL-Modul Meter Relays' + '\n' + 'Timestamp: ' + str(timestampNow) + 'Errorcode --> ' + str(err))
                        sys.__stdout__.flush()

        yield self.subscribe(onWagoReadout, u'eshl.wago.v1.readout.switch.relays')
        print("subscribed to topic 'eshl.wago.v1.readout.switch.relays'")

if __name__ == '__main__':
    runner = ApplicationRunner(url=wampRouterAddress, realm=wampRouterRealm)
    runner.run(OutMysql, auto_reconnect=True)
