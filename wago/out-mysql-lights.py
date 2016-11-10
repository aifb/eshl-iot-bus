# -*- coding: utf-8 -*-
"""
Created on 23.06.2016

subscribed to: eshl.wago.v1.switch

@author: florian fiebig
@author: kaibin bao
"""

import sys
import traceback

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner

import time
import mysql.connector
from mysql.connector import errorcode

from config import *


class OutMysql(ApplicationSession):
    """
    Logs the state of the lights to the configured MySQL database
    """
    def __init__(self, config=None):
            ApplicationSession.__init__(self, config)
            self.connection = mysql.connector.connect(mysqlConfig)
            self.oldState = {}
            self.elapsedTime = 0
            self.errorNotificationInterval = 12  # hours'
            self.timestampOld = 0
            # need for initial comparison


#==============================================================================
# Create SWITCH_Lights Table if it does note exist
#==============================================================================

    def createTableStmt(self, pfc):
        tables = {}
        tableString1 = 'CREATE TABLE IF NOT EXISTS' + " `{}` ".format(pfc) + '('
        tables['{}'.format(pfc)] = (tableString1 +
                                    " `TimestampSYS` bigint NOT NULL,"
                                    " `K16` smallint,"
                                    " `K17` smallint,"
                                    " `K18` smallint,"
                                    " `K19` smallint,"
                                    " `K20` smallint,"
                                    " `K21` smallint,"
                                    " PRIMARY KEY (`TimestampSYS`)"
                                    ") ENGINE=MyISAM")

        return tables['{}'.format(pfc)]

#==============================================================================
# Generates the acutal mysql-statement and fills it with it with current values
#==============================================================================

    def prepareInsertStmt(self, pfc):
        sql = {}
        sqlString1 = "INSERT INTO {} ".format(pfc)
        sql = (sqlString1 +
               "(TimestampSYS,K16,K17,K18,K19,K20,K21) "
               "VALUES (%(TimestampSYS)s,%(K16)s,%(K17)s,%(K18)s,%(K19)s,%(K20)s,%(K21)s)")

        return sql

    @inlineCallbacks
    def onJoin(self, details):
        print("session ready")

        def onWagoReadout(readout):
            timestampNow = round(time.time())
            readoutWithoutTimestamp = readout.copy()
            readoutWithoutTimestamp.pop("TimestampSYS", None)
            self.elapsedTime = self.elapsedTime + 1
            if (self.oldState == readoutWithoutTimestamp) and (self.elapsedTime <= 59):
                pass

            elif (self.elapsedTime >= 60) or (self.oldState != readoutWithoutTimestamp):
                self.elapsedTime = 0
                self.oldState = readoutWithoutTimestamp.copy()

                try:
                    if not self.connection.is_connected():
                        self.connection.close()  # This method tries to send a QUIT command and close the socket. It raises no exceptions.
                        self.connection = mysql.connector.connect(**self.mysqlConfig)
                    cursor = self.connection.cursor()
                    cursor.execute(self.createTableStmt(self.pfc))
                    cursor.execute(self.prepareInsertStmt(self.pfc), readout)
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
                        if timestampNow - self.timestampOld > self.errorNotificationInterval * 3600 :
                            self.timestampOld = timestampNow
                            print(err)
                            sys.__stdout__.write('Error Notification Wago-MySQL-Modul SWITCH-Lights' + '\n' + 'Timestamp: ' + str(timestampNow) + 'Errorcode --> ' + str(err))
                            sys.__stdout__.flush()

        yield self.subscribe(onWagoReadout, u'eshl.wago.v1.switch')
        print("subscribed to topic 'eshl.wago.v1.switch'")

if __name__ == '__main__':
    runner = ApplicationRunner(url=wampRouterAddress, realm=wampRouterRealm)
    runner.run(OutMysql, auto_reconnect=True)
