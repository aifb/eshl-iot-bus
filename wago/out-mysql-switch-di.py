# -*- coding: utf-8 -*-
"""
Created on 30.11.2015

subscribed to: eshl.wago.v1.readout.switch.di

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
    """Logs the state of the digital inputs to the configured MySQL database"""
    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)

        self.connection = mysql.connector.connect(**mysqlConfig)

#==============================================================================
# A notification email will be send every errorNotificationInterval hours
#==============================================================================
        self.pfc = 'SWITCH_DI'
        self.errorNotificationInterval = 12  # hours'
        self.timestampOld = 0  # need for initial comparison


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
            " `T001_di_1_A_0` tinyint,"
            " `T002_di_1_B_2` tinyint,"
            " `T003_di_1_C_4` tinyint,"
            " `T004_di_1_D_6` tinyint,"
            " `T005_di_1_E_1` tinyint,"
            " `T006_di_1_F_3` tinyint,"
            " `T007_di_1_G_5` tinyint,"
            " `T008_di_1_H_7` tinyint,"
            " `T009_di_2_B_2` tinyint,"
            " `T013_di_2_C_4` tinyint,"
            " `T014_di_2_D_6` tinyint,"
            " `T015_di_2_E_1` tinyint,"
            " `T016_di_2_F_3` tinyint,"
            " `T016_di_2_G_5` tinyint,"
            " `T018_di_2_H_7` tinyint,"
            " `T001_di_1_A_0_RE` smallint unsigned,"
            " `T002_di_1_B_2_RE` smallint unsigned,"
            " `T003_di_1_C_4_RE` smallint unsigned,"
            " `T004_di_1_D_6_RE` smallint unsigned,"
            " `T005_di_1_E_1_RE` smallint unsigned,"
            " `T006_di_1_F_3_RE` smallint unsigned,"
            " `T007_di_1_G_5_RE` smallint unsigned,"
            " `T008_di_1_H_7_RE` smallint unsigned,"
            " `T009_di_2_B_2_RE` smallint unsigned,"
            " `T013_di_2_C_4_RE` smallint unsigned,"
            " `T014_di_2_D_6_RE` smallint unsigned,"
            " `T015_di_2_E_1_RE` smallint unsigned,"
            " `T016_di_2_F_3_RE` smallint unsigned,"
            " `T016_di_2_G_5_RE` smallint unsigned,"
            " `T018_di_2_H_7_RE` smallint unsigned,"
            " `T001_di_1_A_0_FE` smallint unsigned,"
            " `T002_di_1_B_2_FE` smallint unsigned,"
            " `T003_di_1_C_4_FE` smallint unsigned,"
            " `T004_di_1_D_6_FE` smallint unsigned,"
            " `T005_di_1_E_1_FE` smallint unsigned,"
            " `T006_di_1_F_3_FE` smallint unsigned,"
            " `T007_di_1_G_5_FE` smallint unsigned,"
            " `T008_di_1_H_7_FE` smallint unsigned,"
            " `T009_di_2_B_2_FE` smallint unsigned,"
            " `T013_di_2_C_4_FE` smallint unsigned,"
            " `T014_di_2_D_6_FE` smallint unsigned,"
            " `T015_di_2_E_1_FE` smallint unsigned,"
            " `T016_di_2_F_3_FE` smallint unsigned,"
            " `T016_di_2_G_5_FE` smallint unsigned,"
            " `T018_di_2_H_7_FE` smallint unsigned,"
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
               "(TimestampSYS,TimestampPFC,T001_di_1_A_0,T002_di_1_B_2,T003_di_1_C_4,T004_di_1_D_6,T005_di_1_E_1,T006_di_1_F_3,T007_di_1_G_5,T008_di_1_H_7,T009_di_2_B_2,T013_di_2_C_4,T014_di_2_D_6,T015_di_2_E_1,T016_di_2_F_3,T016_di_2_G_5,T018_di_2_H_7,T001_di_1_A_0_RE,T002_di_1_B_2_RE,T003_di_1_C_4_RE,T004_di_1_D_6_RE,T005_di_1_E_1_RE,T006_di_1_F_3_RE,T007_di_1_G_5_RE,T008_di_1_H_7_RE,T009_di_2_B_2_RE,T013_di_2_C_4_RE,T014_di_2_D_6_RE,T015_di_2_E_1_RE,T016_di_2_F_3_RE,T016_di_2_G_5_RE,T018_di_2_H_7_RE,T001_di_1_A_0_FE,T002_di_1_B_2_FE,T003_di_1_C_4_FE,T004_di_1_D_6_FE,T005_di_1_E_1_FE,T006_di_1_F_3_FE,T007_di_1_G_5_FE,T008_di_1_H_7_FE,T009_di_2_B_2_FE,T013_di_2_C_4_FE,T014_di_2_D_6_FE,T015_di_2_E_1_FE,T016_di_2_F_3_FE,T016_di_2_G_5_FE,T018_di_2_H_7_FE) "
               "VALUES (%(TimestampSYS)s,%(TimestampPFC)s,%(T001_di_1_A_0)s,%(T002_di_1_B_2)s,%(T003_di_1_C_4)s,%(T004_di_1_D_6)s,%(T005_di_1_E_1)s,%(T006_di_1_F_3)s,%(T007_di_1_G_5)s,%(T008_di_1_H_7)s,%(T009_di_2_B_2)s,%(T013_di_2_C_4)s,%(T014_di_2_D_6)s,%(T015_di_2_E_1)s,%(T016_di_2_F_3)s,%(T016_di_2_G_5)s,%(T018_di_2_H_7)s,%(T001_di_1_A_0_RE)s,%(T002_di_1_B_2_RE)s,%(T003_di_1_C_4_RE)s,%(T004_di_1_D_6_RE)s,%(T005_di_1_E_1_RE)s,%(T006_di_1_F_3_RE)s,%(T007_di_1_G_5_RE)s,%(T008_di_1_H_7_RE)s,%(T009_di_2_B_2_RE)s,%(T013_di_2_C_4_RE)s,%(T014_di_2_D_6_RE)s,%(T015_di_2_E_1_RE)s,%(T016_di_2_F_3_RE)s,%(T016_di_2_G_5_RE)s,%(T018_di_2_H_7_RE)s,%(T001_di_1_A_0_FE)s,%(T002_di_1_B_2_FE)s,%(T003_di_1_C_4_FE)s,%(T004_di_1_D_6_FE)s,%(T005_di_1_E_1_FE)s,%(T006_di_1_F_3_FE)s,%(T007_di_1_G_5_FE)s,%(T008_di_1_H_7_FE)s,%(T009_di_2_B_2_FE)s,%(T013_di_2_C_4_FE)s,%(T014_di_2_D_6_FE)s,%(T015_di_2_E_1_FE)s,%(T016_di_2_F_3_FE)s,%(T016_di_2_G_5_FE)s,%(T018_di_2_H_7_FE)s)")

        return sql

    @inlineCallbacks
    def onJoin(self, details):
        print("session ready")

#==============================================================================
# onWagoReadout is executed everytime something is published to the subscribed domain
#==============================================================================

        def onWagoReadout(readout):
            #print('readout received:')

            timestampNow = round(time.time())

            if not self.connection.is_connected():
                self.connection.close()  # This method tries to send a QUIT command and close the socket. It raises no exceptions.
                self.connection = mysql.connector.connect(**self.mysqlConfig)

            try:

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
                        sys.__stdout__.write('Error Notification Wago-MySQL-Modul METER DI' + '\n' + 'Timestamp: ' + str(timestampNow) + 'Errorcode --> ' + str(err))
                        sys.__stdout__.flush()

        yield self.subscribe(onWagoReadout, u'eshl.wago.v1.readout.switch.di')
        print("subscribed to topic 'eshl.wago.v1.readout.switch.di'")

if __name__ == '__main__':
    runner = ApplicationRunner(url=wampRouterAddress, realm=wampRouterRealm)
    runner.run(OutMysql, auto_reconnect=True)
