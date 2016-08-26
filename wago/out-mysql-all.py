# -*- coding: utf-8 -*-
"""
Created on 15.05.2016

subscribed to: eshl.wago.v2.readout.meter.494
               eshl.wago.v2.readout.wiz.494
               eshl.wago.v2.readout.wiz.495.1
               eshl.wago.v2.readout.wiz.495.2

@author: florian fiebig
@author: kaibin bao
"""

from __future__ import print_function, absolute_import

import mysql.connector
from mysql.connector import errorcode

import sys
from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner

import time
import csv
import os.path

from config import *


class OutMysql(ApplicationSession):
    """
    Logs the metering data which is received by any of the subscribed topics
    to the configured MySQL database
    """
    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)

        self.connectionPool = mysql.connector.pooling.MySQLConnectionPool(pool_name="wagoMeteringConnectionPool",
                                                                          pool_size=8,
                                                                          **mysqlConfig)
#==============================================================================
# A notification will be written to sys.stdout every errorNotificationInterval hours
#==============================================================================
        self.errorNotificationInterval = 12  # hours
        self.timestampOld_Meter = 0
        self.timestampOld_WIZ_494 = 0
        self.timestampOld_WIZ_495_1 = 0
        self.timestampOld_WIZ_495_2 = 0

#==============================================================================
# label of the pfc/clamps for sql schema
#==============================================================================
        self.wagoMeterName = 'METER'
        self.wagoWizName = 'WIZ'
        self.wagoWizHizMeteringName = 'WIZ_HIZ'
        self.wagoWizHizHarmonicsName = 'WIZ_HIZ_Clamp09_Harmonics'

        self.wago494ClampStructure = ['TimestampSYS',
                                      'TimestampPFC',
                                      'I1',
                                      'I2',
                                      'I3',
                                      'U1',
                                      'U2',
                                      'U3',
                                      'P1',
                                      'P2',
                                      'P3',
                                      'Q1',
                                      'Q2',
                                      'Q3',
                                      'S1',
                                      'S2',
                                      'S3',
                                      'CosPhi1',
                                      'CosPhi2',
                                      'CosPhi3',
                                      'PF1',
                                      'PF2',
                                      'PF3',
                                      'Qua1',
                                      'Qua2',
                                      'Qua3',
                                      'AEI1',
                                      'AED1',
                                      'REI1',
                                      'REC1',
                                      'AEI2',
                                      'AED2',
                                      'REI2',
                                      'REC2',
                                      'AEI3',
                                      'AED3',
                                      'REI3',
                                      'REC3',
                                      'DataValid']
#==============================================================================
# wago495MeteringClampStructure is defined since the readout is published as dict-objects and those are not ordered in Python
#==============================================================================
        self.wago495MeteringClampStructure = ['TimestampSYS',
                                              'TimestampPFC',
                                              'I1',
                                              'I2',
                                              'I3',
                                              'U1',
                                              'U2',
                                              'U3',
                                              'P1',
                                              'P2',
                                              'P3',
                                              'Q1',
                                              'Q2',
                                              'Q3',
                                              'S1',
                                              'S2',
                                              'S3',
                                              'CosPhi1',
                                              'CosPhi2',
                                              'CosPhi3',
                                              'PF1',
                                              'PF2',
                                              'PF3',
                                              'Qua1',
                                              'Qua2',
                                              'Qua3',
                                              'f1',
                                              'f2',
                                              'f3',
                                              'PTotal',
                                              'QTotal',
                                              'STotal',
                                              'PFTotal',
                                              'INeutral',
                                              'AEI1',
                                              'AED1',
                                              'REI1',
                                              'REC1',
                                              'AEI2',
                                              'AED2',
                                              'REI2',
                                              'REC2',
                                              'AEI3',
                                              'AED3',
                                              'REI3',
                                              'REC3',
                                              'DataValid']

#==============================================================================
# wago495HarmonicsClampStructure is defined since the readout is published as dict-objects and those are not ordered in Python
#==============================================================================
        self.wago495HarmonicsClampStructure = ['TimestampSYS',
                                               'TimestampPFC',
                                               'L1_f1',
                                               'L1_f2',
                                               'L1_f3',
                                               'L1_f4',
                                               'L1_f5',
                                               'L1_f6',
                                               'L1_f7',
                                               'L1_f8',
                                               'L1_f9',
                                               'L1_f10',
                                               'L1_f11',
                                               'L1_f12',
                                               'L1_f13',
                                               'L1_f14',
                                               'L1_f15',
                                               'L1_f16',
                                               'L1_f17',
                                               'L1_f18',
                                               'L1_f19',
                                               'L1_f20',
                                               'L1_f21',
                                               'L1_f22',
                                               'L1_f23',
                                               'L1_f24',
                                               'L1_f25',
                                               'L1_f26',
                                               'L1_f27',
                                               'L2_f1',
                                               'L2_f2',
                                               'L2_f3',
                                               'L2_f4',
                                               'L2_f5',
                                               'L2_f6',
                                               'L2_f7',
                                               'L2_f8',
                                               'L2_f9',
                                               'L2_f10',
                                               'L2_f11',
                                               'L2_f12',
                                               'L2_f13',
                                               'L2_f14',
                                               'L2_f15',
                                               'L2_f16',
                                               'L2_f17',
                                               'L2_f18',
                                               'L2_f19',
                                               'L2_f20',
                                               'L2_f21',
                                               'L2_f22',
                                               'L2_f23',
                                               'L2_f24',
                                               'L2_f25',
                                               'L2_f26',
                                               'L2_f27',
                                               'L3_f1',
                                               'L3_f2',
                                               'L3_f3',
                                               'L3_f4',
                                               'L3_f5',
                                               'L3_f6',
                                               'L3_f7',
                                               'L3_f8',
                                               'L3_f9',
                                               'L3_f10',
                                               'L3_f11',
                                               'L3_f12',
                                               'L3_f13',
                                               'L3_f14',
                                               'L3_f15',
                                               'L3_f16',
                                               'L3_f17',
                                               'L3_f18',
                                               'L3_f19',
                                               'L3_f20',
                                               'L3_f21',
                                               'L3_f22',
                                               'L3_f23',
                                               'L3_f24',
                                               'L3_f25',
                                               'L3_f26',
                                               'L3_f27']

    def createTableWago494Stmt(self, pfcName, clamp):
        tables = {}
        tableString1 = 'CREATE TABLE IF NOT EXISTS' + " `{}_{}` ".format(pfcName, clamp) + '('
        tables['{}_{}'.format(pfcName, clamp)] = (
        tableString1 +
        " `TimestampSYS` bigint NOT NULL,"
        " `TimestampPFC` bigint NOT NULL,"
        " `I1` double,"
        " `I2` double,"
        " `I3` double,"
        " `U1` double,"
        " `U2` double,"
        " `U3` double,"
        " `P1` double,"
        " `P2` double,"
        " `P3` double,"
        " `Q1` double,"
        " `Q2` double,"
        " `Q3` double,"
        " `S1` double,"
        " `S2` double,"
        " `S3` double,"
        " `CosPhi1` double,"
        " `CosPhi2` double,"
        " `CosPhi3` double,"
        " `PF1` double,"
        " `PF2` double,"
        " `PF3` double,"
        " `Qua1` smallint,"
        " `Qua2` smallint,"
        " `Qua3` smallint,"
        " `AEI1` double,"
        " `AED1` double,"
        " `REI1` double,"
        " `REC1` double,"
        " `AEI2` double,"
        " `AED2` double,"
        " `REI2` double,"
        " `REC2` double,"
        " `AEI3` double,"
        " `AED3` double,"
        " `REI3` double,"
        " `REC3` double,"
        " `DataValid` smallint,"
        " PRIMARY KEY (`TimestampSys`)"
        ") ENGINE=MyISAM")

        return tables['{}_{}'.format(pfcName , clamp)]

#==============================================================================
# This function creates a table for each 750-495 clamp on the PFC if it is not already existent
#==============================================================================

    def createTableWago495MeteringStmt(self, wagoWizHizMeteringName, clamp):
        tables = {}
        tableString1 = 'CREATE TABLE IF NOT EXISTS' + " `{}_Clamp08_Neutral` ".format(wagoWizHizMeteringName) + '('
        tables['{}_Clamp08_Neutral'.format(wagoWizHizMeteringName)] = (
        tableString1 +
        " `TimestampSYS` bigint NOT NULL,"
        " `TimestampPFC` bigint NOT NULL,"
        " `I1` double,"
        " `I2` double,"
        " `I3` double,"
        " `U1` double,"
        " `U2` double,"
        " `U3` double,"
        " `P1` double,"
        " `P2` double,"
        " `P3` double,"
        " `Q1` double,"
        " `Q2` double,"
        " `Q3` double,"
        " `S1` double,"
        " `S2` double,"
        " `S3` double,"
        " `CosPhi1` double,"
        " `CosPhi2` double,"
        " `CosPhi3` double,"
        " `PF1` double,"
        " `PF2` double,"
        " `PF3` double,"
        " `Qua1` smallint,"
        " `Qua2` smallint,"
        " `Qua3` smallint,"
        " `f1` double,"
        " `f2` double,"
        " `f3` double,"
        " `PTotal` double,"
        " `QTotal` double,"
        " `STotal` double,"
        " `PFTotal` double,"
        " `INeutral` double,"
        " `AEI1` double,"
        " `AED1` double,"
        " `REI1` double,"
        " `REC1` double,"
        " `AEI2` double,"
        " `AED2` double,"
        " `REI2` double,"
        " `REC2` double,"
        " `AEI3` double,"
        " `AED3` double,"
        " `REI3` double,"
        " `REC3` double,"
        " `DataValid` smallint,"
        " PRIMARY KEY (`TimestampSys`)"
        ") ENGINE=MyISAM")

        return tables['{}_Clamp08_Neutral'.format(wagoWizHizMeteringName)]

    def createTableWago495HarmonicsStmt(self, wagoWizHizHarmonicsName):
        tables = {}
        tableString1 = 'CREATE TABLE IF NOT EXISTS' + " `{}` ".format(wagoWizHizHarmonicsName) + '('
        tables['{}'.format(wagoWizHizHarmonicsName)] = (
        tableString1 +
        " `TimestampSYS` bigint NOT NULL,"
        " `TimestampPFC` bigint NOT NULL,"
        " `L1_f1` double,"
        " `L1_f2` double,"
        " `L1_f3` double,"
        " `L1_f4` double,"
        " `L1_f5` double,"
        " `L1_f6` double,"
        " `L1_f7` double,"
        " `L1_f8` double,"
        " `L1_f9` double,"
        " `L1_f10` double,"
        " `L1_f11` double,"
        " `L1_f12` double,"
        " `L1_f13` double,"
        " `L1_f14` double,"
        " `L1_f15` double,"
        " `L1_f16` double,"
        " `L1_f17` double,"
        " `L1_f18` double,"
        " `L1_f19` double,"
        " `L1_f20` double,"
        " `L1_f21` double,"
        " `L1_f22` double,"
        " `L1_f23` double,"
        " `L1_f24` double,"
        " `L1_f25` double,"
        " `L1_f26` double,"
        " `L1_f27` double,"
        " `L2_f1` double,"
        " `L2_f2` double,"
        " `L2_f3` double,"
        " `L2_f4` double,"
        " `L2_f5` double,"
        " `L2_f6` double,"
        " `L2_f7` double,"
        " `L2_f8` double,"
        " `L2_f9` double,"
        " `L2_f10` double,"
        " `L2_f11` double,"
        " `L2_f12` double,"
        " `L2_f13` double,"
        " `L2_f14` double,"
        " `L2_f15` double,"
        " `L2_f16` double,"
        " `L2_f17` double,"
        " `L2_f18` double,"
        " `L2_f19` double,"
        " `L2_f20` double,"
        " `L2_f21` double,"
        " `L2_f22` double,"
        " `L2_f23` double,"
        " `L2_f24` double,"
        " `L2_f25` double,"
        " `L2_f26` double,"
        " `L2_f27` double,"
        " `L3_f1` double,"
        " `L3_f2` double,"
        " `L3_f3` double,"
        " `L3_f4` double,"
        " `L3_f5` double,"
        " `L3_f6` double,"
        " `L3_f7` double,"
        " `L3_f8` double,"
        " `L3_f9` double,"
        " `L3_f10` double,"
        " `L3_f11` double,"
        " `L3_f12` double,"
        " `L3_f13` double,"
        " `L3_f14` double,"
        " `L3_f15` double,"
        " `L3_f16` double,"
        " `L3_f17` double,"
        " `L3_f18` double,"
        " `L3_f19` double,"
        " `L3_f20` double,"
        " `L3_f21` double,"
        " `L3_f22` double,"
        " `L3_f23` double,"
        " `L3_f24` double,"
        " `L3_f25` double,"
        " `L3_f26` double,"
        " `L3_f27` double,"
        " PRIMARY KEY (`TimestampSys`)"
        ") ENGINE=MyISAM")

        return tables['{}'.format(wagoWizHizHarmonicsName)]

#==============================================================================
# Actual mysql-statement which is executed for every clamp
#==============================================================================

    def prepareInsert494Stmt(self, pfcName, clamp):
        sql = {}
        sqlString1 = "INSERT INTO {}_{}".format(pfcName, clamp)
        sql = (sqlString1 +
               "(TimestampSYS, TimestampPFC, I1, I2, I3, U1, U2, U3, P1, P2, P3, Q1, Q2, Q3, S1, S2, S3, CosPhi1, CosPhi2, CosPhi3, PF1, PF2, PF3, Qua1, Qua2, Qua3, AEI1, AED1, REI1, REC1, AEI2, AED2, REI2, REC2, AEI3, AED3, REI3, REC3, DataValid) "
               "VALUES (%(TimestampSYS)s, %(TimestampPFC)s, %(I1)s, %(I2)s, %(I3)s, %(U1)s, %(U2)s, %(U3)s, %(P1)s, %(P2)s, %(P3)s, %(Q1)s, %(Q2)s, %(Q3)s, %(S1)s, %(S2)s, %(S3)s, %(CosPhi1)s, %(CosPhi2)s, %(CosPhi3)s, %(PF1)s, %(PF2)s, %(PF3)s, %(Qua1)s, %(Qua2)s, %(Qua3)s, %(AEI1)s, %(AED1)s, %(REI1)s, %(REC1)s, %(AEI2)s, %(AED2)s, %(REI2)s, %(REC2)s, %(AEI3)s, %(AED3)s, %(REI3)s, %(REC3)s, %(DataValid)s)")

        return sql

    def prepareInsert495MeteringStmt(self, wagoWizHizMeteringName, clamp):
        sql = {}
        sqlString1 = "INSERT INTO {}_Clamp08_Neutral ".format(wagoWizHizMeteringName)
        sql = (sqlString1 +
               "(TimestampSYS, TimestampPFC, I1, I2, I3, U1, U2, U3, P1, P2, P3, Q1, Q2, Q3, S1, S2, S3, CosPhi1, CosPhi2, CosPhi3, PF1, PF2, PF3, Qua1, Qua2, Qua3, f1, f2, f3, PTotal, QTotal, STotal, PFTotal, INeutral, AEI1, AED1, REI1, REC1, AEI2, AED2, REI2, REC2, AEI3, AED3, REI3, REC3, DataValid) "
               "VALUES (%(TimestampSYS)s, %(TimestampPFC)s, %(I1)s, %(I2)s, %(I3)s, %(U1)s, %(U2)s, %(U3)s, %(P1)s, %(P2)s, %(P3)s, %(Q1)s, %(Q2)s, %(Q3)s, %(S1)s, %(S2)s, %(S3)s, %(CosPhi1)s, %(CosPhi2)s, %(CosPhi3)s, %(PF1)s, %(PF2)s, %(PF3)s, %(Qua1)s, %(Qua2)s, %(Qua3)s, %(f1)s, %(f2)s, %(f3)s, %(PTotal)s, %(QTotal)s, %(STotal)s, %(PFTotal)s, %(INeutral)s, %(AEI1)s, %(AED1)s, %(REI1)s, %(REC1)s, %(AEI2)s, %(AED2)s, %(REI2)s, %(REC2)s, %(AEI3)s, %(AED3)s, %(REI3)s, %(REC3)s, %(DataValid)s)")
        return sql

    def prepareInsert495HarmonicsStmt(self, wagoWizHizHarmonicsName):
        sql = {}
        sqlString1 = "INSERT INTO {} ".format(wagoWizHizHarmonicsName)
        sql = (sqlString1 +
               "(TimestampSYS, TimestampPFC, L1_f1, L1_f2, L1_f3, L1_f4, L1_f5, L1_f6, L1_f7, L1_f8, L1_f9, L1_f10, L1_f11, L1_f12, L1_f13, L1_f14, L1_f15, L1_f16, L1_f17, L1_f18, L1_f19, L1_f20, L1_f21, L1_f22, L1_f23, L1_f24, L1_f25, L1_f26, L1_f27, L2_f1, L2_f2, L2_f3, L2_f4, L2_f5, L2_f6, L2_f7, L2_f8, L2_f9, L2_f10, L2_f11, L2_f12, L2_f13, L2_f14, L2_f15, L2_f16, L2_f17, L2_f18, L2_f19, L2_f20, L2_f21, L2_f22, L2_f23, L2_f24, L2_f25, L2_f26, L2_f27, L3_f1, L3_f2, L3_f3, L3_f4, L3_f5, L3_f6, L3_f7, L3_f8, L3_f9, L3_f10, L3_f11, L3_f12, L3_f13, L3_f14, L3_f15, L3_f16, L3_f17, L3_f18, L3_f19, L3_f20, L3_f21, L3_f22, L3_f23, L3_f24, L3_f25, L3_f26, L3_f27) "
               "VALUES (%(TimestampSYS)s, %(TimestampPFC)s, %(L1_f1)s, %(L1_f2)s, %(L1_f3)s, %(L1_f4)s, %(L1_f5)s, %(L1_f6)s, %(L1_f7)s, %(L1_f8)s, %(L1_f9)s, %(L1_f10)s, %(L1_f11)s, %(L1_f12)s, %(L1_f13)s, %(L1_f14)s, %(L1_f15)s, %(L1_f16)s, %(L1_f17)s, %(L1_f18)s, %(L1_f19)s, %(L1_f20)s, %(L1_f21)s, %(L1_f22)s, %(L1_f23)s, %(L1_f24)s, %(L1_f25)s, %(L1_f26)s, %(L1_f27)s, %(L2_f1)s, %(L2_f2)s, %(L2_f3)s, %(L2_f4)s, %(L2_f5)s, %(L2_f6)s, %(L2_f7)s, %(L2_f8)s, %(L2_f9)s, %(L2_f10)s, %(L2_f11)s, %(L2_f12)s, %(L2_f13)s, %(L2_f14)s, %(L2_f15)s, %(L2_f16)s, %(L2_f17)s, %(L2_f18)s, %(L2_f19)s, %(L2_f20)s, %(L2_f21)s, %(L2_f22)s, %(L2_f23)s, %(L2_f24)s, %(L2_f25)s, %(L2_f26)s, %(L2_f27)s, %(L3_f1)s, %(L3_f2)s, %(L3_f3)s, %(L3_f4)s, %(L3_f5)s, %(L3_f6)s, %(L3_f7)s, %(L3_f8)s, %(L3_f9)s, %(L3_f10)s, %(L3_f11)s, %(L3_f12)s, %(L3_f13)s, %(L3_f14)s, %(L3_f15)s, %(L3_f16)s, %(L3_f17)s, %(L3_f18)s, %(L3_f19)s, %(L3_f20)s, %(L3_f21)s, %(L3_f22)s, %(L3_f23)s, %(L3_f24)s, %(L3_f25)s, %(L3_f26)s, %(L3_f27)s)")
        return sql

    @inlineCallbacks
    def onJoin(self, details):
        print("session ready")


#==============================================================================
# onWago494Readout is executed everytime something is published to 'eshl.wago.v2.readout.wiz.494'
#==============================================================================

        def onWagoMeter494Readout(readout):

            clamps = []

            for index in readout:
                clamps.append(index)

            timestampNow = round(time.time() * 1000)

            try:
                connection = self.connectionPool.get_connection()

                cursor = connection.cursor()
                for index in readout:
                    cursor.execute(self.createTableWago494Stmt(self.wagoMeterName, index))
                for index in readout:
                    cursor.execute(self.prepareInsert494Stmt(self.wagoMeterName, index), readout[index])
                connection.commit()
                # connection.close() does not close the connection but returns it to the connection pool
                cursor.close()
                connection.close()
#==============================================================================
# Error-Handling
#==============================================================================
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Something is wrong with your user name or password")
                elif err.errno == errorcode.CR_CONN_HOST_ERROR:
                    print("hosterror: " + str(err) + "\n")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    print("Database does not exist")
                for index in readout:
                    csv_exists = os.path.isfile(self.wagoMeterName + str(index) + '.csv')
                    with open(self.wagoMeterName + str(index) + '.csv', 'a', newline='') as csvfile:
                        fieldnames = self.wago494ClampStructure
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        if not csv_exists:
                            writer.writeheader()
                        writer.writerow(readout[index])

                if timestampNow - self.timestampOld_Meter > self.errorNotificationInterval * 3600000 :
                    self.timestampOld_Meter = timestampNow
                    sys.__stdout__.write('Error Notification Wago-MySQL-Modul METER' +
                                         '\n' + 'Timestamp: ' + str(timestampNow) +
                                         ' Errorcode --> ' + str(err) + "\n")
                    sys.__stdout__.flush()

                    # todo: buffer für x Werte, wenn connection wieder da, Werte in mysql schreiben.
                #connection.close()
        yield self.subscribe(onWagoMeter494Readout, u'eshl.wago.v2.readout.meter.494')
        print("subscribed to topic 'eshl.wago.v2.readout.meter.494'")

#==============================================================================
# onWago494Readout is executed everytime something is published to 'eshl.wago.v2.readout.wiz.494'
#==============================================================================

        def onWagoWiz494Readout(readout):

            clamps = []

            for index in readout:
                clamps.append(index)

            timestampNow = round(time.time() * 1000)

            try:
                connection = self.connectionPool.get_connection()

                cursor = connection.cursor()
                for index in readout:
                    cursor.execute(self.createTableWago494Stmt(self.wagoWizName, index))

                for index in readout:
                    cursor.execute(self.prepareInsert494Stmt(self.wagoWizName, index), readout[index])
                connection.commit()
                # connection.close() does not close the connection but returns it to the connection pool
                cursor.close()
                connection.close()
#==============================================================================
# Error-Handling
#==============================================================================
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Something is wrong with your user name or password")
                elif err.errno == errorcode.CR_CONN_HOST_ERROR:
                    print("hosterror: " + str(err) + "\n")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    print("Database does not exist")
                for index in readout:
                    csv_exists = os.path.isfile(self.wagoWizName + str(index) + '.csv')
                    with open(self.wagoWizName + str(index) + '.csv', 'a', newline='') as csvfile:
                        fieldnames = self.wago494ClampStructure
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        if not csv_exists:
                            writer.writeheader()
                        writer.writerow(readout[index])

                if timestampNow - self.timestampOld_WIZ_494 > self.errorNotificationInterval * 3600000 :
                    self.timestampOld_WIZ_494 = timestampNow
                    sys.__stdout__.write('Error Notification Wago-MySQL-Modul WIZ 494' +
                                         '\n' + 'Timestamp: ' + str(timestampNow) +
                                         ' Errorcode --> ' + str(err) + "\n")
                    sys.__stdout__.flush()

                    # todo: buffer für x Werte, wenn connection wieder da, Werte in mysql schreiben.
                #connection.close()

        yield self.subscribe(onWagoWiz494Readout, u'eshl.wago.v2.readout.wiz.494')
        print("subscribed to topic 'eshl.wago.v2.readout.wiz.494'")

#==============================================================================
# onWago495MeteringReadout is executed everytime a data set is published to 'eshl.wago.v2.readout.wiz.495.1'
#==============================================================================

        def onWago495MeteringReadout(readout):
            clamps = []

            for index in readout:
                clamps.append(index)

            timestampNow = round(time.time() * 1000)

            try:
                connection = self.connectionPool.get_connection()
                cursor = connection.cursor()
                for index in readout:
                    cursor.execute(self.createTableWago495MeteringStmt(self.wagoWizHizMeteringName, index))

                for index in readout:
                    cursor.execute(self.prepareInsert495MeteringStmt(self.wagoWizHizMeteringName, index), readout[index])
                connection.commit()
                # connection.close() does not close the connection but returns it to the connection pool
                cursor.close()
                connection.close()
#==============================================================================
# Error-Handling
#==============================================================================
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Something is wrong with your user name or password")
                elif err.errno == errorcode.CR_CONN_HOST_ERROR:
                    print("hosterror: " + str(err) + "\n")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    print("Database does not exist")
                for index in readout:
                    csv_exists = os.path.isfile(self.wagoWizHizMeteringName + str(index) + '.csv')
                    with open(self.wagoWizHizMeteringName + str(index) + '.csv', 'a', newline='') as csvfile:
                        fieldnames = self.wago495MeteringClampStructure
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        if not csv_exists:
                            writer.writeheader()
                        writer.writerow(readout[index])

                if timestampNow - self.timestampOld_WIZ_495_1 > self.errorNotificationInterval * 3600000 :
                    self.timestampOld_WIZ_495_1 = timestampNow
                    sys.__stdout__.write('Error Notification Wago-MySQL-Modul WIZ 495 Metering' +
                                         '\n' + 'Timestamp: ' + str(timestampNow) +
                                         ' Errorcode --> ' + str(err) + "\n")
                    sys.__stdout__.flush()

                    # todo: buffer für x Werte, wenn connection wieder da, Werte in mysql schreiben.
                #connection.close()
        yield self.subscribe(onWago495MeteringReadout, u'eshl.wago.v2.readout.wiz.495.1')
        print("subscribed to topic 'eshl.wago.v2.readout.wiz.495.1'")

#==============================================================================
# onWago495HarmonicsReadout is executed everytime a data set is published to 'eshl.wago.v2.readout.wiz.495.2'
#==============================================================================

        def onWago495HarmonicsReadout(readout):
            clamps = []
            for index in readout:
                clamps.append(index)

            timestampNow = round(time.time() * 1000)

            try:
                connection = self.connectionPool.get_connection()

                cursor = connection.cursor()
                cursor.execute(self.createTableWago495HarmonicsStmt(self.wagoWizHizHarmonicsName))
                cursor.execute(self.prepareInsert495HarmonicsStmt(self.wagoWizHizHarmonicsName), readout['allPhases'])

                connection.commit()
                # connection.close() does not close the connection but returns it to the connection pool
                cursor.close()
                connection.close()
#==============================================================================
# Error-Handling
#==============================================================================
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Something is wrong with your user name or password")
                elif err.errno == errorcode.CR_CONN_HOST_ERROR:
                    print("hosterror: " + str(err) + "\n")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    print("Database does not exist")
                for index in readout:
                    csv_exists = os.path.isfile(self.wagoWizHizHarmonicsName + str(index) + '.csv')
                    with open(self.wagoWizHizHarmonicsName + str(index) + '.csv', 'a', newline='') as csvfile:
                        fieldnames = self.wago495HarmonicsClampStructure
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        if not csv_exists:
                            writer.writeheader()
                        writer.writerow(readout[index])

                if timestampNow - self.timestampOld_WIZ_495_2 > self.errorNotificationInterval * 3600000 :
                    self.timestampOld_WIZ_495_2 = timestampNow
                    sys.__stdout__.write('Error Notification Wago-MySQL-Modul WIZ 495 Harmonics' +
                                         '\n' + 'Timestamp: ' + str(timestampNow) +
                                         ' Errorcode --> ' + str(err) + "\n")
                    sys.__stdout__.flush()

                    # todo: buffer für x Werte, wenn connection wieder da, Werte in mysql schreiben.
                #connection.close()
        yield self.subscribe(onWago495HarmonicsReadout, u'eshl.wago.v2.readout.wiz.495.2')
        print("subscribed to topic 'eshl.wago.v2.readout.wiz.495.2'")


if __name__ == '__main__':
    runner = ApplicationRunner(url=wampRouterAddress, realm=wampRouterRealm)
    runner.run(OutMysql, auto_reconnect=True)
