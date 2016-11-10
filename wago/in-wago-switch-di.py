# -*- coding: utf-8 -*-
"""
Created on 19.05.2016

Topic: eshl.wago.v1.readout.switch.di

@author: florian fiebig
@author: kaibin bao
"""

import sys
import traceback
import time

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner
from twisted.internet import task

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pymodbus.exceptions import ConnectionException

from config import *


class InModbus(ApplicationSession):
    """
    Continuously checks the state of the digital inputs via Modbus and
    publishes those if either any change is detected or 60 seconds have passed
    """
    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)
        self.pfcIp = '192.168.1.52'
        self.modbusTcpPort = '502'
        self.client = ModbusClient(self.pfcIp, self.modbusTcpPort)
        self.oldState = {}
        self.elapsedTime = 0

    def requestLoop(self):

        meterreadings = {}

        try:

            if (self.client.connect() is False):
                print('not connected')
                self.client = self.client.connect()
                print('trying to connecto to ' + str(self.pfcIp))

            result = self.client.read_coils(0, 15)
            diState = {'T001_di_1_A_0': result.bits[0],
                       'T002_di_1_B_2': result.bits[1],
                       'T003_di_1_C_4': result.bits[2],
                       'T004_di_1_D_6': result.bits[3],
                       'T005_di_1_E_1': result.bits[4],
                       'T006_di_1_F_3': result.bits[5],
                       'T007_di_1_G_5': result.bits[6],
                       'T008_di_1_H_7': result.bits[7],
                       'T009_di_2_B_2': result.bits[8],
                       'T013_di_2_C_4': result.bits[9],
                       'T014_di_2_D_6': result.bits[10],
                       'T015_di_2_E_1': result.bits[11],
                       'T016_di_2_F_3': result.bits[12],
                       'T016_di_2_G_5': result.bits[13],
                       'T018_di_2_H_7': result.bits[14]}

            timestampSys = round(time.time() * 1000)
            result2 = self.client.read_holding_registers(1, 36)
            decoder = BinaryPayloadDecoder.fromRegisters(result2.registers, endian=Endian.Little)
            decoded = {'T001_di_1_A_0_RE': decoder.decode_16bit_uint(),
                       'T002_di_1_B_2_RE': decoder.decode_16bit_uint(),
                       'T003_di_1_C_4_RE': decoder.decode_16bit_uint(),
                       'T004_di_1_D_6_RE': decoder.decode_16bit_uint(),
                       'T005_di_1_E_1_RE': decoder.decode_16bit_uint(),
                       'T006_di_1_F_3_RE': decoder.decode_16bit_uint(),
                       'T007_di_1_G_5_RE': decoder.decode_16bit_uint(),
                       'T008_di_1_H_7_RE': decoder.decode_16bit_uint(),
                       'T009_di_2_B_2_RE': decoder.decode_16bit_uint(),
                       'T013_di_2_C_4_RE': decoder.decode_16bit_uint(),
                       'T014_di_2_D_6_RE': decoder.decode_16bit_uint(),
                       'T015_di_2_E_1_RE': decoder.decode_16bit_uint(),
                       'T016_di_2_F_3_RE': decoder.decode_16bit_uint(),
                       'T016_di_2_G_5_RE': decoder.decode_16bit_uint(),
                       'T018_di_2_H_7_RE': decoder.decode_16bit_uint(),
                       'T001_di_1_A_0_FE': decoder.decode_16bit_uint(),
                       'T002_di_1_B_2_FE': decoder.decode_16bit_uint(),
                       'T003_di_1_C_4_FE': decoder.decode_16bit_uint(),
                       'T004_di_1_D_6_FE': decoder.decode_16bit_uint(),
                       'T005_di_1_E_1_FE': decoder.decode_16bit_uint(),
                       'T006_di_1_F_3_FE': decoder.decode_16bit_uint(),
                       'T007_di_1_G_5_FE': decoder.decode_16bit_uint(),
                       'T008_di_1_H_7_FE': decoder.decode_16bit_uint(),
                       'T009_di_2_B_2_FE': decoder.decode_16bit_uint(),
                       'T013_di_2_C_4_FE': decoder.decode_16bit_uint(),
                       'T014_di_2_D_6_FE': decoder.decode_16bit_uint(),
                       'T015_di_2_E_1_FE': decoder.decode_16bit_uint(),
                       'T016_di_2_F_3_FE': decoder.decode_16bit_uint(),
                       'T016_di_2_G_5_FE': decoder.decode_16bit_uint(),
                       'T018_di_2_H_7_FE': decoder.decode_16bit_uint(),
                       'TimestampPFC': str(decoder.decode_64bit_uint())[0:13]}
            diDictComplete = {**diState, **decoded}
            diDictToCompare = diDictComplete.copy()
            diDictToCompare.pop('TimestampPFC', None)

            self.oldState.pop('TimestampPFC', None)
            self.oldState.pop('TimestampSYS', None)
            self.elapsedTime = self.elapsedTime + 1
            if (self.oldState != diDictToCompare) or (self.elapsedTime >= 60):
                self.elapsedTime = 0
                self.oldState = diDictToCompare
                diDictComplete['TimestampSYS'] = timestampSys
                self.publish(u'eshl.wago.v1.readout.switch.di', diDictComplete)

        except Exception as err:

            print("error: {}".format(err), file=sys.stdout)
            traceback.print_exc(file=sys.stdout)

    def onJoin(self, details):
        ApplicationSession.onJoin(self, details)
        print("session ready")

        self._loop = task.LoopingCall(self.requestLoop)
        self._loop.start(1.0)

    def onLeave(self, details):
        ApplicationSession.onLeave(self, details)
        print("leaving")

        if(hasattr(self, "_loop") and self._loop):
            self._loop.stop()

if __name__ == '__main__':
    runner = ApplicationRunner(url=wampRouterAddress, realm=wampRouterRealm)
    runner.run(InModbus, auto_reconnect=True)
