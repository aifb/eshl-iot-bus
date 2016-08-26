# -*- coding: utf-8 -*-
"""
Created on 19.05.2016

Topic: eshl.wago.v1.readout.switch.relays

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
    Continiously checks the state of the relays via Modbus and publishes
    those if either any change is detected or 60 seconds have passed
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
        print(time.time())

        try:

            if (self.client.connect() is False):
                print('not connected')
                self.client.connect()
                print('trying to connecto to ' + str(self.pfcIp))
#==============================================================================
#       Read current timestamp from PFC
#==============================================================================
            timestampSys = round(time.time() * 1000)
            result2 = self.client.read_holding_registers(32, 4)
            decoder = BinaryPayloadDecoder.fromRegisters(result2.registers, endian=Endian.Little)
            timestampPFC = decoder.decode_64bit_int()

            result = self.client.read_coils(0, 18)
            meterreadings = {'K_001': result.bits[0],
                             'K_002': result.bits[1],
                             'K_003': result.bits[2],
                             'K_004': result.bits[3],
                             'K_005': result.bits[4],
                             'K_006': result.bits[5],
                             'K_007': result.bits[6],
                             'K_008': result.bits[7],
                             'K_009': result.bits[8],
                             'K_010': result.bits[9],
                             'K_011': result.bits[10],
                             'K_012': result.bits[11],
                             'K_013': result.bits[12],
                             'K_014': result.bits[13],
                             'K_015': result.bits[14],
                             'Frei_001': result.bits[15],
                             'Frei_002': result.bits[16],
                             'Frei_003': result.bits[17]}
#==============================================================================
#        standardize both TimestampPFC and TimestampSYS precision to be millisecond
#==============================================================================
            meterreadingsToCompare = {}
            meterreadingsToCompare = meterreadings
            self.oldState.pop('TimestampPFC', None)
            self.oldState.pop('TimestampSYS', None)

            self.elapsedTime = self.elapsedTime + 1

            if (self.oldState != meterreadingsToCompare) or (self.elapsedTime >= 60):

                self.elapsedTime = 0
                self.oldState = meterreadingsToCompare
                meterreadings['TimestampPFC'] = str(timestampPFC)[0:13]
                meterreadings['TimestampSYS'] = timestampSys
                self.publish(u'eshl.wago.v1.readout.switch.relays', meterreadings)

#==============================================================================
#      If there is no connection to the pfc-modbus slave or no connection to the pfc at all
#      the blankDataSet is published
#==============================================================================
#==============================================================================
#         except ConnectionException as connErr:
#              for x in range(0, len(self.clamps)):
#                         meterreadings[self.clamps[x]] = json.dumps(self.blankDataSetGen(), sort_keys=True)
#              self.publish(u'org.eshl.wago.readout.meter.494', json.dumps(meterreadings,sort_keys=True))
#              print(str(connErr))
#==============================================================================
        except Exception as err:

            print("error: {}".format(err), file=sys.stdout)
            traceback.print_exc(file=sys.stdout)


#==============================================================================
#   Ensures that the requestLoop is called exactly once every second, sleeps for the rest of the time
#==============================================================================
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
