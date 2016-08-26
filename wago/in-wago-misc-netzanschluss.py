# -*- coding: utf-8 -*-
"""
Created on 25.11.2015

@author: florian fiebig
@author: kaibin bao
"""

import sys
import traceback

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner
from twisted.internet import task

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pymodbus.exceptions import ConnectionException

import time
import json

from config import *


class InModbus(ApplicationSession):
    """
    Continuously checks the state of the four quadrant chopper via Modbus and
    publishes those if either any change is detected or 60 seconds have passed
    """
    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)
        self.pfcIp = '192.168.1.53'
        self.modbusTcpPort = '502'
        self.client = ModbusClient(self.pfcIp, self.modbusTcpPort)
        self.oldState = {}
        self.elapsedTime = 0

    def requestLoop(self):

        meterreadings = {}

        try:
            if (self.client.connect() is False):
                print('not connected')
                self.client.connect()
                print('trying to connecto to ' + str(self.pfcIp))
#==============================================================================
#       Read current timestamp from PFC
#==============================================================================
            timestampSys = round(time.time() * 1000)
            result2 = self.client.read_holding_registers(4, 4)
            decoder = BinaryPayloadDecoder.fromRegisters(result2.registers, endian=Endian.Little)
            timestampPFC = decoder.decode_64bit_int()

#==============================================================================
#       Reads the values from modbus registers clamp by clamp
#       and buffers the results in  meterreadings{}
#		It is not possible to read all registers in one request because of the limitation of the Modbus-Message size to 255kb
# 		When the results of all clamps are buffered, they are published
#==============================================================================
            result = self.client.read_coils(16, 4)
            meterreadings = {'shStarr': result.bits[0],
                             'sh4QS': result.bits[1],
                             'speicherSh': result.bits[2],
                             'speicher4QS': result.bits[3]}
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
                meterreadings['TimestampSYS'] = round(time.time() * 1000)
                self.publish(u'eshl.wago.v1.readout.misc.4qs', json.dumps(meterreadings, sort_keys=True))
                self.publish(u'eshl.wago.v2.readout.misc.4qs', meterreadings)
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
