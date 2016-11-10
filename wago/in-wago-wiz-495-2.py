# -*- coding: utf-8 -*-
"""
Created on 25.11.2016

Topic: eshl.wago.v1.readout.wiz.495.2

@author: florian fiebig
@author: kaibin bao
"""

import sys
import traceback
import time
import json

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner
from twisted.internet import task

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pymodbus.exceptions import ConnectionException

from config import *

timestampPFCRegister = 770


class InModbus(ApplicationSession):
    """
    Reads the Harmonics (f1- f27) of the WAGO 750-495 I/O Clamp every second
    via Modbus and publishes the values to the topic eshl/eshl.wago.v2.readout.meter.494
    """
    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)
        self.pfcIp = '192.168.1.50'
        self.modbusTcpPort = 502
        self.numberOfClamps = 7

        self.client = ModbusClient(self.pfcIp, self.modbusTcpPort)

        self.phases = ['L1_', 'L2_', 'L3_']


#==============================================================================
#   Generates a blank Dataset with the current system timestamp
#==============================================================================
    def blankDataSetGen(self):
        blankDataset = {'L1_f1' : 'NaN',
                        'L1_f2' : 'NaN',
                        'L1_f3' : 'NaN',
                        'L1_f4' : 'NaN',
                        'L1_f5' : 'NaN',
                        'L1_f6' : 'NaN',
                        'L1_f7' : 'NaN',
                        'L1_f8' : 'NaN',
                        'L1_f9' : 'NaN',
                        'L1_f10' : 'NaN',
                        'L1_f11' : 'NaN',
                        'L1_f12' : 'NaN',
                        'L1_f13' : 'NaN',
                        'L1_f14' : 'NaN',
                        'L1_f14' : 'NaN',
                        'L1_f15' : 'NaN',
                        'L1_f16' : 'NaN',
                        'L1_f17': 'NaN',
                        'L1_f18': 'NaN',
                        'L1_f19': 'NaN',
                        'L1_f20': 'NaN',
                        'L1_f21': 'NaN',
                        'L1_f22': 'NaN',
                        'L1_f23': 'NaN',
                        'L1_f24': 'NaN',
                        'L1_f25': 'NaN',
                        'L1_f26': 'NaN',
                        'L1_f27': 'NaN',
                        'L2_f1' : 'NaN',
                        'L2_f2' : 'NaN',
                        'L2_f3' : 'NaN',
                        'L2_f4' : 'NaN',
                        'L2_f5' : 'NaN',
                        'L2_f6' : 'NaN',
                        'L2_f7' : 'NaN',
                        'L2_f8' : 'NaN',
                        'L2_f9' : 'NaN',
                        'L2_f10' : 'NaN',
                        'L2_f11' : 'NaN',
                        'L2_f12' : 'NaN',
                        'L2_f13' : 'NaN',
                        'L2_f14' : 'NaN',
                        'L2_f14' : 'NaN',
                        'L2_f15' : 'NaN',
                        'L2_f16' : 'NaN',
                        'L2_f17': 'NaN',
                        'L2_f18': 'NaN',
                        'L2_f19': 'NaN',
                        'L2_f20': 'NaN',
                        'L2_f21': 'NaN',
                        'L2_f22': 'NaN',
                        'L2_f23': 'NaN',
                        'L2_f24': 'NaN',
                        'L2_f25': 'NaN',
                        'L2_f26': 'NaN',
                        'L2_f27': 'NaN',
                        'L3_f1' : 'NaN',
                        'L3_f2' : 'NaN',
                        'L3_f3' : 'NaN',
                        'L3_f4' : 'NaN',
                        'L3_f5' : 'NaN',
                        'L3_f6' : 'NaN',
                        'L3_f7' : 'NaN',
                        'L3_f8' : 'NaN',
                        'L3_f9' : 'NaN',
                        'L3_f10' : 'NaN',
                        'L3_f11' : 'NaN',
                        'L3_f12' : 'NaN',
                        'L3_f13' : 'NaN',
                        'L3_f14' : 'NaN',
                        'L3_f14' : 'NaN',
                        'L3_f15' : 'NaN',
                        'L3_f16' : 'NaN',
                        'L3_f17': 'NaN',
                        'L3_f18': 'NaN',
                        'L3_f19': 'NaN',
                        'L3_f20': 'NaN',
                        'L3_f21': 'NaN',
                        'L3_f22': 'NaN',
                        'L3_f23': 'NaN',
                        'L3_f24': 'NaN',
                        'L3_f25': 'NaN',
                        'L3_f26': 'NaN',
                        'L3_f27': 'NaN',
                        'TimestampPFC' : 'NaN',
                        'TimestampSYS' : round(time.time() * 1000)}
        return blankDataset

    def requestLoop(self):

        numberOfRegPerPhase = 54  # max number of holding registers is 125 as the total number of bytes incl. CRC is 256 according to the spec (via 03 command)
        meterreadings = {}
        print(time.time())

        try:
            #self.client = ModbusClient(self.pfcIp, self.modbusTcpPort)
            if (self.client.connect() is False):
                print('not connected')
                self.client = self.client.connect()
                print('trying to connecto to ' + str(self.pfcIp))
            address = 608
#==============================================================================
#       Read current timestamp from PFC
#==============================================================================
            timestampSys = round(time.time() * 1000)
            result2 = self.client.read_holding_registers(timestampPFCRegister, 4)
            decoder = BinaryPayloadDecoder.fromRegisters(result2.registers, endian=Endian.Little)
            timestampPFC = decoder.decode_64bit_int()

#==============================================================================
#       Reads the values from modbus registers clamp by clamp
#       and buffers the results in  meterreadings{}
#	   It is not possible to read all registers in one request because of the limitation of the Modbus-Message size to 255kb
# 	   When the results of all clamps are buffered, they are published
#==============================================================================
            meterreadings.clear()
            for x in range(0, len(self.phases)):
                result = self.client.read_holding_registers(address, numberOfRegPerPhase)

                decoder = BinaryPayloadDecoder.fromRegisters(result.registers, endian=Endian.Little)
                decoded = {
                    self.phases[x] + 'f1' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f2' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f3' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f4' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f5' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f6' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f7' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f8' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f9' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f10' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f11' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f12' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f13' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f14' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f15' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f16' : decoder.decode_32bit_float(),
                    self.phases[x] + 'f17': decoder.decode_32bit_float(),
                    self.phases[x] + 'f18': decoder.decode_32bit_float(),
                    self.phases[x] + 'f19': decoder.decode_32bit_float(),
                    self.phases[x] + 'f20': decoder.decode_32bit_float(),
                    self.phases[x] + 'f21': decoder.decode_32bit_float(),
                    self.phases[x] + 'f22': decoder.decode_32bit_float(),
                    self.phases[x] + 'f23': decoder.decode_32bit_float(),
                    self.phases[x] + 'f24': decoder.decode_32bit_float(),
                    self.phases[x] + 'f25': decoder.decode_32bit_float(),
                    self.phases[x] + 'f26': decoder.decode_32bit_float(),
                    self.phases[x] + 'f27': decoder.decode_32bit_float()}
#==============================================================================
#        standardize both TimestampPFC and TimestampSYS precision to be millisecond
#==============================================================================
                decoded['TimestampPFC'] = str(timestampPFC)[0:13]
                decoded['TimestampSYS'] = timestampSys

                meterreadings = {**meterreadings, **decoded}
                address += numberOfRegPerPhase

            meterreadings2 = {}
            meterreadings2['allPhases'] = meterreadings
            self.publish(u'eshl.wago.v1.readout.wiz.495.2', json.dumps(meterreadings2, sort_keys=True))
            self.publish(u'eshl.wago.v2.readout.wiz.495.2', meterreadings2)

#==============================================================================
#      If there is no connection to the pfc-modbus slave or no connection to the pfc at all
#      the blankDataSet is published
#==============================================================================
        except ConnectionException:
            for x in range(0, len(self.phases)):
                meterreadings[self.phases[x]] = self.blankDataSetGen()
            self.publish(u'eshl.wago.v1.readout.wiz.495.2', json.dumps(meterreadings, sort_keys=True))
            self.publish(u'eshl.wago.v2.readout.wiz.495.2', meterreadings)
            sys.__stdout__.write('ConnectionException in-wago-wiz-495-2' + '\n' + 'Timestamp: ' + str(timestampSys) + ', Errorcode --> ' + str(connErr))
            sys.__stdout__.flush()
        except Exception as err:
            sys.__stdout__.write('Exception in-wago-wiz-495-1' + '\n' + 'Timestamp: ' + str(timestampSys) + ', Errorcode --> ' + str(err))
            sys.__stdout__.flush()

    def onJoin(self, details):
        ApplicationSession.onJoin(self, details)
        print("session ready")

        self._loop = task.LoopingCall(self.requestLoop)
        self._loop.start(60.0)

    def onLeave(self, details):
        ApplicationSession.onLeave(self, details)
        print("leaving")

        if(hasattr(self, "_loop") and self._loop):
            self._loop.stop()

if __name__ == '__main__':
    runner = ApplicationRunner(url=wampRouterAddress, realm=wampRouterRealm)
    runner.run(InModbus, auto_reconnect=True)
