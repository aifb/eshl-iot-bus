# -*- coding: utf-8 -*-
"""
Created on 25.11.2016

Topic: eshl.wago.v1.readout.wiz.495.1

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
    Reads the AC-Values of the WAGO 750-495 I/O Clamps every second via Modbus
    and publishes them to the topic eshl/eshl.wago.v2.readout.meter.494
    """
    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)
        self.pfcIp = '192.168.1.50'
        self.modbusTcpPort = 502
        self.numberOfClamps = 1
        self.client = ModbusClient(self.pfcIp, self.modbusTcpPort)
        self.clamps = []
        for i in range(0, self.numberOfClamps):
            self.clamps.append('Clamp' + str(i + 1))

#==============================================================================
#   Generates a blank Dataset with the current system timestamp
#==============================================================================

    def blankDataSetGen(self):
        blankDataset = {'U1': 'NaN',
                        'U2': 'NaN',
                        'U3': 'NaN',
                        'I1': 'NaN',
                        'I2': 'NaN',
                        'I3': 'NaN',
                        'P1': 'NaN',
                        'P2': 'NaN',
                        'P3': 'NaN',
                        'Q1': 'NaN',
                        'Q2': 'NaN',
                        'Q3': 'NaN',
                        'S1': 'NaN',
                        'S2': 'NaN',
                        'S3': 'NaN',
                        'CosPhi1': 'NaN',
                        'CosPhi2': 'NaN',
                        'CosPhi3': 'NaN',
                        'PF1': 'NaN',
                        'PF2': 'NaN',
                        'PF3': 'NaN',
                        'Qua1': 'NaN',
                        'Qua2': 'NaN',
                        'Qua3': 'NaN',
                        'f1': 'NaN',
                        'f2': 'NaN',
                        'f3': 'NaN',
                        'PTotal': 'NaN',
                        'QTotal': 'NaN',
                        'STotal': 'NaN',
                        'PFTotal': 'NaN',
                        'INeutral': 'NaN',
                        'AEI1': 'NaN',
                        'AED1': 'NaN',
                        'REI1': 'NaN',
                        'REC1': 'NaN',
                        'AEI2': 'NaN',
                        'AED2': 'NaN',
                        'REI2': 'NaN',
                        'REC2': 'NaN',
                        'AEI3': 'NaN',
                        'AED3': 'NaN',
                        'REI3': 'NaN',
                        'REC3': 'NaN',
                        'DataValid': '0',
                        'TimestampPFC': 'NaN',
                        'TimestampSYS': round(time.time() * 1000)}
        return blankDataset

    def requestLoop(self):

        numberOfRegPerClamp = 90  # max number of holding registers is 125 as the total number of bytes incl. CRC is 256 according to the spec (via 03 command)
        meterreadings = {}
        print(time.time())

        try:
            #self.client = ModbusClient(self.pfcIp, self.modbusTcpPort)
            if (self.client.connect() is False):
                print('not connected')
                self.client = self.client.connect()
                print('trying to connecto to ' + str(self.pfcIp))
            address = 518
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
#		It is not possible to read all registers in one request because of the limitation of the Modbus-Message size to 255kb
# 		When the results of all clamps are buffered, they are published
#==============================================================================
            for x in range(0, len(self.clamps)):
                result = self.client.read_holding_registers(address, numberOfRegPerClamp)

                decoder = BinaryPayloadDecoder.fromRegisters(result.registers, endian=Endian.Little)
                decoded = {
                    'I1': decoder.decode_32bit_float(),
                    'I2': decoder.decode_32bit_float(),
                    'I3': decoder.decode_32bit_float(),
                    'U1': decoder.decode_32bit_float(),
                    'U2': decoder.decode_32bit_float(),
                    'U3': decoder.decode_32bit_float(),
                    'P1': decoder.decode_32bit_float(),
                    'P2': decoder.decode_32bit_float(),
                    'P3': decoder.decode_32bit_float(),
                    'Q1': decoder.decode_32bit_float(),
                    'Q2': decoder.decode_32bit_float(),
                    'Q3': decoder.decode_32bit_float(),
                    'S1': decoder.decode_32bit_float(),
                    'S2': decoder.decode_32bit_float(),
                    'S3': decoder.decode_32bit_float(),
                    'CosPhi1': decoder.decode_32bit_float(),
                    'CosPhi2': decoder.decode_32bit_float(),
                    'CosPhi3': decoder.decode_32bit_float(),
                    'PF1': decoder.decode_32bit_float(),
                    'PF2': decoder.decode_32bit_float(),
                    'PF3': decoder.decode_32bit_float(),
                    'Qua1': decoder.decode_32bit_float(),
                    'Qua2': decoder.decode_32bit_float(),
                    'Qua3': decoder.decode_32bit_float(),
                    'f1': decoder.decode_32bit_float(),
                    'f2' : decoder.decode_32bit_float(),
                    'f3': decoder.decode_32bit_float(),
                    'PTotal': decoder.decode_32bit_float(),
                    'QTotal' : decoder.decode_32bit_float(),
                    'STotal': decoder.decode_32bit_float(),
                    'PFTotal': decoder.decode_32bit_float(),
                    'INeutral': decoder.decode_32bit_float(),
                    'AEI1': decoder.decode_32bit_float(),
                    'AED1': decoder.decode_32bit_float(),
                    'REI1': decoder.decode_32bit_float(),
                    'REC1': decoder.decode_32bit_float(),
                    'AEI2': decoder.decode_32bit_float(),
                    'AED2': decoder.decode_32bit_float(),
                    'REI2': decoder.decode_32bit_float(),
                    'REC2': decoder.decode_32bit_float(),
                    'AEI3': decoder.decode_32bit_float(),
                    'AED3': decoder.decode_32bit_float(),
                    'REI3': decoder.decode_32bit_float(),
                    'REC3': decoder.decode_32bit_float(),
                    'DataValid': decoder.decode_32bit_float()}
#==============================================================================
#        standardize both TimestampPFC and TimestampSYS precision to be millisecond
#==============================================================================
                decoded['TimestampPFC'] = str(timestampPFC)[0:13]
                decoded['TimestampSYS'] = timestampSys

#==============================================================================
#                 PFC measures energy values in mWh --> convert to watt-seconds
#==============================================================================
                decoded['AEI1'] = float(decoded['AEI1']) * 3.6
                decoded['AED1'] = float(decoded['AED1']) * 3.6
                decoded['REI1'] = float(decoded['REI1']) * 3.6
                decoded['REC1'] = float(decoded['REC1']) * 3.6
                decoded['AEI2'] = float(decoded['AEI2']) * 3.6
                decoded['AED2'] = float(decoded['AED2']) * 3.6
                decoded['REI2'] = float(decoded['REI2']) * 3.6
                decoded['REC2'] = float(decoded['REC2']) * 3.6
                decoded['AEI3'] = float(decoded['AEI3']) * 3.6
                decoded['AED3'] = float(decoded['AED3']) * 3.6
                decoded['REI3'] = float(decoded['REI3']) * 3.6
                decoded['REC3'] = float(decoded['REC3']) * 3.6

                meterreadings[self.clamps[x]] = decoded
                address += numberOfRegPerClamp

            self.publish(u'eshl.wago.v1.readout.wiz.495.1', json.dumps(meterreadings, sort_keys=True))
            self.publish(u'eshl.wago.v2.readout.wiz.495.1', meterreadings)
#==============================================================================
#      If there is no connection to the pfc-modbus slave or no connection to the pfc at all
#      the blankDataSet is published
#==============================================================================
        except ConnectionException as connErr:
            for x in range(0, len(self.clamps)):
                        meterreadings[self.clamps[x]] = self.blankDataSetGen()
            self.publish(u'eshl.wago.v1.readout.wiz.495.1', json.dumps(meterreadings, sort_keys=True))
            self.publish(u'eshl.wago.v2.readout.wiz.495.1', meterreadings)
            sys.__stdout__.write('ConnectionException in-wago-wiz-495-1' + '\n' + 'Timestamp: ' + str(timestampSys) + ', Errorcode --> ' + str(connErr))
            sys.__stdout__.flush()
        except Exception as err:
            sys.__stdout__.write('Exception in-wago-wiz-495-1' + '\n' + 'Timestamp: ' + str(timestampSys) + ', Errorcode --> ' + str(err))
            sys.__stdout__.flush()

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
