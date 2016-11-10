# -*- coding: utf-8 -*-
"""
Created on 23.06.2016

Topics: eshl.wago.v1.switch
RPCs:   eshl.wago.v1.switch.toggle
        eshl.wago.v1.switch.on
        eshl.wago.v1.switch.off

@author: florian fiebig
@author: kaibin bao
"""

import sys
import traceback
import time

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.util import sleep
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner
from twisted.internet import task

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pymodbus.exceptions import ConnectionException

from config import *


class InModbus(ApplicationSession):
    """
    Checks the state of the relays of the lights (eshl/eshl.wago.v1.switch) and
    makes three different RPCs to toggle/switch on/switch off the lights available.
    """
    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)
        self.pfcIp = '192.168.1.52'
        self.modbusTcpPort = 502
        self.client = ModbusClient(self.pfcIp, self.modbusTcpPort)

#==============================================================================
#   Generates a blank Dataset with the current system timestamp
#==============================================================================
    def blankDataSetGen(self):
        blankDataset = {'K16': None,
                        'K17': None,
                        'K18': None,
                        'K19': None,
                        'K20': None,
                        'K21': None,
                        'TimestampSYS' : round(time.time() * 1000)
                        }
        return blankDataset

    def requestLoop(self):
        try:
            if (self.client.connect() is False):
                print('not connected')
                self.client = self.client.connect()
                print('trying to connecto to ' + str(self.pfcIp))

            address = 0
            timestampSys = round(time.time() * 1000)

            result = self.client.read_coils(560, 6)
            relayState = {'K16': result.bits[0],
                           'K17': result.bits[1],
                           'K18': result.bits[2],
                           'K19': result.bits[3],
                           'K20': result.bits[4],
                           'K21': result.bits[5],
                           'TimestampSYS' : timestampSys}

            self.publish(u'eshl.wago.v1.switch', relayState)
        except ConnectionException as connErr:
            relayState = self.blankDataSetGen()
            self.publish(u'eshl.wago.v1.switch', relayState)
            print(str(connErr))
        except Exception as err:
            print("error: {}".format(err), file=sys.stdout)
            traceback.print_exc(file=sys.stdout)

    @inlineCallbacks
    def toggleSwitch(self, id):
        try:
            if (self.client.connect() is False):
                print('not connected')
                self.client.connect()
                print('trying to connecto to ' + str(self.pfcIp))

            print("toggleSwitch {}".format(id))

            id = int(id)

            if (id >= 16 and id <= 21):
                id = 32768 + (id - 16)
            elif (id == 0):
                id = 32768 + 6
            else:
                return "unknown switch"

            self.client.write_coil(id, 1)
            yield sleep(0.1)
            self.client.write_coil(id, 0)

            return "ok"
        except ConnectionException as connErr:
            return "connection error"
        except Exception as err:
            print("error: {}".format(err), file=sys.stdout)
            traceback.print_exc(file=sys.stdout)
            return "connection error"

    @inlineCallbacks
    def switchOn(self, id):
        try:
            if (self.client.connect() is False):
                print('not connected')
                self.client.connect()
                print('trying to connecto to ' + str(self.pfcIp))

            print("switchOn {}".format(id))

            id = int(id)
            state = False

            result = self.client.read_coils(560, 6)

            if (id >= 16 and id <= 21):
                id = (id - 16)
                if(result.bits[id] is False):
                    self.client.write_coil(32768 + id, 1)
                    yield sleep(0.1)
                    self.client.write_coil(32768 + id, 0)
            elif (id == 0):
                for i in range(6):
                    if(result.bits[i] is False):
                        self.client.write_coil(32768 + i, 1)
                yield sleep(0.1)
                for i in range(6):
                    if(result.bits[i] is False):
                        self.client.write_coil(32768 + i, 0)
            else:
                return "unknown switch"

            return "ok"
        except ConnectionException as connErr:
            return "connection error"
        except Exception as err:
            print("error: {}".format(err), file=sys.stdout)
            traceback.print_exc(file=sys.stdout)
            return "connection error"

    @inlineCallbacks
    def switchOff(self, id):
        try:
            if (self.client.connect() is False):
                print('not connected')
                self.client.connect()
                print('trying to connecto to ' + str(self.pfcIp))

            print("switchOff {}".format(id))

            id = int(id)
            state = True

            result = self.client.read_coils(560, 6)

            if (id >= 16 and id <= 21):
                id = (id - 16)
                if(result.bits[id] is True):
                    self.client.write_coil(32768 + id, 1)
                    yield sleep(0.1)
                    self.client.write_coil(32768 + id, 0)
            elif (id == 0):
                for i in range(6):
                    if(result.bits[i] is True):
                        self.client.write_coil(32768 + i, 1)
                yield sleep(0.1)
                for i in range(6):
                    if(result.bits[i] is True):
                        self.client.write_coil(32768 + i, 0)
            else:
                return "unknown switch"

            return "ok"
        except ConnectionException as connErr:
            return "connection error"
        except Exception as err:
            print("error: {}".format(err), file=sys.stdout)
            traceback.print_exc(file=sys.stdout)
            return "connection error"

    #==============================================================================
    #   Ensures that the requestLoop is called exactly once every second, sleeps for the rest of the time
    #==============================================================================
    def onJoin(self, details):
        ApplicationSession.onJoin(self, details)
        print("session ready")

        self.register(self.toggleSwitch, u'eshl.wago.v1.switch.toggle')
        self.register(self.switchOn, u'eshl.wago.v1.switch.on')
        self.register(self.switchOff, u'eshl.wago.v1.switch.off')

        self._loop = task.LoopingCall(self.requestLoop)
        self._requestLoopHandle = self._loop.start(1.0)

    def onLeave(self, details):
        ApplicationSession.onLeave(self, details)
        print("leaving")

        if(hasattr(self, "_loop") and self._loop):
            self._loop.stop()

if __name__ == '__main__':
    runner = ApplicationRunner(url=wampRouterAddress, realm=wampRouterRealm)
    runner.run(InModbus, auto_reconnect=True)
