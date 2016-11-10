# -*- coding: utf-8 -*-
"""
Created on 24.06.2016

RPC : eshl.wago.v1.misc.shuttercontrol

@author: florian fiebig
@author: kaibin bao
"""

import sys
import traceback

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.util import sleep
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner

from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pymodbus.exceptions import ConnectionException

from config import *


class InModbus(ApplicationSession):
    """
    Provides an RPC to control the shutters
    """
    def __init__(self, config=None):
            ApplicationSession.__init__(self, config)
            self.pfcIp = '192.168.1.53'
            self.modbusTcpPort = 502
            self.client = ModbusClient(self.pfcIp, self.modbusTcpPort)

    def shutterMapping(self, name):
        if name == "FlurLinksUnten":
            return 32768
        elif name == "FlurLinksOben":
            return 32769
        elif name == "FlurRechtsUnten":
            return 32770
        elif name == "FlurRechtsOben":
            return 32771
        # elif name == "ZimmerLinksUnten":
            # return 32772
        # elif name == "ZimmerLinksOben":
            # return 32773
        elif name == "ZimmerRechtsUnten":
            return 32774
        elif name == "ZimmerRechtsOben":
            return 32775
        elif name == "KuecheUnten":
            return 32776
        elif name == "KuecheOben":
            return 32777
        else:
            return "unknown"

    @inlineCallbacks
    def shutterControl(self, name):
        name = str(name)
        print(name)
        try:
            if (self.client.connect() is False):
                print('not connected')
                self.client = self.client.connect()
                print('trying to connecto to ' + str(self.pfcIp))

            modbusCoil = self.shutterMapping(name)
            if (modbusCoil != "unknown"):
                    self.client.write_coil(modbusCoil, 1)
                    yield sleep(0.1)
                    self.client.write_coil(modbusCoil, 0)
            else:
                return "unknown shutter"

        except ConnectionException as connErr:
            return "connection error"
        except Exception as err:
            print("error: {}".format(err), file=sys.stdout)
            traceback.print_exc(file=sys.stdout)
            return "connection error"

    def onJoin(self, details):
        print("session ready")

        self.register(self.shutterControl, u'eshl.wago.v1.misc.shuttercontrol')


if __name__ == '__main__':
    runner = ApplicationRunner(url=wampRouterAddress, realm=wampRouterRealm)
    runner.run(InModbus, auto_reconnect=True)
