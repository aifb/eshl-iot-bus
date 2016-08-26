#!/bin/env python3
"""
Created on 13.06.16
RPCs: eshl.preferences.v1.dof.set
      eshl.preferences.v1.dof.inc
      eshl.preferences.v1.dof.increase
      eshl.preferences.v1.dof.dec
      eshl.preferences.v1.dof.decrease

@author: kaibin bao
"""
from __future__ import print_function

import sys
import traceback
import asyncio
import math

from autobahn.asyncio.wamp import ApplicationSession, ApplicationRunner


class DOFService(ApplicationSession):
    """
    Provides RPCs to set, increase or decrease the degrees of freedom
    configuration
    """

    rloop = None

    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)
        self.dofs = {"-1609551312": 3600 * 4,
                     "-1609550966": 3600 * 4,
                     "-1609555623": 3600 * 4,
                     "-1609555631": 3600 * 4,
                     "-1609555510": 3600 * 4,
                     "-1609555628": 3600 * 4}

    def publishCurrentDof(self):
        try:
            if(self.is_attached()):
                self.publish(u'eshl.preferences.v1.dof', self.dofs)
        except Exception as err:
            print("error: {}".format(err), file=sys.stdout)
            traceback.print_exc(file=sys.stdout)

    def loopFunc(self):
        self.publishCurrentDof()
        loop = asyncio.get_event_loop()
        now = loop.time()
        delay = math.floor(now + 1) - now
        self.rloop = loop.call_later(delay, self.loopFunc)

    def startLoop(self):
        if(self.rloop is None):
            loop = asyncio.get_event_loop()
            self.rloop = loop.call_later(1, self.loopFunc)
        else:
            print("loop already started")

    def setDof(self, id, newDof):
        id = str(id)
        self.dofs[id] = newDof

    def increaseDof(self, id):
        id = str(id)
        try:
            oldDof = self.dofs[id]
        except KeyError:
            oldDof = 3600 * 4

        self.dofs[id] = oldDof + 1800

    def decreaseDof(self, id):
        id = str(id)
        try:
            oldDof = self.dofs[id]
        except KeyError:
            oldDof = 3600 * 4

        self.dofs[id] = oldDof - 1800

    @asyncio.coroutine
    def onJoin(self, details):
        ApplicationSession.onJoin(self, details)
        print("session ready")
        yield from self.register(self.setDof,
                                 u'eshl.preferences.v1.dof.set')
        yield from self.register(self.increaseDof,
                                 u'eshl.preferences.v1.dof.increase')
        yield from self.register(self.increaseDof,
                                 u'eshl.preferences.v1.dof.inc')
        yield from self.register(self.decreaseDof,
                                 u'eshl.preferences.v1.dof.decrease')
        yield from self.register(self.decreaseDof,
                                 u'eshl.preferences.v1.dof.dec')
        self.startLoop()
        print("registered everything")

    def onLeave(self, details):
        ApplicationSession.onLeave(self, details)
        print("session left")

if __name__ == '__main__':
    runner = ApplicationRunner(url=u"ws://wamp-router:8080/ws", realm=u"eshl")
    runner.run(DOFService)
