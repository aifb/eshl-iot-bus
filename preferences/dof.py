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
import math
import time
import pickle

from twisted.internet import task
from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner


class DOFService(ApplicationSession):
    """
    Provides RPCs to set, increase or decrease the degrees of freedom
    configuration
    """

    def __init__(self, config=None):
        ApplicationSession.__init__(self, config)
        self.dofs = {"-1609551312": 3600 * 4,
                     "-1609550966": 3600 * 4,
                     "-1609555623": 3600 * 4,
                     "-1609555631": 3600 * 4,
                     "-1609555510": 3600 * 4,
                     "-1609555628": 3600 * 4}
        try:
            with open('dofs.pickle', 'rb') as f:
                self.dofs = pickle.load(f)
        except IOError:
            pass

    def pickleState(self):
        try:
            with open('dofs.pickle', 'wb') as f:
                pickle.dump(self.dofs, f)
        except IOError:
            pass

    def publishCurrentDof(self):
        try:
            if(self.is_attached()):
                self.publish(u'eshl.preferences.v1.dof', self.dofs)
        except Exception as err:
            print("error: {}".format(err), file=sys.stdout)
            traceback.print_exc(file=sys.stdout)

    def loopFunc(self):
        self.publishCurrentDof()

    def setDof(self, id, newDof):
        id = str(id)
        self.dofs[id] = newDof
        try:
            if(self.is_attached()):
                self.publish(u'eshl.preferences.v1.dof.changed',
                             {"timestamp": int(time.time()),
                              "id": id,
                              "dof": newDof})
        except Exception:
            pass
        self.publishCurrentDof()
        self.pickleState()

    def increaseDof(self, id):
        id = str(id)
        try:
            oldDof = self.dofs[id]
        except KeyError:
            oldDof = 3600 * 4

        self.dofs[id] = oldDof + 1800
        self.publishCurrentDof()
        self.pickleState()

    def decreaseDof(self, id):
        id = str(id)
        try:
            oldDof = self.dofs[id]
        except KeyError:
            oldDof = 3600 * 4

        self.dofs[id] = oldDof - 1800
        self.publishCurrentDof()
        self.pickleState()

    @inlineCallbacks
    def onJoin(self, details):
        ApplicationSession.onJoin(self, details)
        print("session ready")
        yield self.register(self.setDof,
                            u'eshl.preferences.v1.dof.set')
        yield self.register(self.increaseDof,
                            u'eshl.preferences.v1.dof.increase')
        yield self.register(self.increaseDof,
                            u'eshl.preferences.v1.dof.inc')
        yield self.register(self.decreaseDof,
                            u'eshl.preferences.v1.dof.decrease')
        yield self.register(self.decreaseDof,
                            u'eshl.preferences.v1.dof.dec')
        self._loop = task.LoopingCall(self.loopFunc)
        self._loop.start(1.0)
        print("registered everything")

    def onLeave(self, details):
        ApplicationSession.onLeave(self, details)
        if(hasattr(self, "_loop") and self._loop):
            self._loop.stop()
        print("session left")

if __name__ == '__main__':
    runner = ApplicationRunner(url=u"ws://wamp-router:8080/ws", realm=u"eshl")
    runner.run(DOFService, auto_reconnect=True)
