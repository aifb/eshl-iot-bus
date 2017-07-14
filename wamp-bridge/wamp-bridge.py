# -*- coding: utf-8 -*-
"""
Created on 14.07.2017
Last change on 14.07.2017

subscribed to: see config

@author: kaibin bao
"""

from __future__ import print_function

import sys, os, traceback

from twisted.internet import task
from twisted.internet.defer import inlineCallbacks
from autobahn.wamp.types import SubscribeOptions
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner
from autobahn.twisted.util import sleep

import time, json

from config import *

sinkSession = None

class WampBridgeSource(ApplicationSession):
    """Subscribes to multiple topics and pushes it to another WAMP router"""
    def __init__(self, config=None):
        super().__init__(config)
        self.periodicalStatusLoop = task.LoopingCall(self.publishStatus)

    @inlineCallbacks
    def publishStatus(self):
        timestampNow = round(time.time())
        yield self.publish(u'wampbridge.eshl-iip', int(timestampNow))

    @inlineCallbacks
    def onJoin(self, details):
        super().onJoin(details)
        print("source session ready")

        self.periodicalStatusLoop.start(1.0)

        @inlineCallbacks
        def onReadoutPattern(msg, details):
            global sinkSession
            if sinkSession is not None:
                yield sinkSession.publish(details.topic, msg)

        @inlineCallbacks
        def onReadoutSingle(topic, readout):
            global sinkSession
            if sinkSession is not None:
                yield sinkSession.publish(topic, readout)

        if wampTopics is not None:
            for topic in wampTopics:
                print("subscribe to {}".format(topic))
                onReadoutTopic = lambda msg, topic=topic: onReadout(topic, msg)
                yield self.subscribe(onReadoutTopic, topic)
        else:
            yield self.subscribe(onReadoutPattern, wampTopicPrefix, SubscribeOptions(match='prefix', details=True))

    def onLeave(self, details):
        super().onLeave(details)
        print("source session left")
        self.periodicalStatusLoop.stop()

class WampBridgeSink(ApplicationSession):
    """Publishes to another WAMP router"""
    def __init__(self, config=None):
        super().__init__(config)
        self.periodicalStatusLoop = task.LoopingCall(self.publishStatus)

    @inlineCallbacks
    def publishStatus(self):
        timestampNow = round(time.time())
        yield self.publish(u'wampbridge.eshl-iip', int(timestampNow))

    def onJoin(self, details):
        global sinkSession
        super().onJoin(details)
        print("sink session ready")
        sinkSession = self


    def onLeave(self, details):
        global sinkSession
        super().onLeave(details)
        print("sink session left")
        sinkSession = None
        self.periodicalStatusLoop.stop()

if __name__ == '__main__':
    sourceRunner = ApplicationRunner(url=wampRouterSource['address'], realm=wampRouterSource['realm'])
    sourceRunner.run(WampBridgeSource, auto_reconnect=True, start_reactor=False)
    sinkRunner = ApplicationRunner(url=wampRouterSink['address'], realm=wampRouterSink['realm'])
    sinkRunner.run(WampBridgeSink, auto_reconnect=True)
