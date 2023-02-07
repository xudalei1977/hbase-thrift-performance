# -*- coding: utf-8 -*-

import json, traceback, sys, datetime, time, logging, threading, random, hashlib
import logging.handlers

import phoenixdb
import phoenixdb.cursor


gReadRecord = 0
gStartTime = 0
gEndTime = 0

#config
recordsPerThread = int(sys.argv[1])       #6000000 #6 million
concurrent = int(sys.argv[2])

mylock = threading.RLock()

class readThread(threading.Thread):
    def __init__(self, threadId, recordsPerThread):
        threading.Thread.__init__(self, name = "Thread_%s" % threadId)
        self.recordsPerThread = recordsPerThread
        self.threadId = threadId

        database_url = 'http://localhost:8765/'
        self.conn = phoenixdb.connect(database_url, autocommit=True)

    def run(self):
        print("********** %s start at := " % (self.getName()))
        global gEndTime
        global gReadRecord
        
        self.read_phoenix()

        mylock.acquire()
        gEndTime = time.time()
        gReadRecord += 1
        print("%s done, %s seconds past, %d reocrds got" % (self.getName(), gEndTime - gStartTime, gReadRecord))
        mylock.release()
        self.conn.close()
             
    def read_phoenix(self):
        print(self.getName(), "Start read")
        for i in range(0, self.recordsPerThread):
            cursor = self.conn.cursor(cursor_factory = phoenixdb.cursor.DictCursor)
            cursor.execute("SELECT PK FROM \"customer\" where PK = ? limit ?", ('abbott_aaliyah_5802042731', 1))
            print(cursor.fetchone()['PK'])
            #print(tresult)

if __name__ == "__main__":
    gStartTime = time.time()
    for threadId in range(0, concurrent):
        t = readThread(threadId, recordsPerThread)
        t.start()
    print("%d thread created, each thread will read %d records" % (concurrent, recordsPerThread))



