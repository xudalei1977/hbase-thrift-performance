# -*- coding: utf-8 -*-

import json, traceback, sys, datetime, time, logging, threading, random, hashlib
import logging.handlers

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase.ttypes import *
from hbase import THBaseService

gWritenRecord = 0
gStartTime = 0
gEndTime = 0

#config
records = int(sys.argv[1])       #6000000 #6 million
concurrent = int(sys.argv[2])

mylock = threading.RLock()

class writeThread(threading.Thread):
    def __init__(self, threadId, recordsPerThread):
        threading.Thread.__init__(self, name = "Thread_%s" % threadId)
        self.recordsPerThread = recordsPerThread
        self.threadId = threadId

        self.transport = TTransport.TBufferedTransport(TSocket.TSocket('127.0.0.1', 9090))
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        self.client = THBaseService.Client(protocol)
        self.transport.open()

    def run(self):
        print("********** %s start at := " % (self.getName()))
        global gEndTime
        global gWritenRecord
        
        self.read_hbase()

        mylock.acquire()
        gEndTime = time.time()
        gWritenRecord += 1
        print("%s done, %s seconds past, %d reocrds got" % (self.getName(), gEndTime - gStartTime, gWritenRecord))
        mylock.release()
        self.transport.close()
             
    def read_hbase(self):
        print(self.getName(), "Start write")

        for i in range(0, self.recordsPerThread):
            get_columns = [
                    TColumn('cf_1'.encode(), 'col_1'.encode()),
                    TColumn('cf_1'.encode(), 'col_2'.encode())
                ]

            rowkey = hashlib.md5(str(random.randrange(100000000)).encode('utf-8')).hexdigest()
            #rowkey = hashlib.md5(str(self.threadId * self.recordsPerThread + i).encode('utf-8')).hexdigest()
            tget = TGet(rowkey.encode(), get_columns)
            tresult = self.client.get('test_ns:test_1'.encode(), tget)
            #print(tresult)

if __name__ == "__main__":
    recordsPerThread = int(records / concurrent)
    gStartTime = time.time()
    for threadId in range(0, concurrent):
        t = writeThread(threadId, recordsPerThread)
        t.start()
    print("%d thread created, each thread will write %d records" % (concurrent, recordsPerThread))



