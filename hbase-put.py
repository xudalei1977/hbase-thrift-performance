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
md5 = hashlib.md5()

#config
records = sys.argv[1]       #6000000 #6 million
concurrent = sys.argv[2]

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
        
        self.write_hbase()

        mylock.acquire()
        gEndTime = time.time()
        gWritenRecord += self.recordsPerThread
        print("%s done, %s seconds past, %d reocrds saved" % (self.getName(), gEndTime - gStartTime, gWritenRecord))
        mylock.release()
        self.transport.close()
             
    def write_hbase(self):
        print(self.getName(), "Start write")

        for i in range(0, self.recordsPerThread):
            #rowkey = hashlib.md5(str(random.randrange(100000)).encode('utf-8')).hexdigest()
            #print(str(self.threadId) + " : " + str(self.threadId * self.recordsPerThread + i))
            rowkey = hashlib.md5(str(self.threadId * self.recordsPerThread + i).encode('utf-8')).hexdigest()
            put_columns = [
                    TColumnValue('cf_1'.encode(), 'col_1'.encode(), 'value_1'.encode()),
                    TColumnValue('cf_1'.encode(), 'col_2'.encode(), 'value_2'.encode()),
                    TColumnValue('cf_1'.encode(), 'col_3'.encode(), 'value_3'.encode()),
                    TColumnValue('cf_1'.encode(), 'col_4'.encode(), 'value_4'.encode()),
                    TColumnValue('cf_1'.encode(), 'col_5'.encode(), 'value_5'.encode())
                    ]
            tput = TPut(rowkey.encode(), put_columns)
            self.client.put("test_ns:test_1".encode(), tput)

if __name__ == "__main__":
    recordsPerThread = int(records / concurrent)
    gStartTime = time.time()
    for threadId in range(0, concurrent):
        t = writeThread(threadId, recordsPerThread)
        t.start()
    print("%d thread created, each thread will write %d records" % (concurrent, recordsPerThread))
