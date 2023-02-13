# -*- coding: utf-8 -*-

import json, traceback, sys, datetime, time, logging, threading, random, hashlib
import logging.handlers
import phoenixdb
import phoenixdb.cursor

round = int(sys.argv[1])
concurrent = int(sys.argv[2])

database_url = 'http://localhost:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)


class readThread(threading.Thread):
    def __init__(self, threadId, recordsPerThread):
        threading.Thread.__init__(self, name = "Thread_%s" % threadId)
        self.recordsPerThread = recordsPerThread
        self.threadId = threadId

    def run(self):
        self.read_phoenix()

    def read_phoenix(self):
        for i in range(0, self.recordsPerThread):
            startTime = time.time()
            cursor = conn.cursor(cursor_factory = phoenixdb.cursor.DictCursor)
            cursor.execute("SELECT PK FROM \"customer\" where PK = ? limit ?", ('abbott_aaliyah_5802042731', 1))
            print("%s done, %s seconds past, result is := %s" % (self.getName(), time.time() - startTime, cursor.fetchone()['PK']))

if __name__ == "__main__":
    for i in range(0, round):
        for threadId in range(0, concurrent):
            t = readThread(threadId, 1)
            t.start()

    time.sleep(5)
    conn.close()



