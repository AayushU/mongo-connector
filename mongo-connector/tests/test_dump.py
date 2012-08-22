
# Copyright 2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file will be used with PyPi in order to package and distribute the final
# product.

"""Test initial dump using DocManagerSimulator
    """
import os
import sys
import inspect

file = inspect.getfile(inspect.currentframe())
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(file)[0]))

doc_folder = cmd_folder.rsplit("/", 1)[0]
doc_folder += '/doc_managers'
if doc_folder not in sys.path:
    sys.path.insert(0, doc_folder)

cmd_folder = cmd_folder.rsplit("/", 1)[0]
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)


import time
import unittest
from setup_cluster import killMongoProc, startMongoProc, start_cluster
from pymongo import Connection
from os import path
from threading import Timer
from doc_manager_simulator import DocManager
from pysolr import Solr
from mongo_connector import Connector
from optparse import OptionParser
from util import retry_until_ok
import pymongo

""" Global path variables
    """
PORTS_ONE = {"PRIMARY": "27117", "SECONDARY": "27118", "ARBITER": "27119",
             "CONFIG": "27220", "MONGOS": "27117"}
conn = None
NUMBER_OF_DOCS = 10000
connector = None
doc_manager = None


class TestDump(unittest.TestCase):

    def runTest(self):
        unittest.TestCase.__init__(self)

    def setUp(self):
        conn['test']['test'].remove(safe=True)
        while len(doc_manager._search()) != 0:
            time.sleep(1)

    def test_initial_dump(self):
        """Test initial dump while new documents are being inserted.
        All documents should show be in database.
        """
        long_string = 'long string '
        long_string *= 42
        for i in range(0, NUMBER_OF_DOCS):
            conn['test']['test'].insert({'updated': False, 'number': i})
        time.sleep(5)
        connector.start()
        for i in range(NUMBER_OF_DOCS - 1, -1, -1):
            if i % 2 == 1:
                conn['test']['test'].update({'number': i},
                                            {'updated': True, 'number': i})
            else:
                conn['test']['test'].update({'number': i},
                                            {'updated': True, 'number': i,
                                             'info': long_string})
        count = 0
        while True:
            error = False
            for doc in doc_manager._search():
                if count > 300:
                    string = 'Docs took too long to update in test dump'
                    logging.error(string)
                    self.assertTrue(False)
                
                if(doc['updated'] is False):
                    error = True
                    count += 1
                    time.sleep(1)
                    break
                if (doc['number'] % 2 == 0):
                    if(doc['info'] != long_string):
                        error = True
                        count += 1
                        time.sleep(1)
                        break
            if error is False:
                break

        print ("PASSED INITIAL DUMP TEST")


def abort_test(self):
    print("TEST FAILED")
    sys.exit(1)

if __name__ == '__main__':
    os.system('rm config.txt; touch config.txt')
    parser = OptionParser()

    #-m is for the main address, which is a host:port pair, ideally of the
    #mongos. For non sharded clusters, it can be the primary.
    parser.add_option("-m", "--main", action="store", type="string",
                      dest="main_addr", default="27217")

    (options, args) = parser.parse_args()
    PORTS_ONE['MONGOS'] = options.main_addr
    connector = Connector('localhost:' + PORTS_ONE["MONGOS"], 'config.txt', None,
                  ['test.test'], '_id', None, None)
    doc_manager = connector.doc_manager
    if options.main_addr != "27217":
        start_cluster(use_mongos=False)
    else:
        start_cluster()
    conn = Connection('localhost:' + PORTS_ONE['MONGOS'],
                      replicaSet="demo-repl")
    unittest.main(argv=[sys.argv[0]])
    connectorjoin()
