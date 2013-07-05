import sys, os, time
import argparse
import logging
import eventlet
from datetime import datetime
from gossip_client import GossipClient
from pprint import pprint

eventlet.monkey_patch()

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(module)s(%(lineno)d): %(message)s",
                    level = logging.DEBUG, stream = sys.stderr)
_logger = logging.getLogger('root')

class TestWorker(object):
    def __init__(self, worker_id, thread_num, hostname, port):
        self.client_id = "test_client_%d" % worker_id
        self.worker_id = worker_id
        self.thread_num = thread_num
        self.hostname = hostname
        self.port = port
        self.client = GossipClient(client_id = self.client_id, clean_session = False)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        self.client.on_unsubscribe = self.on_unsubscribe

    def on_connect(self, mosq, obj, rc):
        if rc == 0:
            _logger.debug('worker %d connected' % self.worker_id)
            self.status['connected'] = 1
        else:
            _logger.error('worker %d on_connect, rc=%d' % (self.worker_id, rc))

    def on_disconnect(self, mosq, obj, rc):
        if rc == 0:
            _logger.debug('worker %d disconnected normally' % self.worker_id)
            self.status['disconnected'] = 1
        else:
            _logger.error('worker %d lost connection to server' % self.worker_id)

    def on_publish(self, mosq, obj, mid):
        _logger.debug('on worker %d, published mid=%d' % (self.worker_id, mid))
        self.status['published'].add(mid)

    def on_subscribe(self, mosq, obj, mid, qos_list):
        _logger.debug('on worker %d, subscribed mid=%d' % (self.worker_id, mid))
        self.status['subscribed'].add(mid)

    def on_unsubscribe(self, mosq, obj, mid):
        _logger.debug('on woker %d, unsubscribed mid=%d' % (self.worker_id, mid))
        self.status['unsubscribed'].add(mid)

    def on_message(self, mosq, obj, msg):
        self.status['message'][int(msg.topic)].add(int(msg.payload))
        _logger.debug('worker %d got message, topic=(%s), payload=(%s)' % 
                      (self.worker_id, msg.topic, msg.payload))
        

    def start(self):
        self.status = self._build_status()
        self.client.connect(self.hostname, self.port)

        for i in xrange(1, self.thread_num+1):
            topic = str(i)
            self.client.subscribe(topic)

        _logger.debug("worker %d waiting for subscribe done" % self.worker_id)
        time.sleep(5)

        for i in xrange(1, self.thread_num+1):
            topic = str(i)
            payload = str(self.worker_id)
            self.client.publish(topic, payload)

        _logger.debug("worker %d waiting for publish done" % self.worker_id)
        time.sleep(5)


        for i in xrange(1, self.thread_num+1):
            topic = str(i)
            self.client.unsubscribe(topic)


        start_time = datetime.now()
        while True:
            ret = self.client.loop()
            now = datetime.now()
            if (now - start_time).seconds > 10:
                self.client.disconnect()
                break


    def _build_status(self):
        return {'connected': 0,
                'disconnected': 0,
                'published': set(),
                'subscribed': set(),
                'message': dict(zip(range(1, self.thread_num + 1), [set()] * self.thread_num)),
                'unsubscribed': set(),
                }

    def collect(self):
        return self.status


def test(worker_id, thread_num, hostname, port):
    worker = TestWorker(worker_id, thread_num, hostname, port)
    _logger.debug("worker(%d) is started" % worker_id)
    worker.start()
    return worker.collect()
    _logger.debug("worker(%d) is done" % worker_id)

    

def start_testing(hostname, port, thread_num, sleep):
    _logger.info("Testing, %s:%d, thread:%d, sleep:%d" % (hostname, port, thread_num, sleep))
    workers = list()
    for i in xrange(thread_num):
        worker = eventlet.spawn(test, i+1, thread_num, hostname, port)
        workers.append(worker)

    statuses = list()
    for worker in workers:
        statuses.append(worker.wait())

    for status in statuses:
        pprint(status)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--thread", dest="thread_num",
                        default=2, help="Number of threads", type=int)
    parser.add_argument("-p", "--port", dest="port",
                        default=1883, help="Port of the broker", type=int)
    parser.add_argument("--host", dest="host",
                        default="localhost", help="Hostname of the broker")
    parser.add_argument("-s", "--sleep", dest="sleep", type=int,
                        default=0, help="Seconds the clients will sleep between requests, default to 0(no sleep)")

    args = parser.parse_args()

    thread_num = args.thread_num
    sleep = args.sleep
    port = args.port
    hostname = args.host

    start_testing(hostname, port, thread_num, sleep)
    

if __name__ == "__main__":
    main()
