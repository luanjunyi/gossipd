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
        self.client = GossipClient(client_id = self.client_id, clean_session = True)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        self.client.on_unsubscribe = self.on_unsubscribe

    def on_connect(self, mosq, obj, rc):
        if rc == 0:
            _logger.debug('worker %d connected' % self.worker_id)
        else:
            _logger.error('worker %d on_connect, rc=%d' % (self.worker_id, rc))

    def on_disconnect(self, mosq, obj, rc):
        if rc == 0:
            _logger.debug('worker %d disconnected normally' % self.worker_id)
        else:
            _logger.error('worker %d lost connection to server' % self.worker_id)

    def on_publish(self, mosq, obj, mid):
        _logger.debug('on worker %d, published mid=%d' % (self.worker_id, mid))

    def on_subscribe(self, mosq, obj, mid, qos_list):
        _logger.debug('on worker %d, subscribed mid=%d' % (self.worker_id, mid))

    def on_unsubscribe(self, mosq, obj, mid):
        _logger.debug('on woker %d, unsubscribed mid=%d' % (self.worker_id, mid))

    def on_message(self, mosq, obj, msg):
        now = int(time.time())
        sent = int(msg.payload)
        elapsed = now - sent
        self.status.append(elapsed)
        _logger.debug('worker %d got message, topic=(%s), payload=(%s)' % 
                      (self.worker_id, msg.topic, msg.payload))

    def start_listening(self, message_num):
        self.status = self._build_status()
        self.client.connect(self.hostname, self.port)

        topic = str(self.worker_id)
        self.client.subscribe(topic)

        while len(self.status) < message_num:
            _logger.debug("worker %d get %d messages" % (self.worker_id, len(self.status)))
            ret = self.client.loop()
            if ret != 0:
                _logger.error("worker %d loop() failed, returned %d" % (self.worker_id, ret))
                break

        self.client.disconnect()
        
    def _build_status(self):
        return list()

    def collect(self):
        return self.status

def create_subscriber(worker_id, thread_num, message_num, hostname, port):
    worker = TestWorker(worker_id, thread_num, hostname, port)
    _logger.debug("worker(%d) is started" % worker_id)
    worker.start_listening(message_num)
    _logger.debug("worker(%d) is done" % worker_id)
    return worker.collect()

def create_publisher(message_num, thread_num, hostname, port):
    published = list()

    def _on_publish(mosq, obj, mid):
        published.append(mid)
        _logger.debug('published %d' % mid)

    client = GossipClient()
    client.on_publish = _on_publish
    client.connect(hostname, port)

    for i in xrange(message_num):
        for j in xrange(thread_num):
            now = int(time.time())
            client.publish(topic=str(j+1), payload=str(now))
            _logger.debug("published topic(%s) %d" % (str(j+1), now))
            time.sleep(0.01) # Mosquitto won't work without this sleep

    while len(published) < message_num * thread_num:
        _logger.debug("published %d" % len(published))
        client.loop()

    _logger.info("all messages published")



def start_testing(hostname, port, thread_num, sleep):
    _logger.info("Testing, %s:%d, thread:%d, sleep:%d" % (hostname, port, thread_num, sleep))
    message_num = 10
    workers = list()
    for i in xrange(thread_num):
        worker = eventlet.spawn(create_subscriber, i+1, thread_num, message_num, hostname, port)
        workers.append(worker)

    _logger.info("waiting 10 seconds for all to get subscribtion to finish")
    time.sleep(4)

    create_publisher(message_num, thread_num, hostname, port)
    
    statuses = list()
    for worker in workers:
        statuses.append(worker.wait())

    return statuses

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
    parser.add_argument("-o", "--output", dest="outfile", 
                        default="test.out", help="Output file default to test.out")


    args = parser.parse_args()

    thread_num = args.thread_num
    sleep = args.sleep
    port = args.port
    hostname = args.host
    outfile = args.outfile

    data = start_testing(hostname, port, thread_num, sleep)

    with open(outfile, "w") as out:
        for line in data:
            cur_line = " ".join([str(i) for i in line])
            out.write(cur_line + "\n")
    

if __name__ == "__main__":
    main()
