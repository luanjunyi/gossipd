import sys, os, time
import argparse
import logging
import eventlet

eventlet.monkey_patch()

from datetime import datetime
from gossip_client import GossipClient
from pprint import pprint



logging.basicConfig(format="%(asctime)s - %(levelname)s - %(module)s-%(funcName)s(%(lineno)d): %(message)s",
                    level = logging.DEBUG, stream = sys.stderr)
_logger = logging.getLogger('root')

g_publish_finish_time = None
g_subscribe_finish_time = None
g_subscribe_done = set()
g_active_client_num = 0

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
        self.subscribe_done = False

    def on_connect(self, mosq, obj, rc):
        if rc == 0:
            _logger.debug('worker %d connected' % self.worker_id)
        else:
            _logger.error('worker %d on_connect, rc=%d' % (self.worker_id, rc))

    def on_disconnect(self, mosq, obj, rc):
        global g_active_client_num
        if rc == 0:
            _logger.debug('worker %d disconnected normally' % self.worker_id)
        else:
            _logger.error('worker %d lost connection to server' % self.worker_id)

        if not self.subscribe_done:
            _logger.error('worker %d not done subscription and lost connection, will decrement active client number',
                          self.worker_id)
            g_active_client_num -= 1

    def on_publish(self, mosq, obj, mid):
        return
        _logger.debug('on worker %d, published mid=%d' % (self.worker_id, mid))

    def on_subscribe(self, mosq, obj, mid, qos_list):
        g_subscribe_done.add(self.worker_id)
        self.subscribe_done = True
        #_logger.debug('on worker %d, subscribed mid=%d' % (self.worker_id, mid))

    def on_unsubscribe(self, mosq, obj, mid):
        return
        _logger.debug('on woker %d, unsubscribed mid=%d' % (self.worker_id, mid))

    def on_message(self, mosq, obj, msg):
        now = time.time()
        sent = float(msg.payload)
        elapsed = now - sent
        self.status.append(elapsed)
        return
        _logger.debug('worker %d got message, topic=(%s), payload=(%s)' % 
                      (self.worker_id, msg.topic, msg.payload))

    def start_listening(self, message_num):
        self.status = self._build_status()
        self.client.connect(self.hostname, self.port, keepalive=6000)

        topic = str(self.worker_id)
        self.subscribe_done = False
        self.client.subscribe(topic, qos=1)

        count = 0
        while len(self.status) < message_num:
            count += 1
            if count % 100 == 0:
                _logger.debug("worker %d get %d messages" % (self.worker_id, len(self.status)))
            ret = self.client.loop()
            if ret != 0:
                _logger.error("worker %d loop() failed, returned %d" % (self.worker_id, ret))
                #sys.exit(0)
                break
            now = time.time()
            if g_publish_finish_time is not None:
                late = (now - g_publish_finish_time)
                if late > 10.0:
                    _logger.debug("worker %d: too long waiting for message, quit with %d received" % (self.worker_id, len(self.status)))
                    break
                else:
                    _logger.debug("worker %d, %.3f seconds late" % (self.worker_id, late))

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
    global g_publish_finish_time

    published = list()

    def _on_publish(mosq, obj, mid):
        published.append(mid)
        #_logger.debug('got publish ack %d' % mid)

    def _on_disconnect(mosq, obj, rc):
        if rc != 0:
            _logger.fatal('publisher lost connection to server')
            sys.exit(0)

    client = GossipClient(client_id="gossipd-test-publisher")
    client.on_publish = _on_publish
    client.on_disconnect = _on_disconnect
    client.connect(hostname, port)

    for i in xrange(message_num):
        for j in xrange(thread_num):
            now = time.time()
            client.publish(topic=str(j+1), payload=str(now), qos=1)
            count = i *  thread_num + j
            if count % 100 == 0:
                _logger.debug("published %d messages" % count)
            ret = client.loop(10.0) # Mosquitto won't work without this sleep
            if ret != 0:
                _logger.fatal("publish loop returned %d" % ret)
                sys.exit(0)

    while len(published) < message_num * thread_num:
        _logger.debug("published %d" % len(published))
        ret = client.loop()
        if ret != 0:
            _logger.fatal("publisher's connection is lost, rc=%d" % rc)
            sys.exit(0)

    g_publish_finish_time = time.time()
    _logger.info("all messages published")


def start_testing(hostname, port, thread_num, sleep):
    global g_active_client_num
    g_active_client_num = thread_num

    _logger.info("Testing, %s:%d, thread:%d, sleep:%d" % (hostname, port, thread_num, sleep))
    message_num = 10
    workers = list()

    for i in xrange(thread_num):
        worker = eventlet.spawn(create_subscriber, i+1, thread_num, message_num, hostname, port)
        workers.append(worker)
        time.sleep(0.01)

    while len(g_subscribe_done) < g_active_client_num:
        _logger.info("waiting %d/%d clients to finish subscribe" %
                     (g_active_client_num - len(g_subscribe_done),
                      g_active_client_num))
        time.sleep(1)

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
            cur_line = " ".join(["%.4f" % i for i in line])
            out.write(cur_line + "\n")

    _logger.info("testing finsished")

if __name__ == "__main__":
    main()
