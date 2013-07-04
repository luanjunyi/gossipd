import sys, os
import argparse
import logging
import eventlet

eventlet.monkey_patch()

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(module)s(%(lineno)d): %(message)s",
                    level = logging.DEBUG, stream = sys.stderr)
_logger = logging.getLogger('root')

def test(worker_id, hostname, port):
    _logger.debug("worker(%d) is created" % worker_id)

def start_testing(hostname, port, thread_num, sleep):
    _logger.info("Testing, %s:%d, thread:%d, sleep:%d" % (hostname, port, thread_num, sleep))
    workers = list()
    for i in xrange(thread_num):
        worker = eventlet.spawn(test, i, hostname, port)
        workers.append(worker)

    for worker in workers:
        worker.wait()


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
