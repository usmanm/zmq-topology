import argparse
from proc import ConsumerProcess, ProducerProcess, PushStrategy
import random
import time


class ViewTopology(object):
  def __init__(self, num_hops, size, ninsert, nworkers, ncombiners, num_messages=10, procfn=None,
               sleep_duration=0.1):
    self.combiners = [ConsumerProcess('combiner-%d' % i, 'ipc:///tmp/combiner%d' % i,
                                      ['ipc:///tmp/worker%d' % i for i in range(nworkers)],
                                      procfn=procfn)
                      for i in range(ncombiners)]
    self.workers = [ConsumerProcess('worker-%d' % i, 'ipc:///tmp/worker%d' % i,
                                    ['ipc:///tmp/combiner%d' % i for i in range(ncombiners)],
                                    procfn=procfn, strategy=PushStrategy.HASHING)
                    for i in range(nworkers)]
    self.inserters = [ProducerProcess('insert-%d' % i,
                                      ['ipc:///tmp/worker%d' % i for i in range(nworkers)],
                                      num_hops, size, num_messages=num_messages,
                                      sleep_duration=sleep_duration)
                      for i in range(ninsert)]

  def run(self, sleep=0):
    for c in self.combiners:
      c.start()

    for w in self.workers:
      w.start()

    for i in self.inserters:
      i.start()

    iresults = []
    for i in self.inserters:
      i.kill()
      iresults.append(i.get_result())

    time.sleep(sleep)

    wresults = []
    for w in self.workers:
      w.kill()
      wresults.append(w.get_result())

    cresults = []
    for c in self.combiners:
      c.kill()
      cresults.append(c.get_result())

    return iresults, wresults, cresults


def double_size(msg):
  return msg.num_hops - 1, msg.size * 2


def half_size(msg):
  return msg.num_hops - 1, int(msg.size / 2)


def random_sleep(msg):
  time.sleep(random.random() / 100)
  return msg.num_hops - 1, msg.size

def random_hops(msg):
  return random.randint(0, msg.num_hops - 1), msg.size


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--num-hops', help='Number of hops per message', type=int, default=10)
  parser.add_argument('--num-insert', help='Number of insert processes', type=int, default=10)
  parser.add_argument('--num-workers', help='Number of worker processes', type=int, default=10)
  parser.add_argument('--num-combiners', help='Number of combiner processes', type=int, default=10)
  parser.add_argument('--num-messages', help='Number of messages to insert per process', type=int,
                      default=100)
  parser.add_argument('--msg-size', help='Size of source message', type=int, default=10240)
  parser.add_argument('--wait-time', help='Wait time before sending kill to workers and combiners',
                      type=int, default=2)
  args = parser.parse_args()

  for procfn in [double_size, half_size, random_sleep, random_hops]:
    v = ViewTopology(args.num_hops, args.msg_size, args.num_insert, args.num_workers,
                     args.num_combiners, args.num_messages, procfn=procfn, sleep_duration=0)
    start = time.time()
    results = v.run(args.wait_time)
    duration = time.time() - start

    print '========'
    print procfn.__name__
    print '========'
    print 'Time taken:', duration
    print 'Messages produced:', sum(results[0])
    print 'Messages consumed by workers:', sum(map(lambda t: t[0], results[1]))
    print 'Messages produced by workers:', sum(map(lambda t: t[1], results[1]))
    print 'Messages consumed by combiners:', sum(map(lambda t: t[0], results[2]))
    print 'Messages produced by combiners:', sum(map(lambda t: t[1], results[2]))
    print
