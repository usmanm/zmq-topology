import multiprocessing
import random
import time
import zmq


def log(msg):
  if DEBUG:
    print '%s: %s' % (multiprocessing.current_process().name, msg)


DEBUG = False
MICROBATCH_SIZE = 2 * 1024 * 1024  # 2mb


class Message(object):
  class Type:
    KILL = 'kill'
    DATA = 'data'


class KillMessage(Message):
  def __init__(self):
    self.type = Message.Type.KILL

  def __repr__(self):
    return 'KillMessage'


class DataMessage(Message):
  def __init__(self, num_hops, size):
    self.type = Message.Type.DATA

    self.num_hops = num_hops
    self.size = size
    self._data = '0' * size
    self._hash = random.randint(0, 4294967296)

  def get_hash(self):
    return self._hash

  def __repr__(self):
    return 'DataMessage(%d)' % self.size


class ZMQSocket(object):
  def __init__(self, _type, addr):
    self._type = _type
    self.addr = addr

  def bind(self, cxt):
    self.sock = cxt.socket(self._type)
    self.sock.bind(self.addr)

  def connect(self, cxt):
    self.sock = cxt.socket(self._type)
    self.sock.connect(self.addr)

  def destroy(self):
    self.sock.close()

  def send_msg(self, msg):
    self.sock.send_pyobj(msg)

  def recv_msg(self, msg):
    return self.sock.recv_pyobj()

  def __repr__(self):
    return 'ZMQSocket(%s)' % self.addr


class PushStrategy:
  ROUND_ROBIN = 'rr'
  HASHING = 'hash'
  RANDOM = 'random'


class ProducerProcess(multiprocessing.Process):
  def __init__(self, name, dests, num_hops, size, num_messages=10, sleep_duration=0.1,
               strategy=PushStrategy.ROUND_ROBIN, sync=False):
    if not hasattr(self, '_res'):
      self._res = multiprocessing.Value('i', 0)
    super(ProducerProcess, self).__init__(name=name, target=self._run, args=(self._res, ))

    self.sleep_duration = sleep_duration or 0.0001
    self.sync = sync
    self.strategy = strategy
    self.num_hops = num_hops
    self.size = size
    self._num_sent = 0
    self.num_messages = num_messages

    self.dests = []
    for addr in (dests or []):
      zs = ZMQSocket(zmq.PUSH, addr)
      self.dests.append(zs)

  def setup(self, cxt):
    for d in self.dests:
      d.connect(cxt)

  def _push_message(self, msg):
    dests = self.dests

    if self.strategy == PushStrategy.ROUND_ROBIN:
      dest = dests[self._num_sent % len(dests)]
    elif self.strategy == PushStrategy.HASHING:
      dest = dests[msg.get_hash() % len(dests)]
    else:
      dest = random.choice(dests)

    dest.send_msg(msg)
    self._num_sent += 1

  def kill(self):
    self.join()

  def get_result(self):
    assert not self.is_alive()
    return self._res.value

  def teardown(self):
    for d in self.dests:
      d.destroy()

  def produce_message(self):
    self._push_message(DataMessage(self.num_hops, self.size))

  def _run(self, res):
    log('run: starting up... will produce %d messages' % self.num_messages)
    cxt = zmq.Context.instance()
    self.setup(cxt)

    while self.num_messages:
      try:
        self.produce_message()
        time.sleep(self.sleep_duration)
        self.num_messages -= 1
      except (KeyboardInterrupt, SystemExit):
        break

    self.teardown()
    cxt.destroy()

    log('run: terminating after %d messages produced!' % self._num_sent)
    res.value = self._num_sent


class ConsumerProcess(ProducerProcess):
  def __init__(self, name, addr, dests=None, procfn=None,
               strategy=PushStrategy.ROUND_ROBIN, sync=False):
    self._res = multiprocessing.Array('i', [0, 0])
    super(ConsumerProcess, self).__init__(name, dests, None, None, strategy=strategy,
                                          sync=sync)
    self.addr = addr
    self.procfn = procfn
    self.zs = ZMQSocket(zmq.PULL, addr)
    self._num_processed = 0

  def setup(self, cxt):
    super(ConsumerProcess, self).setup(cxt)
    self.zs.bind(cxt)

  def produce_message(self):
    raise NotImplementedError

  def teardown(self):
    super(ConsumerProcess, self).teardown()
    self.zs.destroy()

  def process_message(self):
    msg = self.zs.sock.recv_pyobj()

    if msg.type == Message.Type.KILL:
      log('process_message: got kill message')
      return False

    assert msg.num_hops > 0
    self._num_processed += 1

    if self.procfn:
      num_hops, size = self.procfn(msg)
      assert num_hops < msg.num_hops
    else:
      size = msg.size
      num_hops = msg.num_hops - 1

    if num_hops and self.dests:
      self._push_message(DataMessage(num_hops, size))

    return True

  def kill(self):
    assert self.is_alive()
    cxt = zmq.Context.instance()
    zs = ZMQSocket(zmq.PUSH, self.addr)
    zs.connect(cxt)
    zs.send_msg(KillMessage())
    zs.destroy()
    cxt.destroy()
    super(ConsumerProcess, self).kill()

  def get_result(self):
    assert not self.is_alive()
    return tuple(self._res[:])

  def _run(self, res):
    log('run: starting up...')

    cxt = zmq.Context.instance()
    self.setup(cxt)

    while True:
      try:
        if not self.process_message():
          break
      except (KeyboardInterrupt, SystemExit):
        break

    self.teardown()

    log('run: terminating after %d messages processed!' % self._num_processed)
    res[0] = self._num_processed
    res[1] = self._num_sent
