from multiprocessing import Process, Event, Value
from queue import Empty
import time

# Worker lifecycle states
STATE_ACTIVE = 1
STATE_WAITING = 2
STATE_TERMINATING = 3
STATE_TERMINATED = 4

SHARED_FILE = "/tmp/shared_file.txt"
WORK_TIME = 5
WAIT_FOR_MESSAGE_TIME = 0.1


class Worker(object):

	def __init__(self, q, shared_lock):
		self._q = q
		self._process = Process(target=self._work_func)
		self._state = Value('i', STATE_ACTIVE)
		self._signal = Event()
		self._shared_lock = shared_lock

	def start(self):
		self._process.start()
		print("pid %s started" % self._process.pid)

	def wake_up(self):
		print("pid %s woke up" % self._process.pid)
		self._change_state(STATE_ACTIVE)
		self._signal.set()

	def terminate(self):
		self._change_state(STATE_TERMINATING)
		self._signal.set()

	def is_active(self):
		return self.state == STATE_ACTIVE

	def is_waiting(self):
		return self.state == STATE_WAITING

	def is_terminating(self):
		return self.state == STATE_TERMINATING

	def is_terminated(self):
		return self.state == STATE_TERMINATED

	def _change_state(self, new_state):
		self._state.value = new_state

	@property
	def state(self):
		return self._state.value

	def _keep_working(self):
		return self.state != STATE_TERMINATING

	def _work_func(self):
		while self._keep_working():
			try:
				message = self._q.get(timeout=WAIT_FOR_MESSAGE_TIME)

				# writing received message to a file using a shared lock
				# to sync writes between different processes
				with self._shared_lock:
					print('pid %s - %s' % (self._process.pid, message))
					with open(SHARED_FILE, "a") as f:
						f.write(message+"\n")

				time.sleep(WORK_TIME)

			# we got timeout because queue is empty so we wait for
			# wakeup/terminate signal
			except Empty:
				self._wait()

		self._change_state(STATE_TERMINATED)
		print("pid %s terminated" % self._process.pid)

	def _wait(self):
		self._change_state(STATE_WAITING)
		self._signal.clear()
		print("pid %s waiting" % self._process.pid)
		self._signal.wait()
