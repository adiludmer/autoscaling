from multiprocessing import Manager
import threading
from worker import Worker
from functools import reduce
import time

# Auto scaling settings
SCALE_UP_INTERVAL = 0.1
SCALE_DOWN_INTERVAL = 1


class WorkManager(object):

	def __init__(self):
		self._mgr = Manager()
		self._workers = []
		self._shared_q = self._mgr.Queue()
		self._invocations = 0
		self._lock = self._mgr.Lock()
		self._shutdown = False
		self._auto_scale_thread = threading.Thread(target=self._auto_scale)
		self._shared_lock = self._mgr.Lock()

	def start(self):
		self._auto_scale_thread.start()

		# waiting for 1st worker to start
		while self.num_of_all_workers == 0:
			time.sleep(0.1)

	def add_message(self, msg):
		self._shared_q.put(msg)
		self._invocations += 1

	def _wakeup_one(self):
		for worker in self._workers:
			if worker.is_waiting():
				worker.wake_up()
				break

	def _create_worker(self):
		self._lock.acquire()
		worker = Worker(q=self._shared_q, shared_lock=self._shared_lock)
		worker.start()
		self._workers.append(worker)
		self._lock.release()

	def shutdown(self):
		self._shutdown = True

		for w in self._workers:
			w.terminate()

		self._mgr.shutdown()

	def _auto_scale(self):

		while not self._shutdown:
			# !!! This condition should only fulfill when the manager starts,
			# and the first worker process craeted !!!
			if self.num_of_all_workers == 0:
				self._create_worker()

			# if there is still pending work in the queue
			# we scale up by adding more workers or waking up sleeping ones
			elif self._shared_q.qsize() > 0:
				if self.num_of_waiting_workers == 0:
					self._create_worker()
				else:
					self._wakeup_one()

				time.sleep(SCALE_UP_INTERVAL)

			# if there is no work at the moment so we start terminating workers
			else:
				# we keep at least 1  worker on standby
				if self.num_of_waiting_workers > 1:
					idx = None
					for worker in self._workers:
						if worker.is_waiting():
							worker.terminate()
							idx = self._workers.index(worker)
							break

					if idx is not None:
						self._workers.pop(idx)

				time.sleep(SCALE_DOWN_INTERVAL)

	@property
	def num_of_active_workers(self):
		n = reduce(lambda x, y: x+y, map(lambda x: 1 if x.is_active() else 0, self._workers), 0)
		return n

	@property
	def num_of_waiting_workers(self):
		return reduce(lambda x, y: x+y, map(lambda x: 1 if x.is_waiting() else 0, self._workers), 0)

	@property
	def num_of_all_workers(self):
		return len(self._workers)

	@property
	def num_of_invocations(self):
		return self._invocations

	@property
	def qsize(self):
		return 0 if self._shutdown else self._shared_q.qsize()


