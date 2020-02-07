from multiprocessing import Manager
import unittest
from worker import Worker
import time


class TestWorker(unittest.TestCase):

	def test(self):

		# init queue and worker
		self._mgr = Manager()
		lock = self._mgr.Lock()
		self.q = self._mgr.Queue()
		self.worker = Worker(q=self.q, shared_lock=lock)

		# fill queue
		self.q.put('message 1')
		self.q.put('message 2')
		self.worker.start()

		# wait for work to complete
		while self.worker.is_active():
			time.sleep(1)

		# making sure worker stay at waiting state
		self.assertTrue(self.worker.is_waiting())
		self.assertTrue(self.q.empty())

		# testing wake up
		self.q.put('message 3')
		self.worker.wake_up()

		while self.worker.is_active():
			time.sleep(1)

		# testing termination
		self.worker.terminate()
		while self.worker.is_terminating():
			time.sleep(1)

		self.assertTrue(self.worker.is_terminated())
		self.assertTrue(self.q.empty())


if __name__ == '__main__':
	unittest.main(verbosity=2)
