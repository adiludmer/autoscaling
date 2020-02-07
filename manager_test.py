import unittest
import time
from manager import WorkManager


class TestManager(unittest.TestCase):

	def test_workers_reuse(self):
		manager = WorkManager()
		manager.start()

		manager.add_message('hello 1')

		# keeping pid of 1st worker for later comparison
		pid_1 = manager._workers[0]._process.pid

		# letting the worker enough rest so it can accept the
		# 2nd message
		time.sleep(2)

		manager.add_message('hello 2')

		# # of workers should be 1 - no scale up
		self.assertEqual(manager.num_of_all_workers, 1)

		# worker pid should be the same
		pid_2 = manager._workers[0]._process.pid
		self.assertEqual(pid_1, pid_2)

		manager.shutdown()
		time.sleep(3)

	def test_scale_up_and_down(self):
		manager = WorkManager()
		manager.start()

		# fill queue
		for i in range(1, 11):
			manager.add_message('message %d' % i)

		# wait for scale up
		time.sleep(2)
		self.assertGreater(manager.num_of_all_workers, 1)

		# wait for scale down
		time.sleep(10)
		self.assertEqual(manager.num_of_all_workers, 1)

		manager.shutdown()


if __name__ == '__main__':
	unittest.main(verbosity=2)
