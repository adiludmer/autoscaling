import unittest
import json
import time

import server
import os
from worker import SHARED_FILE


class TestServer(unittest.TestCase):

	def test_system_load(self):
		# delete shared file
		try:
			os.unlink(SHARED_FILE)
		except FileNotFoundError:
			pass

		test_client = server.app.test_client()
		test_client.get('/start')

		# fill queue
		for _ in range(10):
			test_client.post(
				'/message',
				data=json.dumps({"message": "hello"}),
				content_type='application/json'
			)

		# wait scale up
		while True:
			response = test_client.get('/statistics')
			stats = json.loads(response.data)
			if stats['active'] > 4:
				break

			time.sleep(0.1)

		# wait for scale down
		while True:
			response = test_client.get('/statistics')
			stats = json.loads(response.data)
			if stats['waiting'] == 1 and stats['active'] == 0:
				break

			time.sleep(0.1)

		test_client.get('/stop')

		# wait for shutdown
		while True:
			response = test_client.get('/statistics')
			stats = json.loads(response.data)
			if stats['waiting'] == 0 and stats['active'] == 0:
				break

			time.sleep(0.1)

		# checking shared file not contining broken messages
		with open(SHARED_FILE, "r") as f:
			for line in f.readlines():
				self.assertEqual(line, "hello\n")


if __name__ == '__main__':
	unittest.main(verbosity=2)
