from flask import Flask, jsonify, request
from manager import WorkManager

app = Flask(__name__)

work_manager = WorkManager()


@app.route('/message', methods=['POST'])
def post_message():
	msg = request.json['message']
	work_manager.add_message(msg)
	return jsonify({'status': 'OK'})


@app.route('/statistics', methods=['GET'])
def get_statistics():
	return jsonify({
		'active': work_manager.num_of_active_workers,
		'waiting': work_manager.num_of_waiting_workers,
		'invocations': work_manager.num_of_invocations,
		'q_size': work_manager.qsize
	})


@app.route('/start', methods=['GET'])
def start_server():
	work_manager.start()
	return jsonify({'status': 'OK'})


@app.route('/stop', methods=['GET'])
def stop_server():
	work_manager.shutdown()
	return jsonify({'status': 'OK'})


if __name__ == '__main__':
	app.run(host="0.0.0.0", debug=False, threaded=True)

