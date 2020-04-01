import gzip
import os
import threading
import uuid

import pika
import time

from http import HTTPStatus

from flask import Flask, request, render_template, jsonify, send_from_directory
from werkzeug.utils import secure_filename

UPLOAD_FOLDER = './uploads'

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config.from_pyfile('./default_settings.cfg')

RABBITMQ_HOST = "152.118.148.95"
RABBITMQ_PORT = 5672
RABBITMQ_WEBSOCKET_PORT = 15674
RABBITMQ_USER = '0806444524'
RABBITMQ_PASSWORD = '0806444524'
RABBITMQ_VHOST = '/0806444524'

NPM = '1606918401'

CHUNK_SIZE = 1024 * 2


class FasilkomRabbitMQConnection:
    def __init__(self, routing_key, npm):
        self.npm = npm
        self.routing_key = routing_key
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST,
                                      port=RABBITMQ_PORT,
                                      virtual_host=RABBITMQ_VHOST,
                                      credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD),
                                      )
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.npm, exchange_type='direct', )

    def send_message(self, message):
        app.logger.info(f'Publishing exchange={self.npm} routing_key={self.routing_key} body={message}')
        self.channel.basic_publish(exchange=self.npm, routing_key=self.routing_key, body=message)

    def close(self):
        self.channel.close()


@app.route('/uploads/<filename>')
def download_file(filename):
    app.logger.info(f'Download Request: {filename}')
    return send_from_directory(app.config['UPLOAD_FOLDER'], secure_filename(filename))


@app.route('/', methods=['POST'], strict_slashes=False)
def index_handler():
    def bad_request(message):
        app.logger.error(message)
        return jsonify({'status': 'error', 'description': message}), HTTPStatus.BAD_REQUEST

    routing_key = request.headers.get('X-ROUTING-KEY', default=None)

    if not routing_key:
        return bad_request('Missing "X-ROUTING-KEY" in the headers')

    if 'file' not in request.files:
        return bad_request('Missing key "file" in headers files')

    file = request.files['file']
    filename = str(uuid.uuid4()) + secure_filename(file.filename)
    save_file(file, filename)
    run_compress_in_background(filename=filename, routing_key=routing_key)

    data = {
        'status': 'ok',
        'rabbitmq_login': RABBITMQ_USER,
        'rabbitmq_passcode': RABBITMQ_PASSWORD,
        'rabbitmq_host': RABBITMQ_VHOST,
        'websocket_url': f'http://{RABBITMQ_HOST}:{RABBITMQ_WEBSOCKET_PORT}/stomp',
        'subscription_topic': f'/exchange/{NPM}/{routing_key}',
        'routing_key': routing_key,
        'filename': filename,
    }
    return jsonify(data), 200


def path(filename):
    return os.path.join(app.config['UPLOAD_FOLDER'], filename)


def save_file(file, filename):
    file.save(path(filename))


def delete_file(filename):
    path_to_file = path(filename)
    os.remove(path_to_file)


def replace_extension(filename, new_extension):
    return os.path.splitext(filename)[0] + '.' + new_extension


def compress(filename, routing_key):
    connection = FasilkomRabbitMQConnection(npm=NPM, routing_key=routing_key)

    path_to_file = path(filename)
    file_size = os.path.getsize(path_to_file)

    compressed_output_file_name = path(replace_extension(filename, 'gz'))

    output_file = gzip.open(compressed_output_file_name, 'wb')
    fp = open(path_to_file, 'rb')

    processed_size = 0
    while True:
        time.sleep(0.001)  # Add this so that progress can be seen when file size is to small
        previous_percentage = processed_size / file_size * 100.0

        byte = fp.read(CHUNK_SIZE)
        output_file.write(gzip.compress(byte))
        processed_size += len(byte)
        new_percentage = processed_size / file_size * 100.0
        if previous_percentage // 10 != new_percentage // 10:
            percentage_in_int = new_percentage // 10 * 10
            connection.send_message(f'Progress: {percentage_in_int}%')

        if len(byte) == 0:
            break

    output_file.close()
    fp.close()

    delete_file(filename)
    connection.send_message(compressed_output_file_name)
    connection.close()


def run_compress_in_background(filename, routing_key):
    thread = threading.Thread(target=compress, args=(filename, routing_key))
    thread.start()


if __name__ == '__main__':
    os.system(f'mkdir -p {UPLOAD_FOLDER}')
    app.run(debug=True, port=5050)
