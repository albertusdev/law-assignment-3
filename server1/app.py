import os
import uuid
import requests

from http import HTTPStatus
from flask import Flask, request, render_template, jsonify
from requests.exceptions import Timeout, RequestException

app = Flask(__name__)
app.config.from_pyfile('./default_settings.cfg')

COMPRESSOR_SERVICE_URL = os.environ.get('COMPRESS_SERVICE_URL', default='http://localhost:5050')


# ADD NEW EXTENSION HERE!
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'doc', 'docx', 'xls', 'mp4', 'djvu', 'xlsx', 'svg'}


def bad_request(error_description):
    return jsonify({'error': 'Bad Request', 'error_description': error_description}), 400


@app.route('/', methods=['GET', 'POST'], strict_slashes=False)
def index_handler():
    if request.method == 'POST':
        return upload_file_handler()
    return render_template('index.html')


def upload_file_handler():
    if 'file' not in request.files:
        return bad_request('Please include file with key "file" in the form data')

    file = request.files['file']
    if file.filename == '':
        return bad_request('No selected file')

    app.logger.info(
        f'Uploading file with filename: {file.filename}, size: {file.content_length}, '
        + f'content-type: {file.content_type}'
    )

    def is_filename_allowed(filename):
        return '.' in filename and \
               filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    def validate_file(file):
        if not is_filename_allowed(file.filename):
            return False, 'Invalid extension or filename'
        return True, ''

    ok, error_description = validate_file(file)
    if not ok:
        return bad_request(error_description)

    unique_routing_key = str(uuid.uuid4())

    return send_request_to_compress_service(files=request.files, unique_routing_key=unique_routing_key)


def send_request_to_compress_service(files, unique_routing_key):
    try:
        file = files['file']
        response = requests.post(COMPRESSOR_SERVICE_URL, headers={
            'X-ROUTING-KEY': unique_routing_key,
        }, files={
            'file': (file.filename, file.read(), file.mimetype),
        })
        data = response.json()

        subscription_topic = data['subscription_topic']
        rabbitmq_login = data['rabbitmq_login']
        rabbitmq_passcode = data['rabbitmq_passcode']
        rabbitmq_host = data['rabbitmq_host']
        websocket_url = data['websocket_url']
        filename = data['filename']
        download_url = COMPRESSOR_SERVICE_URL

        if response.status_code == HTTPStatus.OK:
            return render_template('upload_response.html',
                                   rabbitmq_login=rabbitmq_login,
                                   rabbitmq_passcode=rabbitmq_passcode,
                                   rabbitmq_host=rabbitmq_host,
                                   subscription_topic=subscription_topic,
                                   websocket_url=websocket_url,
                                   filename=filename,
                                   download_url=download_url,
                                   )

        return bad_request('Something went wrong with the request')
    except Timeout:
        app.logger.error(f'Timeout happened when sending requests to {COMPRESSOR_SERVICE_URL}')
        return bad_request('Timeout when connecting to compress service')
    except RequestException as e:
        print(e)
        app.logger.error(str(e))
        return bad_request('Something went wront with the request')


if __name__ == '__main__':
    app.run(debug=True)
