import base64
import json

from flask import Flask, request

app = Flask(__name__)


@app.route("/")
def index():
    return "Working"


@app.route("/api/", methods=["POST"])
def api():
    envelope = request.get_json()
    print(envelope)
    print(type(envelope))
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    request_json = envelope["message"]["data"]

    if not isinstance(request_json, dict):
        json_data = base64.b64decode(request_json).decode("utf-8")
        request_json = json.loads(json_data)

    print(request_json)

    import time
    time.sleep(10)

    print("Success")
    return "success", 200
