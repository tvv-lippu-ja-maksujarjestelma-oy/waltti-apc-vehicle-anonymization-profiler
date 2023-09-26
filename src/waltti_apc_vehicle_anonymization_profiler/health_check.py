"""A simple HTTP health check server."""

# By choice we bind to all interfaces and avoid adding another environment
# variable.
# ruff: noqa: S104

import logging
import multiprocessing

import flask
import werkzeug

from waltti_apc_vehicle_anonymization_profiler import graceful_exit


def create_health_check_server(health_check_config):
    app = flask.Flask(__name__)
    is_healthy_event = multiprocessing.Event()
    process = None

    @app.route("/healthz")
    def health_check():
        if is_healthy_event.wait(timeout=0):
            response = flask.make_response("", 204)
            response.headers.pop("Content-Type", None)
            return response
        response = flask.jsonify({"error": "Service is unhealthy"})
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        return response, 500

    def run_app():
        graceful_exit.reset_signal_handlers()
        # Silence werkzeug logging every request if we use logging.DEBUG
        # elsewhere.
        logger = logging.getLogger("werkzeug")
        logger.setLevel(logging.ERROR)
        server = werkzeug.serving.make_server(
            host="0.0.0.0",
            port=health_check_config["port"],
            app=app,
        )
        server.serve_forever()

    def close_health_check_server():
        nonlocal process
        set_health_ok(False)
        if process is not None:
            process.terminate()
            process.join()
            process = None

    def set_health_ok(is_healthy: bool):
        if isinstance(is_healthy, bool):
            if is_healthy:
                is_healthy_event.set()
            else:
                is_healthy_event.clear()
        else:
            msg = "is_healthy must be a boolean value"
            raise TypeError(msg)

    process = multiprocessing.Process(target=run_app)
    process.start()

    return {
        "close_health_check_server": close_health_check_server,
        "set_health_ok": set_health_ok,
    }
