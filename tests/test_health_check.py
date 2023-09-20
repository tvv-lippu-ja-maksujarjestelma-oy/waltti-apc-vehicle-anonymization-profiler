import time

import pytest
import requests
from waltti_apc_vehicle_anonymization_profiler import health_check


@pytest.fixture(scope="module")
def port():
    yield 8080


@pytest.fixture(scope="module")
def url(port):
    host = "127.0.0.1"
    yield f"http://{host}:{port}/healthz"


@pytest.fixture()
def server(port):
    server = health_check.create_health_check_server({"port": port})
    # Give the server some time to start up in the other process.
    time.sleep(0.1)
    yield server
    server["close_health_check_server"]()


def assert_healthy(response):
    assert response.status_code == 204
    assert "content-type" not in response.headers
    assert response.text == ""


def assert_unhealthy(response):
    assert response.status_code == 500
    assert (
        response.headers["content-type"] == "application/json; charset=utf-8"
    )
    assert response.encoding == "utf-8"
    assert response.json() == {"error": "Service is unhealthy"}


def test_unhealthy_by_default(url, server):
    response = requests.get(url)
    assert_unhealthy(response)


def test_healthy_after_setting_ok(url, server):
    server["set_health_ok"](True)
    response = requests.get(url)
    assert_healthy(response)


def test_unhealthy_after_setting_not_ok(url, server):
    server["set_health_ok"](False)
    response = requests.get(url)
    assert_unhealthy(response)


def test_unhealthy_after_setting_not_ok_after_ok(url, server):
    server["set_health_ok"](True)
    server["set_health_ok"](False)
    response = requests.get(url)
    assert_unhealthy(response)
