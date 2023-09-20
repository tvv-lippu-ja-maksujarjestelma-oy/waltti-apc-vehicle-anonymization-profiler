import functools
import signal
import sys
import traceback


# FIXME:
# Due to a known issue we close Pulsar before we use multiprocessing, just in
# case. Once the issue is satisfactorily resolved, we can merge this back into
# exit_gracefully if we wish to.
# https://github.com/apache/pulsar-client-python/issues/127
def close_pulsar(resources):
    """Close Pulsar resources."""
    logger = resources["logger"]
    logger.info("Start closing Pulsar resources")
    pulsar_catalogue_readers = resources.get("pulsar_catalogue_readers")
    if pulsar_catalogue_readers is not None:
        for (
            feedPublisherId,
            pulsar_catalogue_reader,
        ) in pulsar_catalogue_readers.items():
            try:
                logger.info(
                    "Close Pulsar catalogue reader",
                    extra={
                        "json_fields": {"feedPublisherId": feedPublisherId}
                    },
                )
                pulsar_catalogue_reader.close()
                del pulsar_catalogue_readers[feedPublisherId]
            except Exception as err:
                logger.error(
                    "Something went wrong when closing Pulsar catalogue"
                    " reader",
                    extra={
                        "json_fields": {"err": traceback.format_exception(err)}
                    },
                )
    pulsar_cache_reader = resources.get("pulsar_cache_reader")
    if pulsar_cache_reader is not None:
        try:
            logger.info("Close Pulsar cache reader")
            pulsar_cache_reader.close()
            del resources["pulsar_cache_reader"]
        except Exception as err:
            logger.error(
                "Something went wrong when closing Pulsar cache reader",
                extra={
                    "json_fields": {"err": traceback.format_exception(err)}
                },
            )
    pulsar_producer = resources.get("pulsar_producer")
    if pulsar_producer is not None:
        try:
            logger.info("Flush Pulsar producer")
            pulsar_producer.flush()
        except Exception as err:
            logger.error(
                "Something went wrong when flushing Pulsar producer",
                extra={
                    "json_fields": {"err": traceback.format_exception(err)}
                },
            )
        try:
            logger.info("Close Pulsar producer")
            pulsar_producer.close()
            del resources["pulsar_producer"]
        except Exception as err:
            logger.error(
                "Something went wrong when closing Pulsar producer",
                extra={
                    "json_fields": {"err": traceback.format_exception(err)}
                },
            )
    pulsar_client = resources.get("pulsar_client")
    if pulsar_client is not None:
        try:
            logger.info("Close Pulsar client")
            pulsar_client.close()
            del resources["pulsar_client"]
        except Exception as err:
            logger.error(
                "Something went wrong when closing Pulsar client",
                extra={
                    "json_fields": {"err": traceback.format_exception(err)}
                },
            )


def exit_gracefully(resources, exit_code, exception=None):
    """Exit gracefully closing all open resources in the right order."""
    logger = resources["logger"]
    if exception is not None:
        logger.critical(
            "An error occurred",
            extra={
                "json_fields": {"err": traceback.format_exception(exception)}
            },
        )
    logger.info("Start exiting gracefully")
    set_health_ok = resources.get("set_health_ok")
    if set_health_ok is not None:
        try:
            logger.info("Set health checks to fail")
            set_health_ok(False)
            del resources["set_health_ok"]
        except Exception as err:
            logger.error(
                "Something went wrong when setting health checks to fail",
                extra={
                    "json_fields": {"err": traceback.format_exception(err)}
                },
            )
    close_pulsar(resources)
    close_health_check_server = resources.get("close_health_check_server")
    if close_health_check_server is not None:
        try:
            logger.info("Close health check server")
            close_health_check_server()
            del resources["close_health_check_server"]
        except Exception as err:
            logger.error(
                "Something went wrong when closing health check server",
                extra={
                    "json_fields": {"err": traceback.format_exception(err)}
                },
            )
    logger.info("Exit process")
    sys.exit(exit_code)


def reset_signal_handlers():
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGQUIT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


def capture_frames(frame):
    frames = []
    while frame is not None:
        frames.append(frame)
        frame = frame.f_back
    return frames


def get_exit_handler(resources):
    """Set signal handlers and return a suitable closure to call to exit."""

    def run_exit_handler(exit_code, exception=None):
        exit_gracefully(resources, exit_code, exception)

    def signal_handler(exit_code, signum, frame):
        signal_name = signal.Signals(signum).name
        frames = capture_frames(frame)
        frame_essentials = [
            {
                "filename": frame.f_code.co_filename,
                "lineNumber": frame.f_lineno,
                "functionName": frame.f_code.co_name,
            }
            for frame in frames
        ]
        logger = resources["logger"]
        logger.critical(
            "Received signal",
            extra={
                "json_fields": {
                    "signalNumber": signum,
                    "signalName": signal_name,
                    "executionStackFrames": frame_essentials,
                }
            },
        )
        run_exit_handler(exit_code, exception=RuntimeError(signal_name))

    signal.signal(
        signal.SIGINT,
        functools.partial(signal_handler, 130),
    )
    signal.signal(
        signal.SIGQUIT,
        functools.partial(signal_handler, 131),
    )
    signal.signal(
        signal.SIGTERM,
        functools.partial(signal_handler, 143),
    )
    return run_exit_handler
