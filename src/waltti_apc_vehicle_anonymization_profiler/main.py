"""Main."""

import os
import sys
import traceback

from waltti_apc_vehicle_anonymization_profiler import (
    configuration,
    gcp_logging,
    graceful_exit,
    health_check,
    message_processing,
    pulsar_wrapper,
)


def main():
    service_name = "waltti-apc-vehicle-anonymization-profiler"
    try:
        logger = gcp_logging.create_logger(service_name)
        try:
            resources = {"logger": logger}
            exit_handler = graceful_exit.get_exit_handler(resources)
            logger.info(f"Start service {service_name}")
            logger.info("Read configuration")
            config = configuration.read_configuration(logger)
            logger.info("Create health check server")
            health_check_server = health_check.create_health_check_server(
                config["health_check"]
            )
            close_health_check_server, set_health_ok = (
                health_check_server["close_health_check_server"],
                health_check_server["set_health_ok"],
            )
            resources["close_health_check_server"] = close_health_check_server
            resources["set_health_ok"] = set_health_ok
            logger.info("Create Pulsar client")
            pulsar_client = pulsar_wrapper.create_client(
                logger, config["pulsar"]["client"], config["pulsar"]["oauth2"]
            )
            resources["pulsar_client"] = pulsar_client
            logger.info("Create Pulsar producer")
            pulsar_producer = pulsar_wrapper.create_producer(
                pulsar_client, config["pulsar"]["producer"]
            )
            resources["pulsar_producer"] = pulsar_producer
            logger.info("Create cache-warming Pulsar reader")
            pulsar_cache_reader = pulsar_wrapper.create_reader(
                pulsar_client, config["pulsar"]["cache_reader"]
            )
            resources["pulsar_cache_reader"] = pulsar_cache_reader
            logger.info("Create catalogue Pulsar readers")
            pulsar_catalogue_readers = pulsar_wrapper.create_readers(
                pulsar_client, config["pulsar"]["catalogue_readers"]
            )
            resources["pulsar_catalogue_readers"] = pulsar_catalogue_readers
            logger.info("Set health check status to OK")
            set_health_ok(True)
            logger.info("Process messages")
            message_processing.process_messages(
                logger,
                config["processing"],
                # FIXME:
                # Due to a known issue we send pulsar_config into
                # message_processing so that the module can destroy and create
                # Pulsar resources. Once the issue is satisfactorily resolved,
                # pass just the producer and the readers onwards as usual.
                # https://github.com/apache/pulsar-client-python/issues/127
                config["pulsar"],
                # FIXME:
                # Due to a known issue we send resources into
                # message_processing so that the module can destroy and create
                # Pulsar resources. Once the issue is satisfactorily resolved,
                # pass just the producer and the readers onwards as usual.
                # https://github.com/apache/pulsar-client-python/issues/127
                resources,
            )
            logger.info("Finished successfully")
            exit_handler(os.EX_OK)
        except Exception as err:
            exit_handler(1, err)
    except Exception as err:
        print("Logging failed: " + traceback.format_exception(err))
        sys.exit(1)


if __name__ == "__main__":
    main()
