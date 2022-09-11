import logging

config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
        "json": {
            "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
            "class": "pythonjsonlogger.jsonlogger.JsonFormatter"
        }
    },
    "handlers": {
        "standard": {
            "class": "logging.StreamHandler",
            "formatter": "json"
        },
        "kinesis": {
            "class": "KinesisFirehoseDeliveryStreamHandler.KinesisFirehoseDeliveryStreamHandler",
            "formatter": "json"
        }
    },
    "loggers": {
        "": {
            "handlers": ["standard", "kinesis"],
            "level": logging.INFO
        }
    }
}