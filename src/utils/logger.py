import logging


class Logger:
    """Logger class"""

    _instance = None

    def __init__(self, name=None, filename="log.log", level=logging.INFO):
        if Logger._instance is None:
            Logger._instance = logging.getLogger(name)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

            # File handler
            file_handler = logging.FileHandler(filename)
            file_handler.setFormatter(formatter)
            Logger._instance.addHandler(file_handler)

            # Stream handler
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            Logger._instance.addHandler(stream_handler)

            Logger._instance.setLevel(level)

    def __getattr__(self, attr):
        return getattr(Logger._instance, attr)


logger = Logger(name="my_logger", level=logging.INFO)
