import logging


class Logger:
    """Logger class"""

    _instance = None

    def __init__(self, name=None, filename="log.log", level=logging.INFO):
        if Logger._instance is None:
            Logger._instance = logging.getLogger(name)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler = logging.FileHandler(filename)
            handler.setFormatter(formatter)
            Logger._instance.addHandler(handler)
            Logger._instance.setLevel(level)

    def __getattr__(self, attr):
        return getattr(Logger._instance, attr)


logger = Logger(name="my_logger", level=logging.INFO)
