from logging import getLogger, Formatter, DEBUG, INFO, WARNING, ERROR, CRITICAL
from logging import StreamHandler
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import QueueListener, QueueHandler
from queue import Queue
from pathlib import Path

def get_level(level):
    levels = dict()
    levels["debug"] = DEBUG
    levels["info"] = INFO
    levels["warning"] = WARNING
    levels["error"] = ERROR
    levels["critical"] = CRITICAL
    return levels.get(level, DEBUG)

def get_formatter(formatter, date_format):
    if formatter is None:
        formatter = u"%(asctime)s - %(name)s - %(threadName)s"
        formatter = formatter + u" - %(levelname)s - %(message)s"

    if date_format is None:
        date_format = "%Y-%m-%d %H:%M:%S"

    return Formatter(formatter)

class Stream(StreamHandler):
    def __init__(self, level=None, formatter=None, date_format=None):
        super().__init__()
        self.setLevel(get_level(level))
        self.setFormatter(get_formatter(formatter, date_format))

class Rotated(TimedRotatingFileHandler):
    def __init__(
        self,
        filename,
        when,
        interval,
        backupCount,
        level=None,
        formatter=None,
        date_format=None,
        **kwargs
    ):
        args = filename, when, interval, backupCount
        super().__init__(*args, encoding="utf8", **kwargs)

        self.setLevel(get_level(level))
        self.setFormatter(get_formatter(formatter, date_format))

class Logger:
    def __init__(self, name, level=None, settings=dict()):
        self.name = name
        self.core = getLogger(name)
        self.core.setLevel(get_level(level))
        self._load(settings)

    def _load(self, settings):
        disabled = settings.get("disabled", list())
        handlers = list()
        if "stream" not in disabled:
            stream = settings.get("stream", dict())
            stream_level = stream.get("level", None)
            stream_formatter = stream.get("formatter", None)
            stream_date_format = stream.get("date_format", None)
            stream_args = stream_level, stream_formatter, stream_date_format
            stream_handler = Stream(*stream_args)
            handlers.append(stream_handler)

        if "file" not in disabled:
            rotated = settings.get("file", dict())
            rotated_filename = rotated.get("filename", f"{self.name}.log")
            rotated_when = rotated.get("when", "midnight")
            rotated_interval = rotated.get("interval", 1)
            rotated_backup_count = rotated.get("backup_count", 5)
            rotated_level = rotated.get("level", None)
            rotated_formatter = rotated.get("formatter", None)
            rotated_date_format = rotated.get("date_format", None)
            rotated_args = (
                rotated_filename,
                rotated_when,
                rotated_interval,
                rotated_backup_count,
                rotated_level,
                rotated_formatter,
                rotated_date_format
            )

            # Creates the log directory if it does not already exist
            log_path = Path(rotated_filename).parent
            log_path.mkdir(parents=True, exist_ok=True)

            rotated_handler = Rotated(*rotated_args)
            handlers.append(rotated_handler)

        self.queue = Queue()
        self.queue_handler = QueueHandler(self.queue)

        args = tuple(handlers)
        kwargs = dict()
        kwargs["respect_handler_level"] = True
        self.listener = QueueListener(self.queue, *args, **kwargs)
        self.core.addHandler(self.queue_handler)

    def start(self):
        self.listener.start()

    def stop(self):
        self.listener.stop()

    def debug(self, message, extra=dict()):
        self.core.debug(message, extra=extra)

    def info(self, message, extra=dict()):
        self.core.info(message, extra=extra)

    def warning(self, message, extra=dict()):
        self.core.warning(message, extra=extra)

    def error(self, message, extra=dict()):
        self.core.error(message, extra=extra)

    def critical(self, message, extra=dict()):
        self.core.critical(message, extra=extra)

    def write(self, message):
        self.core.info(message.rstrip())

