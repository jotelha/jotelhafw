# coding: utf-8
import logging

class LoggingContext():
    """Modifies logging within context."""
    def __init__(self, logger=None, handler=None, level=None, close=False):
        self.logger = logger
        self.level = level
        self.handler = handler
        self.close = close

    def __enter__(self):
        """Prepare log output streams."""
        if self.logger is None:
            # temporarily modify root logger
            self.logger = logging.getLogger('')
        if self.level is not None:
            self.old_level = self.logger.level
            self.logger.setLevel(self.level)
        if self.handler:
            self.logger.addHandler(self.handler)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.logger.setLevel(self.old_level)
        if self.handler:
            self.logger.removeHandler(self.handler)
        if self.handler and self.close:
            self.handler.close()
        # implicit return of None => don't swallow exceptions
