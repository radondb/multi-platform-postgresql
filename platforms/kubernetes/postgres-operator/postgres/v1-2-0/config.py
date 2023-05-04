import logging
import os
from typed import List, Optional
import kopf
from constants import (
    AUTOFAILOVER,
    POSTGRESQL,
)

UNDEFINED = object()


class Config:
    BOOTSTRAP_TIMEOUT: int = 3600
    BOOTSTRAP_RETRIES: int = 3
    BOOTSTRAP_RETRY_DELAY: int = 60
    TIMER_INTERVAL: int = 10
    IMAGE_PULL_SECRETS: Optional[List[str]] = None
    LOG_LEVEL: str = "INFO"
    TESTING: bool = False
    DATA_PATH: str = "/data"
    DATA_PATH_AUTOFAILOVER: str = os.path.join(DATA_PATH, AUTOFAILOVER)
    DATA_PATH_POSTGRESQL: str = os.path.join(DATA_PATH, POSTGRESQL)
    IMAGE_REGISTRY: str = ""
    NAMESPACE_OVERRIDE: str = ""

    def __init__(self, *, prefix: str):
        self._prefix = prefix

    def load(self):
        # BOOTSTRAP_TIMEOUT
        bootstrap_timeout = self.env("BOOTSTRAP_TIMEOUT",
                                     default=str(self.BOOTSTRAP_TIMEOUT))
        try:
            self.BOOTSTRAP_TIMEOUT = int(bootstrap_timeout)
        except ValueError:
            raise kopf.TemporaryError(
                f"Invalid {self._prefix}BOOTSTRAP_TIMEOUT="
                f"'{bootstrap_timeout}'. Needs to be a positive integer.")
        if self.BOOTSTRAP_TIMEOUT <= 60:
            raise kopf.TemporaryError(
                f"Invalid {self._prefix}BOOTSTRAP_TIMEOUT="
                f"'{bootstrap_timeout}'. Needs to be large than 60.")

        # BOOTSTRAP_RETRIES
        bootstrap_retries = self.env("BOOTSTRAP_RETRIES",
                                     default=str(self.BOOTSTRAP_RETRIES))
        try:
            self.BOOTSTRAP_RETRIES = int(bootstrap_retries)
        except ValueError:
            raise kopf.TemporaryError(
                f"Invalid {self._prefix}BOOTSTRAP_RETRIES="
                f"'{bootstrap_retries}'. Needs to be a positive integer.")
        if self.BOOTSTRAP_RETRIES <= 1:
            raise kopf.TemporaryError(
                f"Invalid {self._prefix}BOOTSTRAP_RETRIES="
                f"'{bootstrap_retries}'. Needs to be large than 1.")

        # BOOTSTRAP_RETRY_DELAY
        bootstrap_retry_delay = self.env("BOOTSTRAP_RETRY_DELAY",
                                         default=str(
                                             self.BOOTSTRAP_RETRY_DELAY))
        try:
            self.BOOTSTRAP_RETRY_DELAY = int(bootstrap_retry_delay)
        except ValueError:
            raise kopf.TemporaryError(
                f"Invalid {self._prefix}BOOTSTRAP_RETRY_DELAY="
                f"'{bootstrap_retry_delay}'. Needs to be a positive integer.")
        if self.BOOTSTRAP_RETRY_DELAY <= 30:
            raise kopf.TemporaryError(
                f"Invalid {self._prefix}BOOTSTRAP_RETRY_DELAY="
                f"'{bootstrap_retry_delay}'. Needs to be large than 30.")

        # TIMER_INTERVAL
        timer_interval = self.env("TIMER_INTERVAL",
                                  default=str(self.TIMER_INTERVAL))
        try:
            self.TIMER_INTERVAL = int(timer_interval)
        except ValueError:
            raise kopf.TemporaryError(
                f"Invalid {self._prefix}TIMER_INTERVAL="
                f"'{timer_interval}'. Needs to be a positive integer.")
        if self.TIMER_INTERVAL <= 1:
            raise kopf.TemporaryError(
                f"Invalid {self._prefix}TIMER_INTERVAL="
                f"'{timer_interval}'. Needs to be large than 1.")

        # IMAGE_PULL_SECRETS
        secrets = self.env("IMAGE_PULL_SECRETS",
                           default=self.IMAGE_PULL_SECRETS)
        if secrets is not None:
            self.IMAGE_PULL_SECRETS = [
                s for s in (secret.strip() for secret in secrets.split(","))
                if s
            ]

        # LOG_LEVEL
        self.LOG_LEVEL = self.env("LOG_LEVEL", default=self.LOG_LEVEL)
        level = logging.getLevelName(self.LOG_LEVEL)
        for logger_name in ("", "kopf"):
            logger = logging.getLogger(logger_name)
            logger.setLevel(level)

        # DATA_PATH
        self.DATA_PATH = self.env("DATA_PATH", default=self.DATA_PATH)
        if len(self.DATA_PATH) == 0:
            raise kopf.TemporaryError(
                f"Invalid {self._prefix}DATA_PATH, length needs to be large than 0."
            )
        self.DATA_PATH_AUTOFAILOVER = os.path.join(self.DATA_PATH,
                                                   AUTOFAILOVER)
        self.DATA_PATH_POSTGRESQL = os.path.join(self.DATA_PATH, POSTGRESQL)

        # TESTING
        testing = self.env("TESTING", default=str(self.TESTING))
        self.TESTING = testing.lower() == "true"

        # IMAGE_REGISTRY
        self.IMAGE_REGISTRY = self.env("IMAGE_REGISTRY",
                                       default=self.IMAGE_REGISTRY)

        # NAMESPACE_OVERRIDE
        self.NAMESPACE_OVERRIDE = self.env("NAMESPACE_OVERRIDE",
                                           default=self.NAMESPACE_OVERRIDE)

    def env(self, name: str, *, default=UNDEFINED) -> str:
        full_name = f"{self._prefix}{name}"
        try:
            return os.environ[full_name]
        except KeyError:
            if default is UNDEFINED:
                # raise from None - so that the traceback of the original
                # exception (KeyError) is not printed
                # https://docs.python.org/3.8/reference/simple_stmts.html#the-raise-statement
                raise kopf.TemporaryError(
                    f"Required environment variable '{full_name}' is not set."
                ) from None
            return default


# The global instance of the radondb postgres operator config
operator_config = Config(prefix="RADONDB_POSTGRES_OPERATOR_")
