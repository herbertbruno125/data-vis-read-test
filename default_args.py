from datetime import timedelta

from constants import *

DT_PROC = datetime.now().date() - timedelta(days=1)


class DefaultArgs:

    def __init__(self, environment, dt) -> None:
        self._environment = environment
        self._dt = dt

    @property
    def environment(self):
        return ENVIRONMENT if not self._environment else self._environment

    @property
    def dt(self):
        return DT_PROC if not self._dt else self._dt
