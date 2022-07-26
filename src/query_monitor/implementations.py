"""
All implementations of the base QueryMonitor class
"""
from __future__ import annotations

import logging.config
import urllib.parse
from datetime import datetime, timedelta
from typing import Optional

import yaml
from duneapi.api import DuneAPI
from duneapi.types import QueryParameter
from slack.web.client import WebClient

from src.models import TimeWindow
from src.query_monitor.base import BaseQueryMonitor

log = logging.getLogger(__name__)
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)


def load_from_config(config_yaml: str) -> BaseQueryMonitor:
    """Loads a QueryMonitor object from yaml configuration file"""
    with open(config_yaml, "r", encoding="utf-8") as yaml_file:
        cfg = yaml.load(yaml_file, yaml.Loader)

    name, query_id = cfg["name"], cfg["id"]
    params = [
        QueryParameter.from_dict(param_cfg) for param_cfg in cfg.get("parameters", [])
    ]
    try:
        window = TimeWindow.from_cfg(cfg["window"])
        return WindowedQueryMonitor(name, query_id, window, params)
    except KeyError:
        return QueryMonitor(name, query_id, params)


class QueryMonitor(BaseQueryMonitor):
    """This is essentially the base query monitor with all default methods"""


class WindowedQueryMonitor(BaseQueryMonitor):
    """
    All queries here, must have `StartTime` and `EndTime` as parameters,
    set by an instance's window attribute via window.as_query_parameters()
    """

    window: TimeWindow

    def __init__(
        self,
        name: str,
        query_id: int,
        window: TimeWindow,
        params: Optional[list[QueryParameter]] = None,
    ):
        super().__init__(name, query_id, params)
        self._set_window(window)

    def parameters(self) -> list[QueryParameter]:
        """Similar to the base model, but with window parameters appended"""
        return self.fixed_params + self.window.as_query_parameters()

    def result_url(self) -> str:
        """Returns a link to the query"""
        base = super().result_url()
        # Include variable parameters in the URL so they are set
        query = "&".join(
            [f"{p.key}={p.value}" for p in self.window.as_query_parameters()]
        )
        return "?".join([base, urllib.parse.quote_plus(query, safe="=&?")])

    def _set_window(self, window: TimeWindow) -> None:
        if window.end > datetime.now() - timedelta(hours=2):
            log.warning(
                "window end time is beyond 2 hours in the past, some data may not yet be available"
            )
        self.window = window

    def run_loop(
        self, dune: DuneAPI, slack_client: WebClient, alert_channel: str
    ) -> None:
        """
        Originally designed to run infinitely and shift the window after each successful run
        If we go with a memory-free approach this will not be used.
        However, it can be adjusted to use Dune as persistent storage as follows:
            - Query can be loaded from dune by ID.
            - query can then be updated with configuration parameters as new defaults
                (i.e. make the defaults equal to the current run).
            Then, on the next run, shift the time window from the last successful run.
        """
        super().run_loop(dune, slack_client, alert_channel)
        # After a successful run loop, we update the window for the next
        self._set_window(self.window.next())