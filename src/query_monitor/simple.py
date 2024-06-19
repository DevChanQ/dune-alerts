"""SimpleMonitor for relaying query results. Alert set to valuation"""

from dune_client.types import DuneRecord
from dune_client.query import Query

from src.alert import Alert
from src.query_monitor.base import QueryBase


class SimpleQueryMonitor(QueryBase):
    """
    All queries here, must return a single record specifying a column with numeric type.
    """

    def __init__(
        self,
        query: Query,
        column: str,
        formatter: str
    ):
        super().__init__(query)
        self.column = column
        self.formatter = formatter

    def _result_value(self, results: list[DuneRecord]) -> float:
        # assert len(results) == 1, f"Expected single record, got {results}"
        return float(results[0][self.column])

    def get_alert(self, results: list[DuneRecord]) -> Alert:
        return Alert.slack(
            message=f"{self.name}: {self.formatter.format(self._result_value(results))} (cf. {self.result_url()})"
        )
