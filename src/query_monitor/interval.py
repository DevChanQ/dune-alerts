"""QueryMonitor for Intervals. Alert to intervals"""
import math

from dune_client.types import DuneRecord
from dune_client.query import Query

from src.alert import Alert
from src.query_monitor.base import QueryBase

from src.shelve import PgShelve

class IntervalQueryMonitor(QueryBase):
    """
    All queries here, must return a single record specifying a column with numeric type.
    """

    def __init__(
        self,
        query: Query,
        column: str,
        formatter: str = "",
        interval_value: float = 0.0,
    ):
        super().__init__(query)

        self.shelve = PgShelve()
        self.column = column
        self.interval_value = interval_value
        self.formatter = formatter

    def _result_value(self, results: list[DuneRecord]) -> float:
        # assert len(results) == 1, f"Expected single record, got {results}"
        return float(results[0][self.column])
    
    def get_interval_display(self, interval: int) -> str:
        return self.interval_value * interval
    
    @property
    def key(self) -> str:
        return f'{self.column}_interval_{self.interval_value}'

    def get_alert(self, results: list[DuneRecord]) -> Alert:
        try:
            result_value = self._result_value(results)
            current_interval = math.floor(result_value/self.interval_value)
            
            # check last value
            last_value = float(self.shelve[self.key])
            last_interval = math.floor(last_value/self.interval_value)

            # update last value
            self.shelve[self.key] = result_value

            # need to notify if interval is larger or smaller than before
            if current_interval > last_interval:
                return Alert.slack(
                    message=f"{self.name} exceeded {self.formatter.format(self.interval_value * current_interval)} "
                    f"with {self.formatter.format(result_value)} (cf. {self.result_url()})",
                )
            elif current_interval < last_interval:
                return Alert.slack(
                    message=f"{self.name} dropped below {self.formatter.format(self.interval_value * current_interval)} "
                    f"with {self.formatter.format(result_value)} (cf. {self.result_url()})",
                )
            else:
                return Alert.log(
                    message='Interval remained the same as before'
                )
            
        except KeyError:
            self.shelve[self.key] = result_value
            return Alert.log(
                message=f'First time setting key: {self.key} in db'
            )
