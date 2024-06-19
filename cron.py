import os
import dotenv

from apscheduler.schedulers.blocking import BlockingScheduler
from dune_client.client import DuneClient

from src.query_monitor.factory import load_config, AlertType
from src.slack_client import BasicSlackClient
from src.runner import QueryRunner

sched = BlockingScheduler()

dotenv.load_dotenv()

def run_query(path: str):
    config = load_config(path)
    alerter = BasicSlackClient(
        token=os.environ["SLACK_TOKEN"],
        # Use specified channel, or default to "global config"
        channel=config.alert_channel or os.environ["SLACK_ALERT_CHANNEL"],
    )

    query_runner = QueryRunner(
        config.query,
        DuneClient(os.environ["DUNE_API_KEY"]),
        alerter,
        config.ping_frequency
    )
    query_runner.run_loop()

@sched.scheduled_job('interval', hours=6)
def timed_job():
    run_query("./ao-steth.yaml")

sched.start()