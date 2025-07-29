from apscheduler.schedulers.blocking import BlockingScheduler
from src.driver import DataSyncClient
from datetime import datetime
import json



def job():
    _job_for_cinema_tickets()
    _job_for_others()
    _message_after_job()


def _job_for_cinema_tickets():
    syncer = DataSyncClient(".env", "config.json")
    syncer.sync_most_recent_data('C01', syncer.config.get_name('C01'))


def _job_for_others():
    syncer = DataSyncClient(".env", "config.json")
    syncer.sync_all_yesterday()


def _message_after_job():
    syncer = DataSyncClient(".env", "config.json")
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    message = json.dumps({
        "text": ('<at user_id="all"></at> ' f'{timestamp}' f' <b>SERVER OK</b>')
    })

    syncer.lark_client.send_message_to_chat_group(
        message,
        syncer.config.get("CHAT_ID")
    )

def _message_two_hours():
    syncer = DataSyncClient(".env", "config.json")
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    message = json.dumps({
        "text": (f'{timestamp}' f' <b>SERVER OK</b>')
    })

    syncer.lark_client.send_message_to_chat_group(
        message,
        syncer.config.get("CHAT_ID")
    )

if __name__ == "__main__":
    scheduler = BlockingScheduler()

    scheduler.add_job(job, 'cron', hour=8, minute=0)
    scheduler.add_job(_message_two_hours, 'cron', hour='0-23')

    scheduler.start()

