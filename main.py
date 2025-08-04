"""
Main entry point
"""
from datetime import datetime
import json
import os

from apscheduler.schedulers.blocking import BlockingScheduler
from src.driver import DataSyncClient

def job(syncer: DataSyncClient):
    _job_for_cinema_tickets(syncer)
    _job_for_others(syncer)
    _message_after_job(syncer)

def _job_for_cinema_tickets(syncer: DataSyncClient):
    syncer.sync_most_recent_data('C01', syncer.config.get_name('C01'))


def _job_for_others(syncer: DataSyncClient):
    syncer.sync_all_yesterday()
    syncer.sync_screening_data()


def _message_after_job(syncer: DataSyncClient):
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    message = json.dumps({
        "text": ('<at user_id="all"></at> ' f'{timestamp}' f' <b>JOB OK</b>')
    })

    syncer.lark_client.send_message_to_chat_group(
        message,
        syncer.lark_client.get_chat_group_id_by_name('服务器状态')
    )

def _message_ok(syncer: DataSyncClient):
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    message = json.dumps({
        "text": (f'{timestamp}' f' <b>SERVER OK</b>')
    })

    syncer.lark_client.send_message_to_chat_group(
        message,
        syncer.lark_client.get_chat_group_id_by_name('服务器状态')
    )

def _message_init(syncer: DataSyncClient):
    current_time = datetime.now()
    timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    message = json.dumps({
        "text": (f'{timestamp}' f' <b>SERVER INITIALIZING</b>')
    })

    syncer.lark_client.send_message_to_chat_group(
        message,
        syncer.lark_client.get_chat_group_id_by_name('服务器状态')
    )

if __name__ == "__main__":
    global_syncer = DataSyncClient(".env", "config.json")
    try:
        _message_init(global_syncer)
        scheduler = BlockingScheduler()

        scheduler.add_job(job, 'cron', hour=8, minute=0, args=[global_syncer])

        # Use lambda or partial to defer execution
        scheduler.add_job(_message_ok, 'cron', hour='0-23/4', args=[global_syncer])

        scheduler.start()
    except Exception as e:
        os._exit(1)

        

